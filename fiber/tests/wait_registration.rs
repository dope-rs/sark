pub mod common;

use std::{cell::Cell, net::TcpListener, task::Poll, time::Duration};

use common::client::runtime::{
    calls::{Cancel, Reconnect, WAIT_PATHS, WAIT_RESPONSE},
    origins, peer, pools,
};
use common::server::recover;
use dope::core::{driver::life::recovery, io::Reclaimed, platform::wake};
use dope_fiber::{
    BlockOn as _, abi,
    abi::race::{Either, Race},
    task::batch,
};
use dope_test::harness::{
    GUARD,
    peer::{AcceptScript, TcpAcceptor},
    scenario::run::{Entry, Task as _},
};
use sark_client::protocol::response;
use sark_fiber::client::{Ready as _, config::Config};

#[test]
fn rearms_after_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve peer address");
    let address = listener.local_addr().expect("peer address");
    drop(listener);
    pools::Plan::new(
        address,
        Config::try_new("127.0.0.1").expect("HTTP host allocation"),
        1,
        1,
    )
    .expect("client plan")
    .enter(
        |mut scope| {
            let client = scope.handle;
            let deadline = client.sleep(Duration::from_secs(3)).expect("wait deadline");
            let wait = Reconnect::new(client.wait_active(), address);
            assert!(matches!(
                scope.app.block_on(Race::new(deadline, wait)),
                Ok(Either::Right(Ok(Ok(()))))
            ));
        },
        recovery::Policy::new(
            |event| std::assert_matches!(event, Reclaimed::Shutdown),
            recover,
        ),
        |result| result.expect("client execution"),
    )
    .expect("client shutdown");
}

#[test]
fn rearms_reserved() {
    AcceptScript::with(
        |acceptor| -> Result<(), peer::Error> {
            let stream = acceptor.accept()?;
            let mut conn = peer::Conn::new(&stream);
            for path in WAIT_PATHS {
                let request = conn.try_request()?.expect("queued client request");
                assert_eq!(request.target(), path);
                conn.write_all(WAIT_RESPONSE)?;
            }
            Ok(())
        },
        |server| {
            origins::Plan::new(Config::new(server.addr().to_string()).inflight(1), 1, 4, 1)
                .expect("client plan")
                .enter(
                    |mut scope| {
                        let client = scope.handle;
                        let deadline = client
                            .sleep(Duration::from_secs(3))
                            .expect("admission deadline");
                        let requests = dope_fiber::fiber!('_ => async move {
                            let mut pool = batch::Pool::<2>::acquire()
                                .await
                                .expect("request task pool");
                            batch::Join::from_fn(&mut pool, |index| {
                                client.get(WAIT_PATHS[index.get()])
                            }).await
                        });
                        let result = scope
                            .app
                            .block_on(Race::new(deadline, requests))
                            .expect("drive request tasks");
                        assert!(matches!(&result, Either::Right(_)), "admission wake lost");
                        let replies = match result {
                            Either::Right(replies) => Some(replies),
                            Either::Left(()) => None,
                        }
                        .expect("request tasks finished");
                        for result in replies.expect("join request tasks") {
                            result.expect("queued client response");
                        }
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("client shutdown");
            server
                .finish()
                .expect("peer thread")
                .expect("queued peer requests");
        },
    )
    .expect("start peer");
}

#[test]
fn ready_survives_cancel() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve peer address");
    let address = listener.local_addr().expect("peer address");
    drop(listener);
    pools::Plan::new(
        address,
        Config::try_new("127.0.0.1")
            .expect("HTTP host allocation")
            .idle_timeout(Duration::ZERO),
        1,
        4,
    )
    .expect("client plan")
    .enter(
        |mut scope| {
            let client = scope.handle;
            let deadline = client.sleep(Duration::from_secs(3)).expect("wait deadline");
            let waits = dope_fiber::fiber!('_ => async move {
                let waiting = Cell::new(0);
                let canceled = Cell::new(false);
                let listener = Cell::new(None);
                let mut pool = batch::Pool::<2>::acquire().await.expect("wait task pool");
                batch::Join::from_fn(&mut pool, |_| {
                    Cancel::new(
                        client.wait_active(),
                        || client.connections() != 0 && !canceled.replace(true),
                        || {
                            let count = waiting.get() + 1;
                            waiting.set(count);
                            if count == 2 {
                                listener.set(Some(TcpListener::bind(address).expect("start peer")));
                            }
                        },
                    )
                }).await
            });
            let result = scope
                .app
                .block_on(Race::new(deadline, waits))
                .expect("drive wait tasks");
            assert!(matches!(&result, Either::Right(_)), "readiness wake lost");
            let replies = match result {
                Either::Right(replies) => Some(replies),
                Either::Left(()) => None,
            }
            .expect("wait tasks finished");
            let mut ready = replies.expect("join wait tasks").flatten();
            ready
                .next()
                .expect("surviving waiter")
                .expect("connection ready");
            assert!(ready.next().is_none(), "one notified waiter canceled");
        },
        recovery::Policy::new(
            |event| std::assert_matches!(event, Reclaimed::Shutdown),
            recover,
        ),
        |result| result.expect("client execution"),
    )
    .expect("client shutdown");
}

#[test]
fn dispatch_survives_cancel() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve peer address");
    let address = listener.local_addr().expect("peer address");
    drop(listener);
    let (wait, start) = wake::Ends::blocking().expect("peer release").split();
    let peer = Entry(move || {
        wait.wait_timeout(GUARD).expect("queued requests");
        let listener = TcpAcceptor::bind(address).expect("start peer");
        let stream = listener
            .accept_for(GUARD)
            .expect("accept client")
            .expect("client before deadline");
        stream.read_wait(Some(GUARD)).expect("peer deadline");
        stream.write_wait(Some(GUARD)).expect("peer write deadline");
        let mut conn = peer::Conn::new(&stream);
        let path = conn
            .try_request()
            .expect("read request")
            .expect("surviving request")
            .target()
            .to_owned();
        conn.write_all(WAIT_RESPONSE).expect("respond once");
        assert!(
            conn.try_request().expect("request end").is_none(),
            "duplicate request"
        );
        path
    });
    let (path, wire) = peer
        .with(|| {
            pools::Plan::new(
            address,
            Config::try_new("127.0.0.1").expect("HTTP host allocation").idle_timeout(Duration::ZERO),
            1,
            4,
        )
        .expect("client plan")
        .enter(
            |mut scope| {
                let client = scope.handle;
                let deadline = client.sleep(Duration::from_secs(3)).expect("request deadline");
                let start = &start;
                let requests = dope_fiber::fiber!('_ => async move {
                    let waiting = Cell::new(0);
                    let canceled = Cell::new(false);
                    let mut pool = batch::Pool::<2>::acquire().await.expect("request task pool");
                    batch::Join::from_fn(&mut pool, |index| {
                        Cancel::new(
                            client.get(WAIT_PATHS[index.get()]),
                            || client.connections() != 0 && !canceled.replace(true),
                            || {
                                let count = waiting.get() + 1;
                                waiting.set(count);
                                if count == 2 {
                                    start.notify().expect("release peer");
                                }
                            },
                        )
                    }).await
                });
                let result = scope.app.block_on(Race::new(deadline, requests))
                    .expect("drive request tasks");
                assert!(matches!(&result, Either::Right(_)), "dispatch wake lost");
                let replies = match result {
                    Either::Right(replies) => Some(replies),
                    Either::Left(()) => None,
                }.expect("request tasks finished");
                let mut ready = replies.expect("join request tasks").enumerate()
                    .filter_map(|(index, reply)| reply.map(|reply| (index, reply)));
                let (index, reply) = ready.next().expect("surviving request");
                assert_eq!(reply.expect("queued response").status().as_u16(), 200);
                assert!(ready.next().is_none(), "one notified request canceled");
                WAIT_PATHS[index]
            },
            recovery::Policy::new(
                |event| std::assert_matches!(event, Reclaimed::Shutdown),
                recover,
            ),
            |result| result.expect("client execution"),
        )
        .expect("client shutdown")
        })
        .expect("peer worker");
    assert_eq!(wire.expect("peer join"), path);
}

#[test]
fn dynamic_dispatch_survives_cancel() {
    AcceptScript::with(
        |acceptor| -> Result<String, peer::Error> {
            let stream = acceptor.accept()?;
            let mut conn = peer::Conn::new(&stream);
            let held = conn.try_request()?.expect("held request");
            assert_eq!(held.target(), "/held");
            conn.write_all(WAIT_RESPONSE)?;
            let path = conn
                .try_request()?
                .expect("surviving request")
                .target()
                .to_owned();
            conn.write_all(WAIT_RESPONSE)?;
            assert!(conn.try_request()?.is_none(), "duplicate request");
            Ok(path)
        },
        |server| {
            let path =
                origins::Plan::new(Config::new(server.addr().to_string()).inflight(1), 1, 8, 1)
                    .expect("client plan")
                    .enter(
                        |mut scope| {
                            let client = scope.handle;
                            let mut held = scope
                                .app
                                .block_on(client.stream_get("/held"))
                                .expect("drive held request")
                                .expect("held stream");
                            let head = scope
                                .app
                                .block_on(held.next_event())
                                .expect("drive held response")
                                .expect("response head")
                                .expect("held response");
                            assert!(matches!(head, response::Event::Head(_)));
                            drop(head);
                            let waiting = Cell::new(0);
                            let released = Cell::new(false);
                            let canceled = Cell::new(false);
                            let gate =
                                Cell::<Option<dope_fiber::context::wake::Waker<'_>>>::new(None);
                            let mut pool = scope
                                .app
                                .block_on(batch::Pool::<2>::acquire())
                                .expect("drive task pool")
                                .expect("request task pool");
                            let waits = batch::Join::from_fn(&mut pool, |index| {
                                Cancel::new(
                                    client.get(WAIT_PATHS[index.get()]),
                                    || released.get() && !canceled.replace(true),
                                    || {
                                        waiting.set(waiting.get() + 1);
                                        if waiting.get() == 2
                                            && let Some(wake) = gate.take()
                                        {
                                            wake.wake();
                                        }
                                    },
                                )
                            });
                            let waiting = &waiting;
                            let released = &released;
                            let gate = &gate;
                            let release = dope_fiber::fiber!('_ => async move {
                                let ready = abi::poll_fn(move |cx| {
                                    if waiting.get() == 2 {
                                        Poll::Ready(())
                                    } else {
                                        gate.set(Some(cx.as_ref().waker()));
                                        Poll::Pending
                                    }
                                });
                                ready.await;
                                while let Some(event) = held.next_event().await {
                                    event.expect("held response event");
                                }
                                // End-of-response detaches the held registration;
                                // its deferred capacity wake runs after this poll.
                                released.set(true);
                            });
                            let deadline = client
                                .sleep(Duration::from_secs(3))
                                .expect("request deadline");
                            // The live connection fills the attempt source, so
                            // only releasing its reply capacity wakes a waiter.
                            let requests = abi::Join::new(release, waits);
                            let result = scope
                                .app
                                .block_on(Race::new(deadline, requests))
                                .expect("drive request tasks");
                            let replies = match result {
                                Either::Right(((), replies)) => Some(replies),
                                Either::Left(()) => None,
                            }
                            .expect("dynamic dispatch wake lost");
                            assert!(canceled.get(), "notified request cancellation reached");
                            let mut ready = replies
                                .expect("join request tasks")
                                .enumerate()
                                .filter_map(|(index, reply)| reply.map(|reply| (index, reply)));
                            let (index, reply) = ready.next().expect("surviving request");
                            assert_eq!(reply.expect("queued response").status().as_u16(), 200);
                            assert!(ready.next().is_none(), "one notified request canceled");
                            WAIT_PATHS[index]
                        },
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("client shutdown");
            assert_eq!(
                server
                    .finish()
                    .expect("peer thread")
                    .expect("peer requests"),
                path
            );
        },
    )
    .expect("start peer");
}
