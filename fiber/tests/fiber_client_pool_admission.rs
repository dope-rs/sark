use common::client::recover;
use common::client::{
    pool::{BURST, WINDOW},
    runtime::peer,
};
use dope::core::driver::life::recovery;
use dope_test::alloc::Tracker;

#[global_allocator]
static ALLOC: Tracker = Tracker::new();

use common::client::runtime::peer::Conn;
use dope_test::{fibers, harness::peer::AcceptScript};
pub mod common;

use std::{
    num::NonZero,
    thread,
    time::{Duration, Instant},
};

use common::client::{
    config::{Config as _, FLAVORS},
    runtime::{
        calls,
        cases::{
            Batch, Flavor,
            stream::{Scenario, Stream},
        },
        peer::Response as _,
        pools,
    },
};
use dope::core::io::Reclaimed;
use dope_fiber::{BlockOn as _, abi::Join};
use sark_fiber::client::config::Config;
use sark_http::method::Method;

#[test]
fn active_skips_idle_lease() {
    for flavor in FLAVORS {
        let count = AcceptScript::with(
            |acceptor| -> Result<_, peer::Error> {
                let conn_io = acceptor.accept()?;
                let mut conn = Conn::new(&conn_io);
                let request = conn.try_request()?.expect("read first active request");
                assert_eq!(request.target(), "/first");
                conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n")?;
                let request = conn
                    .try_request()?
                    .expect("active connection must admit the second request");
                assert_eq!(request.target(), "/second");
                conn.write_all(b"done")?;
                conn.write_all(
                    "HTTP/1.1 200 OK"
                        .response(&[("Connection", "close"), ("Content-Length", "2")], b"ok")
                        .as_slice(),
                )?;
                Ok(1)
            },
            |server| {
                let addr = server.addr();

                let status = Stream::new(
                    flavor,
                    addr,
                    flavor
                        .try_config(addr)
                        .expect("HTTP host allocation")
                        .timeout(Duration::from_secs(3))
                        .idle_timeout(Duration::from_millis(100))
                        .inflight(2),
                    NonZero::new(1).expect("client capacity"),
                    Scenario::Interleave {
                        first: "/first",
                        second: "/second",
                        gap: Duration::from_millis(300),
                    },
                )
                .try_run(
                    |result| result.expect("client scenario"),
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("client scenario");
                assert_eq!(status, 200);
                server
                    .finish()
                    .expect("peer thread")
                    .expect("active response peer")
            },
        )
        .expect("spawn active response peer");
        assert_eq!(count, 1);
    }
}

#[test]
fn close_head_revokes_pool() {
    AcceptScript::with(
        |acceptor| -> Result<_, peer::Error> {
            let first_io = acceptor.accept()?;
            let mut first = Conn::new(&first_io);
            let request = first.try_request()?.expect("read first request");
            assert_eq!(request.target(), "/first");
            first
                .write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\n")?;

            let other_io = acceptor.accept()?;
            let mut second = Conn::new(&other_io);
            let request = second.try_request()?.expect("read second request");
            assert_eq!(request.target(), "/second");
            let mut byte = [0];
            match first.try_read(&mut byte) {
                Ok(None | Some(0)) => {}
                Ok(Some(read)) => {
                    assert_eq!(read, 0, "second request leaked onto the closing connection")
                }
                Err(error) => return Err(error.into()),
            }
            first.write_all(b"done")?;
            second.write_all(
                "HTTP/1.1 200 OK"
                    .response(&[("Connection", "close"), ("Content-Length", "2")], b"ok")
                    .as_slice(),
            )?;
            Ok(2)
        },
        |server| {
            let addr = server.addr();

            let status = Stream::new(
                Flavor::Fixed,
                addr,
                Config::try_new("127.0.0.1")
                    .expect("HTTP host allocation")
                    .timeout(Duration::from_secs(3)),
                NonZero::new(2).expect("client capacity"),
                Scenario::Interleave {
                    first: "/first",
                    second: "/second",
                    gap: Duration::ZERO,
                },
            )
            .try_run(
                |result| result.expect("client scenario"),
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("client scenario");
            assert_eq!(status, 200);
            assert_eq!(
                server
                    .finish()
                    .expect("peer thread")
                    .expect("close-head peer"),
                2
            );
        },
    )
    .expect("spawn close-head peer")
}

#[test]
fn drop_seals_same_poll() {
    for flavor in FLAVORS {
        AcceptScript::with(
            |acceptor| -> Result<_, peer::Error> {
                let first_io = acceptor.accept()?;
                let mut wire = Vec::new();
                first_io.read_to_end(&mut wire)?;
                assert!(
                    !wire
                        .windows(b"POST /replacement ".len())
                        .any(|window| window == b"POST /replacement "),
                    "same-poll replacement leaked onto the closing generation",
                );

                let next_io = acceptor.accept()?;
                let mut next = Conn::new(&next_io);
                let request = next.try_request()?.expect("read replacement request");
                assert_eq!(request.method(), "POST");
                assert_eq!(request.target(), "/replacement");
                assert_eq!(request.body(), b"exactly");
                next.write_all(
                    "HTTP/1.1 200 OK"
                        .response(&[("Connection", "close"), ("Content-Length", "2")], b"ok")
                        .as_slice(),
                )?;
                Ok(())
            },
            |server| {
                let addr = server.addr();

                let status = Stream::new(
                    flavor,
                    addr,
                    flavor
                        .try_config(addr)
                        .expect("HTTP host allocation")
                        .timeout(Duration::from_secs(2))
                        .request_pool(2, 2048),
                    NonZero::new(1).expect("client capacity"),
                    Scenario::Cancel {
                        first: "/abandoned",
                        method: Method::POST,
                        second: "/replacement",
                        body: b"exactly",
                    },
                )
                .try_run(
                    |result| result.expect("client scenario"),
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("client scenario");
                assert!((status) == (200));
                server
                    .finish()
                    .expect("peer thread")
                    .expect("stream-drop peer");
            },
        )
        .expect("spawn stream-drop peer")
    }
}

#[test]
fn burst_socket_per_request() {
    AcceptScript::with(
        |acceptor| -> Result<_, peer::Error> {
            let deadline = Instant::now() + WINDOW;
            let mut conns = Vec::new();
            while conns.len() < BURST && Instant::now() < deadline {
                match acceptor.try_accept()? {
                    Some(conn) => conns.push(conn),
                    None => thread::sleep(Duration::from_millis(1)),
                }
            }
            for accepted in &conns {
                let mut conn = Conn::new(accepted);
                if conn.try_request()?.is_some() {
                    conn.write_all(
                        "HTTP/1.1 200 OK"
                            .response(&[("Content-Length", "2")], b"ok")
                            .as_slice(),
                    )?;
                }
            }
            Ok(conns.len())
        },
        |server| {
            let addr = server.addr();
            let plan = pools::Plan::new(
                addr,
                Config::try_new("127.0.0.1")
                    .expect("HTTP host allocation")
                    .timeout(Duration::from_secs(2)),
                BURST,
                4,
            )
            .expect("burst plan");
            let codes = plan
                .enter(
                    |mut app| {
                        let client = app.handle;
                        app.wait().expect("connector admits its endpoint");
                        let burst = Join::new(
                            Join::new(client.get("/1"), client.get("/2")),
                            Join::new(client.get("/3"), client.get("/4")),
                        );
                        let ((a, b), (c, d)) = app.app.block_on(burst).expect("burst drive");
                        [a, b, c, d].map(|result| result.expect("burst response").status().as_u16())
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("burst execution");
            assert_eq!(codes, [200; BURST]);
            assert_eq!(
                server.finish().expect("peer thread").expect("burst peer"),
                BURST,
                "a concurrent burst must own one socket per in-flight request"
            );
        },
    )
    .expect("spawn burst peer")
}

#[test]
fn queued_body_yields() {
    AcceptScript::with(
        |acceptor| {
            let conn_io = acceptor.accept()?;
            let mut conn = Conn::new(&conn_io);
            conn.pipeline()
        },
        |server| {
            pools::Plan::new(server.addr(), Config::new(server.addr().to_string()), 1, 2)
                .expect("fixed plan")
                .enter(
                    |mut app| {
                        app.wait().expect("connection");
                        let client = app.handle;
                        let queued = fibers::TEST
                            .try_run(
                                &mut app.app,
                                calls::Queued::new(client.get("/first"), client.get("/second")),
                            )
                            .expect("queued collection");
                        let (turns, first, second) = queued
                            .map_err(|first| first.map(|response| response.status()))
                            .expect("first collection must remain pending");
                        assert_eq!(first.expect("first response").body().len(), calls::BODY);
                        assert_eq!(second.expect("second response").status().as_u16(), 204);
                        assert!(
                            turns >= calls::CHUNKS,
                            "resident events bypassed the continuation budget: {turns}"
                        );
                    },
                    recovery::Policy::new(|_| {}, recover),
                    |result| result.expect("client execution"),
                )
                .expect("fixed shutdown");
            server
                .finish()
                .expect("peer thread")
                .expect("pipeline shutdown");
        },
    )
    .expect("pipeline peer");
}

#[test]
fn recovers_stale_idle() {
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(acceptor, |stream, _| {
                let response = "HTTP/1.1 200 OK".response(&[("Content-Length", "2")], b"ok");
                stream.write_all(response.as_slice())
            })
        },
        |server| {
            let addr = server.addr();

            let config = Config::try_new("127.0.0.1")
                .expect("HTTP host allocation")
                .timeout(Duration::from_secs(2))
                .idle_timeout(Duration::from_millis(100));

            let codes = Batch::new(
                Flavor::Fixed,
                addr,
                config,
                NonZero::new(2).expect("client capacity"),
                &["/1", "/2", "/3", "/4"],
                &["/5", "/6", "/7", "/8"],
                Some(Duration::from_millis(400)),
            )
            .try_run(
                |result| result.expect("batch response").status().as_u16(),
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("client scenario");
            assert_eq!(codes, vec![200, 200, 200, 200, 200, 200, 200, 200]);
            server
                .stop()
                .expect("peer thread")
                .expect("stop stale-recycle peer");
        },
    )
    .expect("spawn stale-recycle peer")
}
