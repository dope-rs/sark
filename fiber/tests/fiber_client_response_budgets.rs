use common::client::recover;
use common::client::runtime::{cases::Flavor, origins, peer, peer::Conn, pools};
use dope::{
    core::{driver::life::recovery, io::Reclaimed},
    manifold::client::attempt::queue::mailbox,
    runtime::executor::drive,
};
use dope_test::harness::peer::{AcceptScript, Stream};
pub mod common;

use std::{convert::Infallible, str, time::Duration};

use common::client::{config::FLAVORS, retry::AGGRESSIVE, runtime::cases::request::Request};
use dope_fiber::{
    BlockOn as _,
    abi::Pending,
    reply::usage::{Capacity, Resource},
    task::time::budget,
};
use o3::buffer::{hold, storage::provided::account};
use sark_client::{Error, Failure, Safety, capacity, protocol, timeout};
use sark_fiber::client::config::{Config, Stage};
use sark_http::method::Method;

#[test]
fn response_at_cap_succeeds() {
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(
                acceptor,
                |stream: &mut &Stream<'_>, _: peer::Request<'_>| {
                    let body = "Hello, World!";
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    stream.write_all(resp.as_bytes())
                },
            )
        },
        |server| {
            for flavor in FLAVORS {
                let config =
                    Config::new(server.addr().to_string()).wire_limit("Hello, World!".len());
                let resp = Request::new(
                    flavor,
                    &server.addr().to_string(),
                    config,
                    Method::GET,
                    "/",
                    &[],
                    &[],
                )
                .try_run(
                    |result| result.expect("HTTP response"),
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("HTTP request execution");
                assert_eq!(resp.status().as_u16(), 200);
                assert_eq!(
                    str::from_utf8(resp.body().as_slice()).expect("peer body is source UTF-8"),
                    "Hello, World!"
                );
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop response-cap peer");
        },
    )
    .expect("spawn response-cap peer");
}

#[test]
fn response_over_cap_errors() {
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(
                acceptor,
                |stream: &mut &Stream<'_>, _: peer::Request<'_>| {
                    let body = vec![b'x'; 4096];
                    let mut resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .into_bytes();
                    resp.extend_from_slice(&body);
                    stream.write_all(&resp)
                },
            )
        },
        |server| {
            for flavor in FLAVORS {
                let config = Config::new(server.addr().to_string()).wire_limit(1024);
                Request::new(
                    flavor,
                    &server.addr().to_string(),
                    config,
                    Method::GET,
                    "/",
                    &[],
                    &[],
                )
                .try_run(
                    |outcome| {
                        let error = (outcome).err().expect("over-cap response must fail");
                        assert!(matches!(
                            error.cause(),
                            Error::Protocol(protocol::Error::BodyLimit)
                        ));
                        assert!((error.safety()) == (Safety::PossiblyCommitted));
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("HTTP request execution");
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop oversized-response peer");
        },
    )
    .expect("spawn oversized-response peer");
}

#[test]
fn queue_limit_at_delivery() {
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(
                acceptor,
                |stream: &mut &Stream<'_>, _: peer::Request<'_>| {
                    let body = vec![b'x'; 4096];
                    let mut response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .into_bytes();
                    response.extend_from_slice(&body);
                    stream.write_all(&response)
                },
            )
        },
        |server| {
            for flavor in FLAVORS {
                let config = Config::new(server.addr().to_string())
                    .wire_limit(4096)
                    .queue_bytes(64);
                Request::new(
                    flavor,
                    &server.addr().to_string(),
                    config,
                    Method::GET,
                    "/",
                    &[],
                    &[],
                )
                .try_run(
                    |outcome| {
                        let error = (outcome)
                            .err()
                            .expect("response queue must reject an oversized live reply");
                        assert!(matches!(
                            error.cause(),
                            Error::Capacity(capacity::Error::ResponseCapacity(
                                Capacity::Limit {
                                    resource: Resource::Bytes,
                                    limit,
                                    ..
                                }
                            )) if limit.get() == 64
                        ));
                        assert!((error.safety()) == (Safety::PossiblyCommitted));
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("HTTP request execution");
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop queue-capacity peer");
        },
    )
    .expect("spawn queue-capacity peer");
}

#[test]
fn wire_memory_at_ingress() {
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(
                acceptor,
                |stream: &mut &Stream<'_>, _: peer::Request<'_>| {
                    let body = vec![b'x'; 4096];
                    let mut response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .into_bytes();
                    response.extend_from_slice(&body);
                    stream.write_all(&response)
                },
            )
        },
        |server| {
            for flavor in FLAVORS {
                let config = Config::new(server.addr().to_string())
                    .wire_limit(4096)
                    .wire_memory(256)
                    .queue_bytes(8192);
                Request::new(
                    flavor,
                    &server.addr().to_string(),
                    config,
                    Method::GET,
                    "/",
                    &[],
                    &[],
                )
                .try_run(
                    |outcome| {
                        let error = (outcome)
                            .err()
                            .expect("response ingress must honor its resident capacity");
                        let budget = match error.cause() {
                            Error::Protocol(protocol::Error::Retention(hold::Error::Account(
                                account::Cause::Budget(budget),
                            ))) => Some(budget),
                            _ => None,
                        }
                        .expect("response retention must report its exact budget failure");
                        let (need, spare) = budget.into_parts();
                        assert!(need.get() > 256);
                        assert!(spare <= 256);
                        assert!((error.safety()) == (Safety::PossiblyCommitted));
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("HTTP request execution");
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop resident-capacity peer");
        },
    )
    .expect("spawn resident-capacity peer");
}

#[test]
fn staging_ceiling_closes() {
    const HEAD_LIMIT: usize = 16 * 1024;

    for flavor in FLAVORS {
        AcceptScript::with(
            move |acceptor| {
                use common::client::runtime::peer::Error;
                let conn_io = acceptor.accept().map_err(Error::Io)?;
                let mut conn = Conn::new(&conn_io);
                conn.try_request()?.expect("client request");

                let mut prefix = b"HTTP/1.1 200 OK\r\nX-Fill: ".to_vec();
                prefix.resize(HEAD_LIMIT, b'x');
                conn.write_all(&prefix).map_err(Error::Io)?;
                match peer::closed(&mut conn) {
                    Err(Error::Io(dope_test::harness::peer::Error::Syscall(error)))
                        if error.is_aborted() || error.is_reset() =>
                    {
                        Ok(())
                    }
                    result => result,
                }
            },
            |server| {
                let config = Config::new(server.addr().to_string())
                    .head_limit(HEAD_LIMIT)
                    .wire_memory(HEAD_LIMIT * 2)
                    .queue_bytes(HEAD_LIMIT * 2);
                let mut checked = false;
                let mut check = |outcome: Result<_, Failure<Infallible, mailbox::Error<'_>>>| {
                    let error = outcome
                        .err()
                        .expect("an incomplete head cannot grow beyond the staging ceiling");
                    assert!(
                        matches!(
                            error.cause(),
                            Error::Capacity(capacity::Error::ResponseStaging)
                        ) && error.safety() == Safety::PossiblyCommitted,
                        "{error:?}",
                    );
                    checked = true;
                };
                let policy = recovery::Policy::new(
                    |event| std::assert_matches!(
                        event,
                        Reclaimed::Shutdown | Reclaimed::Socket {
                            result: Ok(()),
                            close: None,
                            ..
                        }
                    ),
                    recover,
                );
                match flavor {
                    Flavor::Fixed => pools::Plan::new(server.addr(), config, 1, 1)
                        .expect("fixed client plan")
                        .enter(
                            server.completion(),
                            |mut app| {
                                app.wait().expect("active endpoint");
                                let client = app.handle;
                                let result = app.app.block_on(dope_fiber::fiber!('_ => async move {
                                    check(client.collect_with(Method::GET, "/", &[], &[])
                                        .await.map_err(Failure::widen));
                                    Pending::<()>::new().await;
                                }));
                                std::assert_matches!(result, Err(drive::Error::Shutdown));
                            },
                            policy,
                            |result| result.expect("client execution"),
                        )
                        .expect("fixed HTTP request execution"),
                    Flavor::OnDemand => origins::Plan::new(config, 1, 1, 1)
                        .expect("on-demand client plan")
                        .enter(
                            server.completion(),
                            |mut app| {
                                let client = app.handle;
                                let result = app.app.block_on(dope_fiber::fiber!('_ => async move {
                                    check(client.collect_with(Method::GET, "/", &[], &[]).await);
                                    Pending::<()>::new().await;
                                }));
                                std::assert_matches!(result, Err(drive::Error::Shutdown));
                            },
                            policy,
                            |result| result.expect("client execution"),
                        )
                        .expect("on-demand HTTP request execution"),
                }
                assert!(checked, "the exact staging failure was observed");
                server
                    .finish()
                    .expect("peer thread")
                    .expect("the exact staging ceiling must close the client connection");
            },
        )
        .expect("spawn staging-capacity peer");
    }
}

#[test]
fn timeout_retry() {
    let short = Duration::from_millis(50);
    let long = Duration::from_secs(3);
    for flavor in FLAVORS {
        for (post, total, head) in [
            (false, long, short),
            (false, short, long),
            (true, long, short),
        ] {
            let retry = !post && head < total;
            let elapsed = if head < total {
                budget::Elapsed::Limit
            } else {
                budget::Elapsed::Budget
            };
            AcceptScript::with(
                move |acceptor| {
                    let accepted = acceptor.accept()?;
                    let mut first = peer::Conn::new(&accepted);
                    assert_eq!(
                        first.try_request()?.expect("first request").method(),
                        if post { "POST" } else { "GET" }
                    );
                    peer::closed(&mut first)?;
                    if retry {
                        let accepted = acceptor.accept()?;
                        let mut second = peer::Conn::new(&accepted);
                        assert_eq!(second.try_request()?.expect("retried get").method(), "GET");
                        second.write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                        )?;
                        peer::closed(&mut second)?;
                    }
                    peer::empty(acceptor)
                },
                |server| {
                    let addr = server.addr().to_string();
                    let config = Config::try_new(&addr)
                        .expect("HTTP host allocation")
                        .retry(AGGRESSIVE)
                        .timeout(total)
                        .limit(Stage::Head, head);
                    let method = if post { Method::POST } else { Method::GET };
                    Request::new(flavor, &addr, config, method, "/", &[], &[])
                        .try_run(
                            |outcome| {
                                if retry {
                                    assert_eq!(
                                        outcome.expect("retried response").status().as_u16(),
                                        200
                                    );
                                } else {
                                    let failure = outcome.err().expect("terminal timeout");
                                    assert!(matches!(failure.cause(), Error::Timeout(error)
                                        if error.phase() == timeout::Phase::Head
                                            && error.elapsed() == elapsed));
                                    assert_eq!(failure.attempt(), 1);
                                    assert_eq!(failure.safety(), Safety::PossiblyCommitted);
                                }
                            },
                            recovery::Policy::new(
                                |event| std::assert_matches!(event, Reclaimed::Shutdown),
                                recover,
                            ),
                            |result| result.expect("client execution"),
                        )
                        .expect("HTTP request execution");
                    server
                        .finish()
                        .expect("peer thread")
                        .expect("response peer");
                },
            )
            .expect("response peer");
        }
    }
}
