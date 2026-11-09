pub mod common;

use common::client::recover;
use common::client::{
    config::FLAVORS,
    response::{framing, head::PIPELINED_RESPONSES},
    runtime::{
        cases::{Flavor, events::Reader, pipeline, request::Request},
        origins,
        peer::{self, Conn},
    },
};
use dope::core::{driver::life::recovery, io::Reclaimed};
use dope_fiber::BlockOn as _;
use dope_test::harness::peer::AcceptScript;
use sark_fiber::client::config::Config;
use sark_http::method::Method;

#[test]
fn pipelined_request() {
    for flavor in FLAVORS {
        use common::client::runtime::peer::Error;
        AcceptScript::with(
            |acceptor| {
                let conn_io = acceptor.accept().map_err(Error::Io)?;
                let mut conn = Conn::new(&conn_io);
                {
                    let request = conn.try_request()?.expect("HEAD request");
                    assert_eq!(request.method(), "HEAD");
                    assert_eq!(request.target(), "/head");
                }
                {
                    let request = conn.try_request()?.expect("GET request");
                    assert_eq!(request.method(), "GET");
                    assert_eq!(request.target(), "/get");
                }
                conn.write_all(PIPELINED_RESPONSES).map_err(Error::Io)?;
                Ok::<_, Error>(())
            },
            |peer| {
                let (head, get) = pipeline::try_run(
                    flavor,
                    peer.addr(),
                    |result| result.expect("pipeline response"),
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("pipeline execution");
                assert_eq!(
                    head.data + head.trailers,
                    0,
                    "HEAD response published payload events"
                );
                assert_eq!(head.early_hints, 1, "103 response count");
                assert_eq!(head.status, Some(200), "HEAD final status");
                assert_eq!(
                    get.informational + get.trailers,
                    0,
                    "GET response published an unexpected event"
                );
                assert_eq!(get.status, Some(201), "GET final status");
                assert_eq!(get.body.as_slice(), b"ok", "GET body");
                peer.finish()
                    .expect("peer thread")
                    .expect("finish pipelined HEAD peer");
            },
        )
        .expect("spawn pipelined HEAD peer");
    }
}

#[test]
fn orders_informational() {
    AcceptScript::with(
        |acceptor| {
            peer::serve_each(acceptor, |stream, request| {
                std::assert_matches!(request.target(), "/continue" | "/continue-buffered");
                framing::info(stream)
            })
        },
        |server| {
            let addr = server.addr();

            let (status, body, trailers, info) = Reader::new(
                addr,
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                "/continue",
            )
            .try_run(
                |result| {
                    let read = result.expect("client scenario");
                    (read.status, read.body, read.trailers, read.info)
                },
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("client scenario");
            assert_eq!(status, 200);
            assert_eq!(body, b"ok");
            assert_eq!(trailers, 0);
            assert_eq!(info, 1);

            let fixed = Request::new(
                Flavor::Fixed,
                &(addr).to_string(),
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                Method::GET,
                "/continue-buffered",
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
            let dynamic = origins::Plan::new(Config::new(server.addr().to_string()), 2, 1, 2)
                .expect("dynamic client plan")
                .enter(
                    (),
                    |mut app| {
                        app.app
                            .block_on(app.handle.collect_with(
                                Method::GET,
                                "/continue-buffered",
                                &[],
                                &[],
                            ))
                            .expect("drive HTTP request")
                            .expect("HTTP response")
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("client scope");
            assert_eq!(fixed.status(), dynamic.status());
            assert_eq!(fixed.body().as_slice(), dynamic.body().as_slice());
            server
                .stop()
                .expect("peer thread")
                .expect("stop informational peer");
        },
    )
    .expect("spawn informational peer");
}
