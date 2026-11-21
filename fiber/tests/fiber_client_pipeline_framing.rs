pub mod common;

use common::client::assert_shutdown;
use common::client::{
    config::FLAVORS,
    response::{framing, head::PIPELINED_RESPONSES},
    runtime::{
        cases::{Flavor, events, pipeline, request::Request},
        origins,
        peer::{self, Conn},
    },
};
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
                    assert_shutdown,
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
    peer::with_wire(
        &["/continue", "/continue-buffered"],
        framing::INFO,
        |addr| {
            let observed = events::read(addr, "/continue");
            assert_eq!(observed.status, 200);
            assert_eq!(observed.body, b"ok");
            assert_eq!(observed.trailers, 0);
            assert_eq!(observed.info, 1);

            let fixed = Request::get(
                Flavor::Fixed,
                addr,
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                "/continue-buffered",
            )
            .collect();
            let dynamic = origins::Plan::new(Config::new(addr.to_string()), 2, 1, 2)
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
                    assert_shutdown,
                )
                .expect("client scope");
            assert_eq!(fixed.status(), dynamic.status());
            assert_eq!(fixed.body().as_slice(), dynamic.body().as_slice());
        },
    );
}
