use dope::core::driver::life::recovery;
pub mod common;

use std::{cell::Cell, convert, net::TcpListener, time::Duration};

use common::client::recover;
use common::{
    client::{
        response::head::PIPELINED_RESPONSES,
        runtime::{server::Server, tls::Plan},
        tls,
        tls::{
            TlsDispatch,
            large::{MARKETS_PATHS, *},
            *,
        },
    },
    server,
};
use dope::core::io::Reclaimed;
use dope_fiber::BlockOn as _;
use dope_test::harness::{
    scenario::run::{Entry, Task as _},
    server::Pair,
};
use dope_tls::tls::endpoint;
use sark_client::protocol::response::Event;
use sark_fiber::{
    Config,
    client::{Ready, config},
};
use sark_http::{method::Method, payload::response::status};

fn recover_server(failure: recovery::Recovery<'_, '_>) -> convert::Infallible {
    panic!("native server failure: {:?}", failure.cause())
}

#[test]
fn payload_has_no_prefix() {
    let harness = (Pair::bind()).expect("bind TLS harness");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(5)).build();
    let result = harness.run(
        move |context, listener| {
            let wire =
                endpoint::Config::from(Keys::new(&SEED).server().expect("TLS server config"));
            let mut reports = server::Plan::try_reports(&server).expect("runtime resource claims");
            let errors = Cell::new(Vec::new());
            let result = TlsDispatch::serve_with(
                server.with_bound(listener),
                wire,
                context,
                server::State(&errors),
                Config::new(server::MAX_CONNECTIONS),
                recovery::Policy::new(|event| reports.push(event), recover_server),
                |outcome| {
                    assert!(outcome.is_ok(), "server execution");
                },
            );
            assert!(result.is_ok(), "payload_has_no_prefix server shutdown");
            drop(result);
            let errors = errors.into_inner();
            assert!(errors.is_empty(), "TLS server errors: {errors:?}");
            assert!(
                reports
                    .events()
                    .expect("valid retirement reports")
                    .is_empty(),
                "unexpected reports server operations: {reports:?}"
            );
            Ok::<(), convert::Infallible>(())
        },
        |addr| {
            Plan::new(
                addr,
                config::Config::try_new("test.local").expect("HTTP host allocation"),
                endpoint::Config::from(Keys::new(&SEED).client_plan().expect("TLS client plan")),
                1,
                1,
                1,
            )
            .expect("TLS stream client topology")
            .enter(
                |mut app| {
                    let client = app.handle;
                    app.wait().expect("TLS client readiness");
                    let stream = app
                        .app
                        .block_on(client.stream_get("/xml"))
                        .expect("TLS stream driver");
                    assert!(stream.is_ok(), "TLS response stream");
                    let mut stream = stream.ok().expect("checked TLS response stream");
                    let mut hints = 0;
                    let mut streamed = Vec::new();
                    let mut tails = 0;
                    while let Some(event) = app
                        .app
                        .block_on(stream.next_event())
                        .expect("TLS event driver")
                    {
                        assert!(event.is_ok(), "TLS response event");
                        match event.ok().expect("checked TLS response event") {
                            Event::Head(head) => assert_eq!(head.status(), status::Code::OK),
                            Event::Data(data) => {
                                assert!(!data.as_slice().starts_with(b"HTTP/1.1"));
                                streamed.extend_from_slice(data.as_slice());
                            }
                            Event::Informational(_) => hints += 1,
                            Event::Trailers(_) => tails += 1,
                        }
                    }
                    assert_eq!(hints, 0, "unexpected informational TLS response");
                    assert_eq!(tails, 0, "unexpected TLS response trailers");
                    assert_eq!(streamed, XML);
                    let response = app
                        .app
                        .block_on(client.get("/xml"))
                        .expect("TLS buffered driver");
                    assert!(response.is_ok(), "TLS buffered response");
                    let response = response.ok().expect("checked TLS buffered response");
                    assert_eq!(response.body().contiguous(), Some(XML));
                },
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("run TLS stream client topology");
        },
    );
    (result).as_ref().expect("TLS client/server harness");
}

#[test]
fn lanes_reuse_connection() {
    for lanes in 3..=5 {
        let harness = (Pair::bind()).expect("bind TLS harness");
        let bind = harness.addr();
        let server = server::Plan::new(bind, Duration::from_secs(5)).build();
        let result = harness.run(
            move |context, listener| {
                let wire =
                    endpoint::Config::from(Keys::new(&SEED).server().expect("TLS server config"));
                let mut reports =
                    server::Plan::try_reports(&server).expect("runtime resource claims");
                let errors = Cell::new(Vec::new());
                let result = TlsDispatch::serve_with(
                    server.with_bound(listener),
                    wire,
                    context,
                    server::State(&errors),
                    Config::new(server::MAX_CONNECTIONS),
                    recovery::Policy::new(|event| reports.push(event), recover_server),
                    |outcome| {
                        assert!(outcome.is_ok(), "server execution");
                    },
                );
                assert!(result.is_ok(), "lanes_reuse_connection server shutdown");
                drop(result);
                let errors = errors.into_inner();
                assert!(errors.is_empty(), "TLS server errors: {errors:?}");
                assert!(
                    reports
                        .events()
                        .expect("valid retirement reports")
                        .is_empty(),
                    "unexpected reports server operations: {reports:?}"
                );
                Ok::<(), convert::Infallible>(())
            },
            move |addr| {
                Plan::new(
                    addr,
                    config::Config::try_new("test.local")
                        .expect("HTTP host allocation")
                        .timeout(Duration::from_secs(2))
                        .inflight(1),
                    endpoint::Config::from(
                        Keys::new(&SEED).client_plan().expect("TLS client plan"),
                    ),
                    lanes,
                    8,
                    1,
                )
                .expect("chunked TLS client topology")
                .enter(
                    |mut app| {
                        let client = app.handle;
                        app.wait().expect("TLS client readiness");
                        let result = app.app.block_on(dope_fiber::fiber!('_ => async move {
                            for _ in 0..4 {
                                let response = client.get("/chunk").await?;
                                assert_eq!(response.status(), status::Code::OK);
                                assert_eq!(response.body().contiguous(), Some(b"ok".as_slice()));
                                assert_eq!(client.connections(), 1);
                            }
                            Ok::<_, sark_client::Failure<_>>(())
                        })).expect("sequential chunked TLS driver");
                        assert!(result.is_ok(), "sequential chunked TLS responses");
                    },
                    recovery::Policy::new(
                        |event| {
                            assert!(
                                matches!(event, Reclaimed::Shutdown),
                                "unexpected client retirement: {event:?}"
                            )
                        },
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("run chunked TLS client topology");
            },
        );
        (result)
            .as_ref()
            .expect("TLS persistent chunked client/server harness");
    }
}

#[test]
fn reuses_connection() {
    for lanes in 3..=5 {
        let listener = (TcpListener::bind("127.0.0.1:0")).expect("bind raw TLS server");
        let addr = (listener.local_addr()).expect("raw TLS server address");
        let server = Entry(move || {
            Server { listener }.try_run(
                tls::Keys::new(&tls::SEED)
                    .server()
                    .expect("TLS server config"),
                [()],
                |mut peer, ()| {
                    peer.try_ready()?;
                    for path in MARKETS_PATHS {
                        peer.try_request("GET", path)?;
                        try_response(&mut peer)?;
                    }
                    peer.try_closed()
                },
            )
        });
        let ((), served) = server.with(|| {
        Plan::new(
            addr,
            config::Config::try_new("test.local").expect("HTTP host allocation").timeout(Duration::from_secs(5)).inflight(1),
            endpoint::Config::from(Keys::new(&SEED).client_plan().expect("TLS client plan")),
            lanes,
            8,
            1,
        )
        .expect("multi-lane TLS client topology")
        .enter(
|mut app| {
                let client = app.handle;
                app.wait().expect("TLS client readiness");
                let result = app.app.block_on(dope_fiber::fiber!('_ => async move {
                    for path in MARKETS_PATHS {
                        let response = client.get(path).await?;
                        assert_eq!(response.status(), status::Code::OK);
                        assert_eq!(response.body().len(), BODY_LEN);
                        assert!(response.body().segments().all(|segment| segment.iter().all(|byte| *byte == b'x')));
                        assert_eq!(client.connections(), 1);
                    }
                    Ok::<_, sark_client::Failure<_>>(())
                })).expect("multi-lane TLS response driver");
                assert!(result.is_ok(), "multi-lane TLS responses");
            },
recovery::Policy::new(|event| assert!(matches!(event, Reclaimed::Shutdown), "unexpected client retirement: {event:?}"), recover),
|result| result.expect("client execution"),
)
        .expect("run multi-lane TLS client topology");
        }).expect("raw TLS server thread");
        served.expect("TLS server execution");
    }
}

#[test]
fn keeps_pipeline_order() {
    let listener = (TcpListener::bind("127.0.0.1:0")).expect("bind raw TLS server");
    let addr = (listener.local_addr()).expect("raw TLS server address");
    let server = Entry(move || {
        Server { listener }.try_run(
            tls::Keys::new(&tls::SEED)
                .server()
                .expect("TLS server config"),
            [()],
            |mut peer, ()| {
                peer.try_ready()?;
                peer.try_request("HEAD", "/head")?;
                peer.try_request("GET", "/get")?;
                peer.try_write(PIPELINED_RESPONSES)?;
                peer.try_closed()
            },
        )
    });
    let ((), served) = server.with(|| {
    Plan::new(
        addr,
        config::Config::try_new("test.local").expect("HTTP host allocation").timeout(Duration::from_secs(5))
            .request_pool(2, 4096).queue_bytes(4096).inflight(2),
        endpoint::Config::from(Keys::new(&SEED).client_plan().expect("TLS client plan")),
        1,
        4,
        1,
    )
    .expect("pipelined TLS client topology")
    .enter(
|mut app| {
            let client = app.handle;
            app.wait().expect("TLS client readiness");
            let result = app.app.block_on(dope_fiber::fiber!('_ => async move {
                let mut head = client.stream_with(Method::HEAD, "/head", &[], &[]).await?;
                let mut get = client.stream_get("/get").await?;
                let mut hints = 0;
                let mut head_bad = 0;
                let mut status = None;
                while let Some(event) = head.next_event().await {
                    match event? {
                        Event::Informational(head) => hints += usize::from(head.status().as_u16() == 103),
                        Event::Head(head) => status = Some(head.status().as_u16()),
                        Event::Data(_) | Event::Trailers(_) => head_bad += 1,
                    }
                }
                let mut get_code = None;
                let mut get_bad = 0;
                let mut body = Vec::new();
                while let Some(event) = get.next_event().await {
                    match event? {
                        Event::Head(head) => get_code = Some(head.status().as_u16()),
                        Event::Data(data) => body.extend_from_slice(data.as_slice()),
                        Event::Informational(_) | Event::Trailers(_) => get_bad += 1,
                    }
                }
                assert_eq!(head_bad, 0, "HEAD response published payload events");
                assert_eq!(hints, 1);
                assert_eq!(status, Some(200));
                assert_eq!(get_bad, 0, "GET response published an unexpected event");
                assert_eq!(get_code, Some(201));
                assert_eq!(body, b"ok");
                Ok::<_, sark_client::Failure<_>>(())
            })).expect("pipelined TLS response driver");
            assert!(result.is_ok(), "pipelined TLS responses");
        },
recovery::Policy::new(|event| assert!(matches!(event, Reclaimed::Shutdown), "unexpected client retirement: {event:?}"), recover),
|result| result.expect("client execution"),
)
    .expect("run pipelined TLS client topology");
    }).expect("raw TLS server thread");
    served.expect("TLS server execution");
}
