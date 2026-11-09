use std::num::NonZero;

use common::client::recover;
use common::client::runtime::peer;
use dope::core::driver::life::recovery;
use dope_test::harness::peer::AcceptScript;
pub mod common;

use common::client::{
    response::owners::{self, BODY_LEN, CHUNK, SCOPED_ALLOCATIONS, Value},
    runtime::{origins, pools},
};
use dope::core::io::Reclaimed;
use dope_fiber::BlockOn;
use dope_test::alloc::Tracker;
use sark_client::{
    Error, Safety,
    reply::{self, detach, detach::Part},
};
use sark_fiber::client::{
    config::Config,
    process::{self, Process as _},
};
use sark_http::{method::Method, payload::response::status};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn shape_and_ceiling() {
    AcceptScript::with(
        move |acceptor| {
            peer::serve(acceptor, |request: peer::Request<'_>| {
                assert_eq!(request.method(), "GET");
                assert!(request.body().is_empty());
                owners::wire(NonZero::new(CHUNK).expect("body chunk"))
            })
        },
        |server| {
            let check = |response: Value<'_, '_>| match response {
                Value::Scoped(response) => check_scope(response),
                Value::Retained(response) => check_keep(response),
                Value::Contiguous(response) => check_flat(response),
            };
            let fixed = pools::Plan::new(
                server.addr(),
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                1,
                1,
            )
            .expect("fixed topology")
            .enter(
                (),
                |mut app| {
                    let client = app.handle;
                    app.wait().expect("fixed client readiness");
                    owners::try_measure(
                        &mut app.app,
                        client.send(Method::GET, "/warm", &[]),
                        || client.send(Method::GET, "/scoped", &[]),
                        || client.retain(Method::GET, "/retained", &[]),
                        || client.collect(Method::GET, "/flat", &[]),
                        check,
                    )
                    .expect("measure fixed responses")
                },
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("fixed shutdown");
            let dynamic = origins::Plan::new(Config::new(server.addr().to_string()), 1, 1, 1)
                .expect("on-demand topology")
                .enter(
                    (),
                    |mut app| {
                        let client = app.handle;
                        owners::try_measure(
                            &mut app.app,
                            client.send(Method::GET, "/warm", &[]),
                            || client.send(Method::GET, "/scoped", &[]),
                            || client.retain(Method::GET, "/retained", &[]),
                            || client.collect(Method::GET, "/flat", &[]),
                            check,
                        )
                        .expect("measure on-demand responses")
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("on-demand shutdown");
            for [scoped, retained, flat] in [fixed, dynamic] {
                assert_eq!(
                    scoped.0, SCOPED_ALLOCATIONS,
                    "scoped E2E response allocated {scoped:?}"
                );
                assert!(
                    retained.0 <= scoped.0 + 2,
                    "retained E2E response allocated {retained:?} over baseline {scoped:?}"
                );
                assert_eq!(
                    flat.0,
                    scoped.0 + 1,
                    "flat E2E response allocated {flat:?} over baseline {scoped:?}"
                );
                assert!(
                    retained.1 < scoped.1 + CHUNK,
                    "retained copied a body-sized region: {retained:?} over baseline {scoped:?}"
                );
                assert!(
                    flat.1 >= scoped.1 + BODY_LEN,
                    "flat did not materialize the full body: {flat:?} over baseline {scoped:?}"
                );
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop detach peer");
        },
    )
    .expect("spawn detach peer");
}

#[test]
fn retained_keeps_backing() {
    AcceptScript::with(
        move |acceptor| {
            peer::serve(acceptor, |request: peer::Request<'_>| {
                assert_eq!(request.method(), "GET");
                assert!(request.body().is_empty());
                owners::wire(NonZero::new(CHUNK).expect("body chunk"))
            })
        },
        |server| {
            for work in [false, true] {
                let response = pools::Plan::new(
                    server.addr(),
                    Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                    1,
                    1,
                )
                .expect("fixed topology")
                .enter(
                    (),
                    |mut app| {
                        let client = app.handle;
                        app.wait().expect("fixed client readiness");
                        let result = app
                            .app
                            .block_on(client.process(
                                Method::GET,
                                "/retained",
                                &[],
                                &[],
                                |response, _, _| {
                                    if work {
                                        Err(process::Error::Work(response))
                                    } else {
                                        Ok(response)
                                    }
                                },
                            ))
                            .expect("drive processed request")
                            .expect("processed response");
                        let scoped = if work {
                            result.err().expect("retained work error")
                        } else {
                            result.ok().expect("retained successful value")
                        };
                        let backing = [
                            scoped.body().segment(0).expect("first segment").as_ptr(),
                            scoped.body().segment(1).expect("second segment").as_ptr(),
                        ];
                        check_scope(
                            app.app
                                .block_on(client.send(Method::GET, "/reuse", &[]))
                                .expect("drive reuse")
                                .expect("reused response"),
                        );
                        let retained = scoped.try_retain().expect("retain response");
                        assert_eq!(
                            retained
                                .body()
                                .segments()
                                .nth(0)
                                .expect("retained first")
                                .as_ptr(),
                            backing[0]
                        );
                        assert_eq!(
                            retained
                                .body()
                                .segments()
                                .nth(1)
                                .expect("retained second")
                                .as_ptr(),
                            backing[1]
                        );
                        retained
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("fixed shutdown");
                check_keep(response);
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop detach peer");
        },
    )
    .expect("spawn detach peer");
}

#[test]
fn failures_keep_provenance() {
    use o3::buffer::segments::Count;

    AcceptScript::with(
        move |acceptor| {
            peer::serve(acceptor, |request: peer::Request<'_>| {
                assert_eq!(request.method(), "GET");
                assert!(request.body().is_empty());
                owners::wire(NonZero::new(CHUNK).expect("body chunk"))
            })
        },
        |server| {
            pools::Plan::new(
                server.addr(),
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                1,
                1,
            )
            .expect("fixed topology")
            .enter(
                (),
                |mut app| {
                    let client = app.handle;
                    app.wait().expect("fixed client readiness");
                    let warm = app
                        .app
                        .block_on(client.send(Method::GET, "/warm", &[]))
                        .expect("drive warm request")
                        .expect("warm response");
                    check_scope(warm);

                    let retained = (Tracker::fail_after(SCOPED_ALLOCATIONS, || {
                        app.app
                            .block_on(client.retain(Method::GET, "/retained", &[]))
                    })
                    .expect("drive retained failure"))
                    .err()
                    .expect("retained detach must fail");
                    let (cause, safety) = retained.into_parts();
                    assert!((safety) == (Safety::PossiblyCommitted));
                    assert!(matches!(
                        &cause,
                        Error::Detach(detach::Error::Metadata {
                            segments: Count::TWO,
                            ..
                        })
                    ));

                    let flat = (Tracker::fail_after(SCOPED_ALLOCATIONS, || {
                        app.app.block_on(client.collect(Method::GET, "/flat", &[]))
                    })
                    .expect("drive flat failure"))
                    .err()
                    .expect("flat detach must fail");
                    let (cause, safety) = flat.into_parts();
                    assert!((safety) == (Safety::PossiblyCommitted));
                    let (parts, bytes) = match cause {
                        Error::Detach(detach::Error::StorageExhausted {
                            parts,
                            requested: bytes,
                            ..
                        }) => Some((parts, bytes)),
                        _ => None,
                    }
                    .expect("flat detach must report storage exhaustion");
                    assert!(parts.contains(Part::Body));
                    assert!(bytes >= BODY_LEN);
                },
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("fixed shutdown");
            server
                .stop()
                .expect("peer thread")
                .expect("stop detach peer");
        },
    )
    .expect("spawn detach peer");
}

fn check_scope(response: reply::Scoped<'_, '_>) {
    assert_eq!(response.status(), status::Code::OK);
    assert_eq!(response.body().len(), BODY_LEN);
    assert_eq!(response.body().segments().len(), 2);
    assert_eq!(
        response.trailers().expect("trailers").get("x-checksum"),
        Some(b"42".as_slice())
    );
}

fn check_keep(response: reply::Retained) {
    assert_eq!(response.status(), status::Code::OK);
    assert_eq!(
        response.headers().get("x-origin"),
        Some(b"fixture".as_slice())
    );
    assert_eq!(response.body().len(), BODY_LEN);
    assert_eq!(response.body().segments().len(), 2);
    assert!(response.body().contiguous().is_none());
    assert!(
        response
            .body()
            .segments()
            .nth(0)
            .expect("first segment")
            .iter()
            .all(|byte| *byte == b'a')
    );
    assert!(
        response
            .body()
            .segments()
            .nth(1)
            .expect("second segment")
            .iter()
            .all(|byte| *byte == b'b')
    );
    assert_eq!(
        response.trailers().expect("trailers").get("x-checksum"),
        Some(b"42".as_slice())
    );
}

fn check_flat(response: reply::Contiguous) {
    assert_eq!(response.status(), status::Code::OK);
    assert_eq!(
        response.headers().get("x-origin"),
        Some(b"fixture".as_slice())
    );
    assert_eq!(response.body().len(), BODY_LEN);
    assert!(response.body()[..CHUNK].iter().all(|byte| *byte == b'a'));
    assert!(response.body()[CHUNK..].iter().all(|byte| *byte == b'b'));
    assert_eq!(
        response.trailers().expect("trailers").get("x-checksum"),
        Some(b"42".as_slice())
    );
}
