pub mod common;

use std::{cell::Cell, convert::Infallible};

use common::grpc::{
    call::{
        borrowed::Borrowed,
        egress::{
            failure::Active,
            reply::{self, Reply},
            streams::AfterMessage,
        },
    },
    settings::Settings,
    wire::Peer,
};
use dope::{core::driver::route::KeyTag, net::wire};
use dope_test::harness::server::Pair;
use sark_core::manifold::{
    server::app,
    stream::{Boundary, Section},
};
use sark_grpc::{
    Code,
    server::{
        config::{Limits, Output},
        runtime::{run, state},
    },
};
use sark_h2::{
    conn,
    frame::numbers::ErrorCode,
    hpack::encode::fields,
    server::runtime::{
        self,
        app::{Failure, Report},
    },
    stream::Id,
};

#[test]
fn reports_write_failure() {
    let pair = Pair::bind().expect("gRPC reporting pair");
    let settings = Settings::new(pair.addr(), Limits::default());
    pair.run(
        move |context, listener| {
            let calls = Cell::new(0);
            let reports = Cell::new(0);
            let observed = Cell::new(None);
            let report = Report::new(
                |failure: Failure<
                    '_,
                    '_,
                    '_,
                    wire::Identity,
                    KeyTag<0>,
                    run::Failure,
                    runtime::Build<state::Error>,
                >| {
                    if let Failure::Diagnostic { cause, .. } = failure {
                        observed.set(Some(cause));
                        reports.set(reports.get() + 1);
                    }
                },
            );
            let retired = settings
                .try_report(
                    Borrowed(&calls),
                    context,
                    listener,
                    report,
                    common::recover,
                    |result| result.expect("gRPC runtime"),
                )
                .expect("gRPC server");
            assert!(
                retired.is_empty(),
                "unexpected server retirement: {retired:?}"
            );
            assert_eq!(reports.get(), 1);
            assert!(matches!(
                observed.take(),
                Some(app::Error::Connection(conn::Error::HeaderEncode(
                    fields::Error::List {
                        used: 0,
                        name: 7,
                        value: 3,
                        limit: 1,
                    }
                )))
            ));
            assert_eq!(calls.get(), 0);
            Ok::<(), Infallible>(())
        },
        |address| {
            assert_eq!(
                Peer::try_rejected::<1>(address, b"/invalid", Boundary::End)
                    .expect("rejected request"),
                [ErrorCode::InternalError]
            );
        },
    )
    .expect("gRPC reporting integration");
}

#[test]
fn active_write() {
    let pair = Pair::bind().expect("gRPC active failure pair");
    let settings = Settings::new(
        pair.addr(),
        Limits::default().try_calls(1).expect("one call slot"),
    );
    pair.run(
        move |context, listener| {
            let calls = Cell::new(0);
            let reports = Cell::new(0);
            let report = Report::new(
                |failure: Failure<
                    '_,
                    '_,
                    '_,
                    wire::Identity,
                    KeyTag<0>,
                    run::Failure,
                    runtime::Build<state::Error>,
                >| {
                    if let Failure::Diagnostic { cause, .. } = failure {
                        assert!(matches!(
                            cause,
                            app::Error::Connection(conn::Error::HeaderEncode(
                                fields::Error::List {
                                    used: 0,
                                    name: 7,
                                    value: 3,
                                    limit: 1,
                                }
                            ))
                        ));
                        reports.set(reports.get() + 1);
                    }
                },
            );
            let retired = settings
                .try_report(
                    Active(&calls),
                    context,
                    listener,
                    report,
                    common::recover,
                    |result| result.expect("gRPC runtime"),
                )
                .expect("gRPC server");
            assert!(
                retired.is_empty(),
                "unexpected server retirement: {retired:?}"
            );
            assert_eq!(calls.get(), 2);
            assert_eq!(reports.get(), 2);
            Ok::<(), Infallible>(())
        },
        |address| {
            assert_eq!(
                Peer::try_rejected::<2>(address, b"/failure/active", Boundary::More)
                    .expect("active request resets"),
                [ErrorCode::InternalError; 2]
            );
        },
    )
    .expect("gRPC active failure integration");
}

#[test]
fn response_phase() {
    for (window, invalid) in [(0, true), (9, true), (9, false), (7, true), (7, false)] {
        let pair = Pair::bind().expect("gRPC error phase pair");
        let limits =
            Limits::default().with_output(Output::try_new(1, 9).expect("one response capacity"));
        let settings = Settings::new(pair.addr(), limits);
        pair.run(
            move |context, listener| {
                let retired = settings
                    .try_serve_on(AfterMessage, context, listener, common::recover, |result| {
                        result.expect("gRPC runtime")
                    })
                    .expect("gRPC server");
                assert!(
                    retired.is_empty(),
                    "unexpected server retirement: {retired:?}"
                );
                Ok::<_, Infallible>(())
            },
            |address| {
                let result = Reply::try_failure(address, window, invalid);
                if window == 7 {
                    assert!(matches!(
                        result,
                        Err(reply::Error::Reset {
                            stream: Id::FIRST_CLIENT,
                            code: ErrorCode::InternalError,
                        })
                    ));
                    return;
                }
                let reply = result.expect("failure trailers");
                if window == 0 {
                    assert!(reply.messages.is_empty());
                } else {
                    assert_eq!(reply.messages, [b"sent".to_vec()]);
                }
                assert!(matches!(
                    reply.headers.as_slice(),
                    [(Section::Initial, _), (Section::Trailing, _)]
                ));
                assert_eq!(
                    reply.status().expect("failure status").code(),
                    Code::Internal
                );
            },
        )
        .expect("gRPC error phase integration");
    }
}
