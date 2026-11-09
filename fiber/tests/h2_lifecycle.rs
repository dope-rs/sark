pub mod common;

use std::{cell::Cell, fmt, io::Write as _, time::Duration};

use common::h2::recover;
use common::h2::{
    events::{Credit, CreditLog, Log},
    facts::{GuardProbe, State},
    peer::{Peer, Sender},
    server::{CONFIG, Request, Server},
};

use dope_test::harness::server::Pair;
use sark_core::{
    life::{
        audit,
        mode::{self, Observed},
    },
    manifold::stream::Boundary,
};
use sark_h2::{
    conn::config,
    frame::{controls::RstStream, numbers::ErrorCode, payload::Payload},
};

#[test]
fn records_exchange_life() {
    let harness = Pair::bind().expect("H2 harness");
    let config = CONFIG;
    let mut facts = Vec::new();

    harness
        .check(
            move |context, listener| {
                let log = Cell::new(Vec::new());
                Server::new(config, 8, State::default())
                    .try_run(
                        context,
                        listener,
                        Observed::new(Log(&log, diagnose)),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .map(|()| log.into_inner())
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                let stream = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/quick")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("start observed H2 request");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                let (_, body) = Peer::new(&mut socket, &mut client)
                    .try_response(stream)
                    .expect("H2 peer response");
                assert_eq!(body, b"quick");
            },
            |result| {
                facts = result.expect("H2 setup");
            },
        )
        .expect("observed H2 runtime");

    let stream = facts
        .iter()
        .find_map(|&(kind, stream)| (kind == "exchange-opened").then_some(stream))
        .expect("one observed H2 exchange");
    let expected = [
        "exchange-opened",
        "head-admitted",
        "input-finished",
        "build-ready",
        "task-admitted",
        "task-poll-ready",
        "response-committed",
        "request-owner-scope-closed",
        "terminal",
        "exchange-retired",
    ];
    let actual = facts
        .iter()
        .filter_map(|&(kind, source)| (source == stream).then_some(kind))
        .collect::<Vec<_>>();
    let mut cursor = 0;
    for kind in actual {
        if expected.get(cursor) == Some(&kind) {
            cursor += 1;
        }
    }
    assert_eq!(
        cursor,
        expected.len(),
        "missing ordered H2 facts: {facts:?}"
    );
    assert_eq!(
        facts
            .iter()
            .filter(|&&(kind, source)| kind == "terminal" && source == stream)
            .count(),
        1,
        "one exchange must have exactly one terminal fact",
    );
    assert_eq!(
        facts
            .iter()
            .filter(|&&(kind, source)| {
                matches!(
                    kind,
                    "response-staging-acquired"
                        | "response-staging-buffered"
                        | "response-staging-released"
                ) && source == stream
            })
            .count(),
        0,
        "a static response must not invent a retained staging lifetime",
    );
    assert_eq!(
        facts
            .iter()
            .filter(|&&(kind, source)| {
                (kind == "response-handoff" || kind == "response-transport-released")
                    && source == stream
            })
            .count(),
        0,
        "a buffered small response must not invent a transport owner",
    );
    assert!(facts.iter().any(|event| event.0 == "session-opened"));
    assert!(facts.iter().any(|event| event.0 == "session-established"));
    let closing = facts
        .iter()
        .position(|event| event.0 == "session-closing")
        .expect("H2 close must publish its closing boundary");
    let audit = facts
        .iter()
        .position(|event| event.0 == "audit")
        .expect("H2 close must publish its canonical final audit");
    let closed = facts
        .iter()
        .position(|event| event.0 == "session-closed")
        .expect("H2 close must publish its terminal session fact");
    assert!(
        closing < audit && audit < closed,
        "H2 close ordering must be closing -> cleanup/audit -> closed: {facts:?}",
    );
}

#[test]
fn close_pending_in_order() {
    let mut facts = Vec::new();
    let mut credit = Credit::default();
    Pair::bind()
        .expect("H2 harness")
        .check(
            move |context, listener| {
                let log = Cell::new(Vec::new());
                let observed = Cell::new(Credit::default());
                Server::new(CONFIG, 8, State::default())
                    .try_run(
                        context,
                        listener,
                        Observed::new(CreditLog {
                            log: Log(&log, diagnose),
                            state: &observed,
                        }),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .map(|()| (log.into_inner(), observed.into_inner()))
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                for _ in 0..3 {
                    let stream = client
                        .streams()
                        .headers()
                        .request(
                            Request::new(b"/segmented")
                                .try_headers()
                                .expect("valid request fields"),
                            Boundary::More,
                        )
                        .expect("pending body");
                    Sender::new(&mut client)
                        .try_send(stream, b"first-", Boundary::More)
                        .expect("H2 peer send");
                }
                let fence = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/quick")
                            .try_headers()
                            .expect("application fence"),
                        Boundary::End,
                    )
                    .expect("fence stream");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                let (_, reply) = Peer::new(&mut socket, &mut client)
                    .try_response(fence)
                    .expect("H2 peer response");
                assert_eq!(reply, b"quick");
            },
            |result| (facts, credit) = result.expect("H2 setup"),
        )
        .expect("observed H2 runtime");

    let opened = facts
        .iter()
        .filter_map(|&(kind, stream)| (kind == "exchange-opened").then_some(stream))
        .take(3)
        .collect::<Vec<_>>();
    assert_eq!(opened.len(), 3);
    let expected = opened
        .into_iter()
        .flat_map(|stream| {
            [
                ("pre-admission-released", stream),
                ("request-owner-scope-closed", stream),
                ("terminal", stream),
                ("exchange-retired", stream),
            ]
        })
        .collect::<Vec<_>>();
    let actual = facts
        .iter()
        .skip_while(|event| event.0 != "session-closing")
        .filter(|event| {
            matches!(
                event.0,
                "pre-admission-released"
                    | "request-owner-scope-closed"
                    | "terminal"
                    | "exchange-retired"
            )
        })
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert!(!credit.invalid, "exact H2 body credit arithmetic");
    assert_eq!(credit.reserved.0, 18);
    assert_eq!(credit.closing, Some(credit.reserved));
    assert!(credit.reserved.1 > 0);
    assert_eq!(credit.reserved, credit.released);
    assert!(credit.audit.expect("final H2 audit").is_zero(
        audit::Capability::PRE_ADMISSION_BYTES.union(audit::Capability::PRE_ADMISSION_CHUNKS)
    ));
}

#[test]
fn reset_pending_owner() {
    let mut facts = Vec::new();
    let mut credit = Credit::default();
    Pair::bind()
        .expect("H2 harness")
        .check(
            |context, listener| {
                let log = Cell::new(Vec::new());
                let observed = Cell::new(Credit::default());
                let mut state = State::try_body().expect("owned guard body");
                state.guard = Some(GuardProbe::default());
                let mut limits = CONFIG;
                limits.max_outbound_bytes = 1;
                let server = Server::new(limits, 1, state).with_config(config::Config {
                    event_capacity: 1,
                    ..Default::default()
                });
                server
                    .try_run(
                        context,
                        listener,
                        Observed::new(CreditLog {
                            log: Log(&log, diagnose),
                            state: &observed,
                        }),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .map(|()| {
                        assert!(server.state.body.take().is_none(), "guard consumed owner");
                        (log.into_inner(), observed.into_inner())
                    })
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                let stream = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/guarded/body")
                            .try_headers()
                            .expect("guard request fields"),
                        Boundary::End,
                    )
                    .expect("guard stream");
                let mut wire = client.output().first().to_vec();
                client.transfer().drain(wire.len());
                Payload::RstStream(
                    RstStream::new(stream, ErrorCode::Cancel).expect("nonzero stream"),
                )
                .encode(&mut wire)
                .expect("reset frame");
                let fence = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/quick")
                            .try_headers()
                            .expect("fence request fields"),
                        Boundary::End,
                    )
                    .expect("fence stream");
                let output = client.output().first();
                wire.extend_from_slice(output);
                let len = output.len();
                client.transfer().drain(len);
                socket.write_all(&wire).expect("headers and reset batch");
                let (_, reply) = Peer::new(&mut socket, &mut client)
                    .try_response(fence)
                    .expect("fence response");
                assert_eq!(reply, b"quick");
            },
            |result| (facts, credit) = result.expect("H2 setup"),
        )
        .expect("observed H2 runtime");

    let stream = facts
        .iter()
        .find_map(|&(kind, stream)| (kind == "exchange-opened").then_some(stream))
        .expect("guard exchange");
    let expected = [
        "response-committed",
        "response-staging-released",
        "exchange-cancelled",
        "terminal",
        "exchange-retired",
    ];
    let actual = facts
        .iter()
        .filter_map(|&(kind, id)| {
            (id == stream && expected.contains(&kind)).then_some(kind)
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert!(credit.audit.expect("final H2 audit").is_zero(
        audit::Capability::RESPONSE_STAGING_OWNER_BYTES
            .union(audit::Capability::TRANSPORT_RESPONSE_OWNER_BYTES)
    ));
}

#[test]
fn rejects_oversized_body() {
    let harness = Pair::bind().expect("H2 harness");
    let mut config = CONFIG;
    config.max_request_body_bytes = 1;
    config.max_connection_body_bytes = 1;

    harness
        .check(
            move |context, listener| {
                let server = Server::new(config, 1, State::with_guard(GuardProbe::default()));
                let result = server.try_run(context, listener, mode::Disabled, recover, |result| {
                    result.expect("H2 execution")
                });
                result.map(|()| server.state.guard_counts())
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                let stream = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/guarded/body")
                            .try_length(b"2")
                            .expect("valid request fields"),
                        Boundary::More,
                    )
                    .expect("start oversized guarded request");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");

                assert_eq!(
                    Peer::new(&mut socket, &mut client)
                        .try_reset(stream)
                        .expect("H2 peer reset"),
                    ErrorCode::EnhanceYourCalm,
                );
            },
            |result| {
                assert_eq!(result.expect("H2 setup"), (0, 0));
            },
        )
        .expect("oversized guarded H2 runtime");
}

#[test]
fn rejects_finished_body() {
    let harness = Pair::bind().expect("H2 harness");
    let config = CONFIG;

    harness
        .check(
            move |context, listener| {
                let server = Server::new(config, 1, State::with_guard(GuardProbe::default()));
                let result = server.try_run(context, listener, mode::Disabled, recover, |result| {
                    result.expect("H2 execution")
                });
                result.map(|()| server.state.guard_counts())
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                let stream = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/guarded/body")
                            .try_length(b"1")
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("start finished guarded request");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");

                assert_eq!(
                    Peer::new(&mut socket, &mut client)
                        .try_reset(stream)
                        .expect("H2 peer reset"),
                    ErrorCode::ProtocolError,
                );
            },
            |result| {
                assert_eq!(result.expect("H2 setup"), (0, 0));
            },
        )
        .expect("finished guarded H2 runtime");
}

fn diagnose(cause: &dyn fmt::Debug) {
    eprintln!("lifecycle diagnostic: {cause:?}");
}
