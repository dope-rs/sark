pub mod common;

use std::{cell::Cell, convert::Infallible};

use common::server::h3::{
    apps::{DUPLEX_INITIAL, FAIR_REQUESTS},
    proof::{
        CancelAction, Cancellation, ConnKey, Dispatch, JSON_INPUT, LARGE_JSON_INPUT,
        NESTED_JSON_INPUT, Proof, RAW_INPUT, Readiness, SEGMENTED_FIRST, SEGMENTED_INPUT,
        request::Spec,
    },
    sessions::Scenario,
};
use common::server::recover;
use dope::core::driver::life::recovery::Recovery;
use dope_quic::wire::stream::Id;
use dope_test::harness::observe::VecCellExt as _;
use sark_core::{
    life::{self, exchange, session::Disabled},
    manifold::stream::Boundary,
};
use sark_h3::{
    conn::options::{self, Egress},
    frame,
    manifold::server::{deadline, rotation::Policy},
};

fn run(scenario: Scenario<'_>) -> Proof<'_> {
    run_with(scenario, Disabled)
}

fn run_with<'a>(scenario: Scenario<'a>, lifetime: impl for<'d> Policy<'d>) -> Proof<'a> {
    let name = scenario.name;
    scenario.try_run_with(
        common::server::h3::sessions::plain,
        options::Config::default(),
        None,
        lifetime,
        deadline::Disabled,
        |error: Recovery<'_, '_>| -> Infallible { panic!("{name}: {:?}", error.cause()) },
        |cause| eprintln!("lifecycle diagnostic: {cause:?}"),
        sark_core::life::mode::Observed::new,
        |outcome| outcome.expect(name),
    )
}

#[test]
fn sync_rejection() {
    let path = *b"/missing";
    let body = *b"rejected-owner-is-not-retained";
    let mut missing = Spec::new(&path, &body);
    missing.expect.status = b"404";
    missing.expect.input_finished = false;
    missing.data.split = Some(9);
    let mut body = Spec::new(b"/body", RAW_INPUT);
    body.data.split = Some(7);
    run(Scenario {
        name: "sync-and-rejection",
        requests: vec![body, missing],
        expected_bodies: vec![Some(RAW_INPUT), Some(b"")],
        task_capacity: 1,
        cancellation: None,
        must_finish_before_last: None,
        linked: 1,
    });

    for (path, body) in [
        (b"/missing".as_slice(), b"".as_slice()),
        (b"/guarded", b""),
        (b"/body", b""),
        (b"/body", b"x"),
    ] {
        let mut request = Spec::new(path, body);
        request.expect.error = Some(frame::ErrorCode::ExcessiveLoad);
        let cause = Cell::new(None);
        let proof = Scenario {
            name: "response-preparation-rejected",
            requests: vec![request],
            expected_bodies: vec![None],
            task_capacity: 1,
            cancellation: None,
            must_finish_before_last: None,
            linked: 0,
        }
        .try_run_with(
            common::server::h3::sessions::plain,
            Default::default(),
            None,
            Disabled,
            deadline::Disabled,
            recover,
            |error| assert!(cause.replace(Some(format!("{error:?}"))).is_none()),
            |probe| {
                {
                    let mut proof = probe.0.write().expect("proof");
                    proof.network.protocol.ingress.field_section_bytes = 0;
                }
                life::mode::Observed::new(probe)
            },
            |outcome| outcome.expect("response preparation rejected"),
        );
        assert_eq!(
            cause.take().as_deref(),
            Some("Encode(FieldSection { remaining: 0, name: 7, value: 3 })")
        );
        assert!(proof.trace.events.iter().any(|event| matches!(
            event,
            life::Event::Terminal {
                outcome: life::Outcome::Rejected,
                ..
            }
        )));
    }
}

#[test]
fn guard_before_fin() {
    let mut guarded = Spec::new(b"/guarded", b"not-admitted");
    guarded.expect.status = b"401";
    guarded.expect.input_finished = false;
    guarded.data.boundary = Boundary::More;
    run(Scenario {
        name: "guard-before-body-fin",
        requests: vec![guarded],
        expected_bodies: vec![Some(b"denied")],
        task_capacity: 1,
        cancellation: None,
        must_finish_before_last: None,
        linked: 0,
    });
}

#[test]
fn rejects_declared_body() {
    for (path, body, length) in [
        (&b"/guarded"[..], &b""[..], 1),
        (b"/guarded", b"x", 0),
        (b"/missing", b"x", 0),
    ] {
        let mut request = Spec::new(path, body);
        request.data.declared_body_len = Some(length);
        request.expect.error = Some(frame::ErrorCode::Message);
        request.expect.input_finished = false;
        run(Scenario {
            name: "invalid-content-before-response",
            requests: vec![request],
            expected_bodies: vec![None],
            task_capacity: 1,
            cancellation: None,
            must_finish_before_last: None,
            linked: 0,
        });
    }
}

#[test]
fn extended_duplex() {
    const INPUT: &[u8] = b"h3-duplex-owner";
    let mut request = Spec::duplex(INPUT);
    request.data.split = Some(5);
    request.data.defer_tail = true;
    run(Scenario {
        name: "extended-connect-duplex",
        requests: vec![request],
        expected_bodies: vec![Some(INPUT)],
        task_capacity: 1,
        cancellation: None,
        must_finish_before_last: None,
        linked: 0,
    });
}

#[test]
fn duplex_segments() {
    const FIRST: &[u8] = b"first-h3-duplex-owner";
    const SECOND: &[u8] = b"second-h3-duplex-owner";
    let mut first = Spec::duplex(FIRST);
    first.data.split = Some(7);
    let mut second = Spec::duplex(SECOND);
    second.data.split = Some(8);
    run(Scenario {
        name: "concurrent-segmented-duplex",
        requests: vec![first, second],
        expected_bodies: vec![Some(FIRST), Some(SECOND)],
        task_capacity: 2,
        cancellation: None,
        must_finish_before_last: None,
        linked: 0,
    });
}

#[test]
fn duplex_retries_only_temporary_pressure() {
    for (name, capacity, wire, rejected) in [
        ("duplex-queue-pressure", 2, usize::MAX, false),
        ("duplex-wire-pressure", 8, 75, false),
        ("duplex-regular-wire-pressure", 8, 76, false),
        ("duplex-oversized-output", 8, 70, true),
    ] {
        let mut request = Spec::duplex(b"");
        request.head.protocol = Some(b"sark-start");
        request.head.path = b"/duplex/start";
        request.expect.error = rejected.then_some(frame::ErrorCode::ExcessiveLoad);
        let diagnostics = Cell::new(Vec::new());
        Scenario {
            name,
            requests: vec![request],
            expected_bodies: vec![(!rejected).then_some(DUPLEX_INITIAL)],
            task_capacity: 1,
            cancellation: None,
            must_finish_before_last: None,
            linked: 0,
        }
        .try_run_with(
            common::server::h3::sessions::plain,
            options::Config {
                egress: Egress {
                    write_capacity: capacity,
                    wire_bytes: wire,
                    ..Egress::default()
                },
                ..options::Config::default()
            },
            None,
            Disabled,
            deadline::Disabled,
            |error: Recovery<'_, '_>| -> Infallible { panic!("{name}: {:?}", error.cause()) },
            |cause| diagnostics.push(format!("{cause:?}")),
            sark_core::life::mode::Observed::new,
            |outcome| outcome.expect(name),
        );
        let expected = if rejected {
            vec![format!(
                "{:?}",
                sark_h3::conn::Error::BudgetWire {
                    retained: 5,
                    additional: 71,
                    limit: 70,
                }
            )]
        } else {
            Vec::new()
        };
        assert_eq!(diagnostics.take(), expected);
    }
}

#[test]
fn classic_duplex() {
    const INPUT: &[u8] = b"h3-classic-duplex-owner";
    run(Scenario {
        name: "classic-connect-duplex",
        requests: vec![Spec::classic(INPUT)],
        expected_bodies: vec![Some(INPUT)],
        task_capacity: 1,
        cancellation: None,
        must_finish_before_last: None,
        linked: 0,
    });
}

#[test]
fn classic_rejects_length() {
    let mut request = Spec::classic(b"");
    request.data.declared_body_len = Some(1);
    request.data.boundary = Boundary::More;
    request.expect.status = b"400";
    request.expect.input_finished = false;
    run(Scenario {
        name: "classic-connect-declared-body",
        requests: vec![request],
        expected_bodies: vec![Some(b"")],
        task_capacity: 1,
        cancellation: None,
        must_finish_before_last: None,
        linked: 0,
    });
}

#[test]
fn static_response() {
    use sark_core::life::Event;

    let proof = run(Scenario {
        name: "static-response-owner-projection",
        requests: vec![Spec::new(b"/async/quick", b"")],
        expected_bodies: vec![Some(b"quick")],
        task_capacity: 1,
        cancellation: None,
        must_finish_before_last: None,
        linked: 0,
    });
    let charge = proof
        .trace
        .events
        .iter()
        .find_map(|event| match event {
            Event::ResponseCommitted { charge, .. } => Some(*charge),
            _ => None,
        })
        .expect("static H3 response commitment");
    assert!(charge.is_empty());
    assert!(
        proof
            .trace
            .events
            .iter()
            .all(|event| !matches!(event, Event::ResponseOwnership(_)))
    );
}

#[test]
fn async_owners() {
    let mut segments = Spec::new(b"/segmented", SEGMENTED_INPUT);
    segments.data.split = Some(SEGMENTED_FIRST.len());
    let mut delayed = Spec::new(b"/async/segmented", SEGMENTED_INPUT);
    delayed.data.split = Some(SEGMENTED_FIRST.len());
    let mut json = Spec::new(b"/async/json", JSON_INPUT);
    json.head.content_type = Some(b"application/json");
    json.data.split = Some(JSON_INPUT.len() - 2);
    let mut nested = Spec::new(b"/async/json/nested", NESTED_JSON_INPUT);
    nested.head.content_type = Some(b"application/json");
    nested.data.split = Some(0);
    let mut large = Spec::new(b"/async/json/large", LARGE_JSON_INPUT);
    large.head.content_type = Some(b"application/json");
    let proof = run(Scenario {
        name: "async-owner-lifetimes",
        requests: vec![
            segments,
            Spec::new(b"/async/raw", RAW_INPUT),
            Spec::new(b"/async/save", b"saved-across-requests"),
            Spec::new(b"/async/replay", b""),
            delayed,
            json,
            nested,
            large,
            Spec::new(b"/users/beta/teams/alpha", b""),
            Spec::new(b"/async/stream/ok", RAW_INPUT),
        ],
        expected_bodies: vec![
            Some(SEGMENTED_INPUT),
            Some(RAW_INPUT),
            Some(b"saved"),
            Some(b"saved-across-requests"),
            Some(b"segmented"),
            Some(JSON_INPUT),
            Some(b"nested"),
            Some(b"large"),
            Some(b"named-h3"),
            Some(RAW_INPUT),
        ],
        task_capacity: 9,
        cancellation: None,
        must_finish_before_last: None,
        linked: 2,
    });
    assert!(proof.stream.resident.get() > 0);
    assert_eq!(proof.stream.polls.get(), 3);
    assert_eq!(proof.stream.drops.get(), 1);
    assert!(!proof.stream.moved.get());
}

#[test]
fn cancels_all() {
    for (action, followup, name) in [
        (CancelAction::RequestCancelled, false, "reset-cancellation"),
        (
            CancelAction::StopSending,
            false,
            "stop-sending-cancellation",
        ),
        (
            CancelAction::KeyClose,
            false,
            "connection-close-cancellation",
        ),
        (
            CancelAction::RequestCancelled,
            true,
            "active-task-cancel-then-quick",
        ),
        (
            CancelAction::StopSending,
            true,
            "active-task-cancel-then-quick",
        ),
        (CancelAction::Qpack, true, "active-task-cancel-then-quick"),
    ] {
        let cancelled = Spec::new(b"/async/cancel", b"");
        let (requests, expected_bodies) = if followup {
            let mut quick = Spec::new(b"/async/quick", b"");
            quick.dispatch = Dispatch::AfterCancellation;
            (
                vec![cancelled, quick],
                vec![None, Some(b"quick".as_slice())],
            )
        } else {
            (vec![cancelled], vec![None])
        };
        let proof = run(Scenario {
            name,
            requests,
            expected_bodies,
            task_capacity: 1,
            cancellation: Some(Cancellation {
                action,
                readiness: Readiness::TaskPollPending,
            }),
            must_finish_before_last: None,
            linked: 0,
        });
        assert!(proof.cancel.issued, "{name}: cancellation was not issued");
        assert_eq!(
            proof.cancel.dropped.get(),
            1,
            "{name}: task future drop count"
        );
        if followup {
            assert!(proof.cancel.retired);
            assert!(proof.cancel.followup);
        }
    }
}

#[test]
fn stop_reserved() {
    use sark_core::life::{Event, Outcome};

    let mut quick = Spec::new(b"/async/quick", b"");
    quick.dispatch = Dispatch::AfterCancellation;
    let proof = run(Scenario {
        name: "reserved-stop-then-quick",
        requests: vec![Spec::reserved(), quick],
        expected_bodies: vec![None, Some(b"quick")],
        task_capacity: 1,
        cancellation: Some(Cancellation {
            action: CancelAction::StopSending,
            readiness: Readiness::ExchangeOpened,
        }),
        must_finish_before_last: None,
        linked: 0,
    });
    assert!(proof.cancel.issued && proof.cancel.retired && proof.cancel.followup);
    let (conn, stream) = proof.replies.streams[0].expect("reserved request stream");
    assert!(proof.replies.streams[1].map(|(connection, _)| connection) == Some(conn));
    let matches = |exchange: &exchange::Id<ConnKey, Id>| {
        exchange.connection() == conn && exchange.stream() == stream
    };
    assert!(proof.trace.events.iter().all(|event| !matches!(
        event,
        Event::HeadAdmitted(exchange) | Event::TaskAdmitted(exchange) if matches(exchange)
    )));
    assert!(proof.trace.events.iter().any(|event| matches!(
        event,
        Event::Terminal {
            exchange,
            outcome: Outcome::Cancelled,
        } if matches(exchange)
    )));
}

#[test]
fn reset_then_followup() {
    use sark_core::life::{Event, Outcome};

    let mut pending = Spec::new(b"/body", b"partial");
    pending.data.declared_body_len = Some(64);
    pending.data.boundary = Boundary::More;
    pending.expect.input_finished = false;
    let mut quick = Spec::new(b"/async/quick", b"");
    quick.dispatch = Dispatch::AfterCancellation;
    let proof = run(Scenario {
        name: "pending-body-reset-then-quick",
        requests: vec![pending, quick],
        expected_bodies: vec![None, Some(b"quick")],
        task_capacity: 1,
        cancellation: Some(Cancellation {
            action: CancelAction::RequestCancelled,
            readiness: Readiness::HeadAdmitted,
        }),
        must_finish_before_last: None,
        linked: 0,
    });
    let (conn, stream) = proof.replies.streams[0].expect("cancelled request stream");
    let matches = |exchange: &exchange::Id<ConnKey, Id>| {
        exchange.connection() == conn && exchange.stream() == stream
    };
    assert!(proof.cancel.issued, "pending request RESET was not issued");
    assert!(
        proof.cancel.retired,
        "pending request did not retire before the follow-up"
    );
    assert_eq!(
        proof.cancel.dropped.get(),
        0,
        "a pending request owned a task"
    );
    assert!(
        proof.cancel.followup,
        "same-connection follow-up was not causally dispatched"
    );
    assert!(
        proof
            .trace
            .events
            .iter()
            .any(|event| matches!(event, Event::HeadAdmitted(exchange) if matches(exchange)))
    );
    assert!(
        proof
            .trace
            .events
            .iter()
            .all(|event| !matches!(event, Event::TaskAdmitted(exchange) if matches(exchange)))
    );
    assert!(proof.trace.events.iter().any(|event| matches!(
        event,
        Event::Terminal {
            exchange,
            outcome: Outcome::Cancelled,
        } if matches(exchange)
    )));
}

#[test]
fn fail_then_followup() {
    let mut underrun = Spec::new(b"/body", b"abc");
    underrun.data.declared_body_len = Some(4);
    underrun.expect.error = Some(frame::ErrorCode::Message);
    underrun.expect.input_finished = false;

    let mut overrun = Spec::new(b"/body", b"abc");
    overrun.data.declared_body_len = Some(2);
    overrun.expect.error = Some(frame::ErrorCode::Message);
    overrun.expect.input_finished = false;

    let mut invalid = Spec::new(b"/async/json", b"{");
    invalid.head.content_type = Some(b"application/json");
    invalid.expect.error = Some(frame::ErrorCode::Message);

    for (name, failed) in [
        ("declared-body-underrun", underrun),
        ("declared-body-overrun", overrun),
        ("streaming-body-malformed", invalid),
    ] {
        let mut followup = Spec::new(b"/body", failed.data.bytes);
        followup.dispatch = Dispatch::AfterFailure;
        let proof = run(Scenario {
            name,
            requests: vec![failed, followup],
            expected_bodies: vec![None, Some(failed.data.bytes)],
            task_capacity: 1,
            cancellation: None,
            must_finish_before_last: None,
            linked: 1,
        });
        assert!(
            proof.cancel.followup,
            "{name}: same-connection follow-up was not causally dispatched"
        );
    }
}

#[test]
fn json_failure_precedes_next_data() {
    use sark_core::life::Event;

    let mut invalid = Spec::new(b"/async/json", b"!x");
    invalid.head.content_type = Some(b"application/json");
    invalid.data.split = Some(1);
    invalid.data.boundary = Boundary::More;
    invalid.expect.error = Some(frame::ErrorCode::Message);
    invalid.expect.input_finished = false;
    let mut followup = Spec::new(b"/body", b"ok");
    followup.dispatch = Dispatch::AfterFailure;
    let diagnostics = Cell::new(Vec::new());
    let proof = Scenario {
        name: "json-cleanup-before-next-data",
        requests: vec![invalid, followup],
        expected_bodies: vec![None, Some(b"ok")],
        task_capacity: 1,
        cancellation: None,
        must_finish_before_last: None,
        linked: 1,
    }
    .try_run_with(
        common::server::h3::sessions::plain,
        options::Config::default(),
        None,
        Disabled,
        deadline::Disabled,
        recover,
        |cause| diagnostics.push(format!("{cause:?}")),
        sark_core::life::mode::Observed::new,
        |outcome| outcome.expect("JSON cleanup before next DATA"),
    );
    assert_eq!(
        diagnostics.take(),
        vec!["Build(R0007(Parse(Invalid)))".to_owned()]
    );
    let (conn, stream) = proof.replies.streams[0].expect("malformed JSON stream");
    let reserved: usize = proof
        .trace
        .events
        .iter()
        .filter_map(|event| match event {
            Event::PreAdmissionReserved {
                exchange, bytes, ..
            } if exchange.connection() == conn && exchange.stream() == stream => Some(*bytes),
            _ => None,
        })
        .sum();
    assert_eq!(
        reserved, 1,
        "next DATA must stay behind the cleanup barrier"
    );
    assert!(proof.cancel.followup);
}

#[test]
fn stream_observes_without_retention() {
    use common::server::h3::observer::StreamObserver;
    use o3::buffer::hold;

    let charge = Cell::new(None);
    let proof = Scenario {
        name: "custom-stream-observer",
        requests: vec![Spec::new(b"/async/stream/resident", b"retained-input")],
        expected_bodies: vec![Some(b"retained-")],
        task_capacity: 1,
        cancellation: None,
        must_finish_before_last: None,
        linked: 0,
    }
    .try_run_with(
        common::server::h3::sessions::plain,
        Default::default(),
        None,
        Disabled,
        deadline::Disabled,
        recover,
        |cause| panic!("{cause:?}"),
        |probe| StreamObserver {
            probe,
            charge: &charge,
        },
        |outcome| outcome.expect("custom stream observation"),
    );
    let resident = proof.stream.resident.get();
    assert!(resident > 0);
    assert_eq!(charge.get(), Some(hold::Charge::from_owner(9, resident)));
}

#[test]
fn stream_failure_isolated() {
    use sark_h3::manifold::server::failure::Quiet;

    let mut failed = Spec::new(b"/async/stream/fail", b"before-error");
    failed.expect.error = Some(frame::ErrorCode::Internal);
    let mut followup = Spec::new(b"/async/quick", b"");
    followup.dispatch = Dispatch::AfterFailure;
    let diagnostics = Cell::new(Vec::new());
    let proof = Scenario {
        name: "quiet-stream-error",
        requests: vec![failed, followup],
        expected_bodies: vec![None, Some(b"quick")],
        task_capacity: 1,
        cancellation: None,
        must_finish_before_last: None,
        linked: 0,
    }
    .try_run_with(
        common::server::h3::sessions::plain,
        Default::default(),
        None,
        Disabled,
        deadline::Disabled,
        recover,
        |cause| diagnostics.push(format!("{cause:?}")),
        Quiet::new,
        |outcome| outcome.expect("quiet stream failure"),
    );
    assert_eq!(diagnostics.take(), ["Read(\"read-failed\")"]);
    assert_eq!(proof.stream.polls.get(), 4);
    assert_eq!(proof.stream.drops.get(), 1);
    assert!(!proof.stream.moved.get());
    assert!(proof.cancel.followup);
}

#[test]
fn routed_response_or_unsupported() {
    use common::server::h3::sessions::Error;
    use dope::core::io::datagram;
    use dope_quic::runtime::{endpoint::server::Plan, routing};

    let mut group = None;
    let result = Scenario {
        name: "routed-response",
        requests: vec![Spec::new(b"/async/quick", b"")],
        expected_bodies: vec![Some(b"quick")],
        task_capacity: 1,
        cancellation: None,
        must_finish_before_last: None,
        linked: 0,
    }
    .try_run_with(
        |(address, reservation), tls, options, endpoint| {
            drop(reservation);
            let binding = group
                .insert(routing::Group::bind(address, routing::Count::MIN).map_err(Error::Group)?)
                .split()
                .next()
                .expect("one routed binding");
            let plan = Plan::routed(binding, tls, options, endpoint).map_err(Error::Endpoint)?;
            Ok((plan, ()))
        },
        Default::default(),
        None,
        Disabled,
        deadline::Disabled,
        recover,
        |error| panic!("{error:?}"),
        life::mode::Observed::new,
        |outcome| match outcome {
            Ok(_) => Ok(()),
            Err(Error::Group(error @ datagram::Error::Unsupported { .. })) => Err(error),
            Err(error) => panic!("{error:?}"),
        },
    );
    eprintln!("routed native outcome: {result:?}");
}

#[test]
fn quiet_connection_failures() {
    use std::{num::NonZero, time::Duration};

    use common::server::h3::{proof::Failure, sessions::Error};
    use sark_core::life::session::Config;
    use sark_h3::manifold::server::failure::Quiet;

    let short = Duration::from_millis(1);
    let mut deferred = [0x21, 0].repeat(33);
    deferred.push(frame::TYPE_HEADERS.get() as u8);
    for (streams, age, drain, send_bytes, wire, expected) in [
        (0, short, short, None, None, "QuicStream(PeerLimit)"),
        (
            1,
            Duration::from_secs(60),
            short,
            None,
            None,
            "QuicStream(PeerLimit)",
        ),
        (3, Duration::MAX, short, None, None, "Age { source: Range {"),
        (
            3,
            short,
            Duration::MAX,
            None,
            None,
            "Deadline { source: Range {",
        ),
        (
            3,
            Duration::from_secs(60),
            short,
            NonZero::new(256),
            None,
            "Resident(Stream(ValueOutOfRange))",
        ),
        (
            3,
            Duration::from_secs(60),
            short,
            None,
            Some(deferred.as_slice()),
            "H3(Incomplete { stream_id: 0, buffered: 1, cause: VarInt(Underflow { actual: 0, required: 1 }) })",
        ),
    ] {
        let diagnostics = Cell::new(Vec::new());
        let mut request = Spec::new(
            if send_bytes.is_some() {
                b"/async/stream/two_park"
            } else {
                b"/async/stream/park"
            },
            if wire.is_some() { b"" } else { b"x" },
        );
        request.head.wire = wire;
        request.dispatch = Dispatch::Established;
        Scenario {
            name: expected,
            requests: vec![request],
            expected_bodies: vec![None],
            task_capacity: 1,
            cancellation: None,
            must_finish_before_last: None,
            linked: 0,
        }
        .try_run_with(
            common::server::h3::sessions::plain,
            Default::default(),
            send_bytes,
            Config::try_new(age, drain).expect("finite lifetime"),
            deadline::Disabled,
            recover,
            |cause| diagnostics.push(format!("{cause:?}")),
            |probe| {
                {
                    let mut proof = probe.0.write().expect("proof");
                    if streams == 1 {
                        proof.network.protocol.qpack.decoder.table_bytes = 1024;
                    }
                    let params = &mut proof.network.connect[0].params;
                    *params = params
                        .to_builder()
                        .uni_count(streams)
                        .try_build()
                        .expect("peer stream limit");
                }
                Quiet::new(probe)
            },
            |outcome| match outcome {
                Err(Error::Fixture(errors))
                    if matches!(errors.as_slice(), [Failure::Scenario(error)]
                        if error.starts_with("E2E H3 connection: ")) => {}
                Err(error) => panic!("{expected}: {error:?}"),
                Ok(_) => panic!("connection failure must terminate"),
            },
        );
        let errors = diagnostics.take();
        if streams <= 1 || send_bytes.is_some() || wire.is_some() {
            assert_eq!(errors, [expected]);
        } else {
            assert!(
                matches!(errors.as_slice(), [error]
                if error.starts_with(expected)
                    && error.ends_with(&format!("duration: Positive({:?}) }} }}", Duration::MAX))),
                "{expected}: {errors:?}"
            );
        }
    }
}

#[test]
fn returned_stream_cancels() {
    for action in [
        CancelAction::RequestCancelled,
        CancelAction::StopSending,
        CancelAction::KeyClose,
    ] {
        let mut requests = vec![Spec::new(b"/async/stream/park", RAW_INPUT)];
        let mut expected_bodies = vec![None];
        if action != CancelAction::KeyClose {
            let mut quick = Spec::new(b"/async/quick", b"");
            quick.dispatch = Dispatch::AfterCancellation;
            requests.push(quick);
            expected_bodies.push(Some(b"quick".as_slice()));
        }
        let proof = run(Scenario {
            name: "post-await-stream-cancellation",
            requests,
            expected_bodies,
            task_capacity: 1,
            cancellation: Some(Cancellation {
                action,
                readiness: Readiness::StreamPending,
            }),
            must_finish_before_last: None,
            linked: 0,
        });
        assert!(proof.cancel.issued && proof.cancel.retired);
        assert_eq!(proof.cancel.followup, action != CancelAction::KeyClose);
        assert!(proof.stream.polls.get() >= 3);
        assert_eq!(proof.stream.drops.get(), 1);
        assert!(!proof.stream.moved.get());
    }
}

#[test]
fn stream_rejects_intrinsic_pressure() {
    use std::num::NonZero;

    use sark_core::life::{Event, mode::Observed};
    use sark_h3::conn;
    use sark_http::field::metadata::block::Connect;

    for (mut request, slots, wire, resident) in [
        (
            Spec::new(b"/async/stream/wire", &[b'w'; 64]),
            2,
            64,
            usize::MAX,
        ),
        (
            Spec::new(b"/async/stream/resident", &[b'r'; 8192]),
            8,
            16,
            8192,
        ),
        (Spec::classic(DUPLEX_INITIAL), 2, 64, usize::MAX),
    ] {
        let streaming = request.head.method != b"CONNECT";
        request.dispatch = Dispatch::Established;
        request.expect.error = Some(frame::ErrorCode::ExcessiveLoad);
        let protocol = options::Config {
            protocol: options::Protocol {
                extended_connect: Connect::Enabled,
                ..Default::default()
            },
            egress: Egress {
                write_capacity: slots,
                wire_bytes: wire,
                resident_bytes: resident,
            },
            ..Default::default()
        };
        let mut settings = Vec::new();
        options::plan::Config::new(protocol.clone())
            .expect("H3 configuration")
            .settings()
            .encode(&mut settings)
            .expect("SETTINGS payload");
        let mut control = Vec::new();
        frame::STREAM_TYPE_CONTROL
            .encode(&mut control)
            .expect("control prefix");
        frame::Payload::encode(frame::TYPE_SETTINGS, &settings, &mut control)
            .expect("control frame");
        let send_bytes = NonZero::new(control.len()).expect("control stream bytes");
        let diagnostics = Cell::new(Vec::new());
        let proof = Scenario {
            name: "stream-intrinsic-pressure",
            requests: vec![request],
            expected_bodies: vec![None],
            task_capacity: 1,
            cancellation: None,
            must_finish_before_last: None,
            linked: 0,
        }
        .try_run_with(
            common::server::h3::sessions::plain,
            protocol,
            Some(send_bytes),
            Disabled,
            deadline::Disabled,
            recover,
            |cause| diagnostics.push(format!("{cause:?}")),
            |probe| {
                // Unsent SETTINGS fill QUIC's budget, so HEAD remains in the H3 queue.
                let mut proof = probe.0.write().expect("proof");
                let params = &mut proof.network.connect[0].params;
                *params = params
                    .to_builder()
                    .stream_uni(0)
                    .try_build()
                    .expect("zero control credit");
                assert_eq!(params.flow().stream_uni().get(), 0);
                drop(proof);
                Observed::new(probe)
            },
            |outcome| outcome.expect("intrinsic pressure must terminate"),
        );
        assert!(!proof.replies.status_ok[0], "head remained queued");
        assert_eq!(
            proof
                .trace
                .events
                .iter()
                .filter(|event| matches!(event, Event::ResponseCommitted { .. }))
                .count(),
            1
        );
        assert_eq!(proof.stream.polls.get(), if streaming { 2 } else { 0 });
        assert_eq!(proof.stream.drops.get(), usize::from(streaming));
        assert!(!proof.stream.moved.get());
        let expected = if slots == 2 {
            conn::Error::BudgetRegularWire {
                retained: 0,
                additional: if streaming { 67 } else { 71 },
                limit: 60,
            }
        } else {
            assert_eq!(proof.stream.resident.get(), 8192);
            conn::Error::BudgetRegularResident {
                retained: 0,
                additional: proof.stream.resident.get(),
                limit: 7680,
            }
        };
        assert_eq!(diagnostics.take(), [format!("{expected:?}")]);
    }
}

#[test]
fn drain_rejects_new_stream() {
    use o3::base::time::Positive;
    use sark_core::life::{Event, Outcome, session};

    let mut pending = Spec::new(b"/body", RAW_INPUT);
    pending.data.split = Some(7);
    pending.data.defer_tail = true;
    let mut newcomer = Spec::new(b"/async/quick", b"");
    newcomer.dispatch = Dispatch::AfterGoAway;
    newcomer.expect.error = Some(frame::ErrorCode::RequestRejected);
    newcomer.expect.input_finished = false;
    let proof = run_with(
        Scenario {
            name: "new-stream-during-graceful-drain",
            requests: vec![pending, newcomer],
            expected_bodies: vec![Some(RAW_INPUT), None],
            task_capacity: 1,
            cancellation: None,
            must_finish_before_last: None,
            linked: 1,
        },
        session::Config::new(Positive::from_secs::<1>(), Positive::from_secs::<5>()),
    );
    let (conn, stream) = proof.replies.streams[1].expect("new request stream");
    assert_eq!(proof.network.goaway, Some(stream.get()));
    assert_eq!(proof.replies.completion_order, [1, 0]);
    assert!(proof.trace.events.iter().any(|event| matches!(event,
        Event::Terminal { exchange, outcome: Outcome::Rejected }
        if exchange.connection() == conn && exchange.stream() == stream
    )));
    assert!(proof.trace.events.iter().all(|event| !matches!(event,
        Event::HeadAdmitted(exchange) | Event::TaskAdmitted(exchange)
        if exchange.connection() == conn && exchange.stream() == stream
    )));
}

#[test]
fn scheduler_fairness() {
    let mut requests = Vec::with_capacity(FAIR_REQUESTS + 1);
    let mut expected = Vec::with_capacity(FAIR_REQUESTS + 1);
    for slot in 0..FAIR_REQUESTS {
        requests.push(Spec::new(
            if slot % 2 == 0 {
                b"/fair/a"
            } else {
                b"/fair/b"
            },
            b"",
        ));
        expected.push(Some(if slot % 2 == 0 { &b"a"[..] } else { &b"b"[..] }));
    }
    let quick = requests.len();
    requests.push(Spec::new(b"/async/quick", b""));
    expected.push(Some(b"quick"));
    run(Scenario {
        name: "scheduler-fairness",
        requests,
        expected_bodies: expected,
        task_capacity: FAIR_REQUESTS + 1,
        cancellation: None,
        must_finish_before_last: Some(quick),
        linked: 0,
    });
}
