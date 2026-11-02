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
use sark_core::{
    life::{exchange, session::Disabled},
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
        options::Config::default(),
        lifetime,
        deadline::Disabled,
        |error: Recovery<'_, '_>| -> Infallible { panic!("{name}: {:?}", error.cause()) },
        |cause| eprintln!("lifecycle diagnostic: {cause:?}"),
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
            options::Config {
                egress: Egress {
                    write_capacity: capacity,
                    wire_bytes: wire,
                    ..Egress::default()
                },
                ..options::Config::default()
            },
            Disabled,
            deadline::Disabled,
            |error: Recovery<'_, '_>| -> Infallible { panic!("{name}: {:?}", error.cause()) },
            |cause| eprintln!("{name}: {cause:?}"),
            |outcome| outcome.expect(name),
        );
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
    run(Scenario {
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
        ],
        task_capacity: 9,
        cancellation: None,
        must_finish_before_last: None,
        linked: 2,
    });
}

#[test]
fn cancels_all() {
    for action in [
        CancelAction::RequestCancelled,
        CancelAction::StopSending,
        CancelAction::KeyClose,
    ] {
        let name = match action {
            CancelAction::RequestCancelled => "reset-cancellation",
            CancelAction::StopSending => "stop-sending-cancellation",
            CancelAction::KeyClose => "connection-close-cancellation",
        };
        let proof = run(Scenario {
            name,
            requests: vec![Spec::new(b"/async/cancel", b"")],
            expected_bodies: vec![None],
            task_capacity: 1,
            cancellation: Some(Cancellation {
                action,
                readiness: Readiness::TaskPollPending,
                expected_task_drops: 1,
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
            expected_task_drops: 0,
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
            expected_task_drops: 0,
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
fn task_then_followup() {
    for action in [CancelAction::RequestCancelled, CancelAction::StopSending] {
        let mut quick = Spec::new(b"/async/quick", b"");
        quick.dispatch = Dispatch::AfterCancellation;
        let proof = run(Scenario {
            name: "active-task-cancel-then-quick",
            requests: vec![Spec::new(b"/async/cancel", b""), quick],
            expected_bodies: vec![None, Some(b"quick")],
            task_capacity: 1,
            cancellation: Some(Cancellation {
                action,
                readiness: Readiness::TaskPollPending,
                expected_task_drops: 1,
            }),
            must_finish_before_last: None,
            linked: 0,
        });
        assert!(proof.cancel.issued);
        assert!(proof.cancel.retired);
        assert_eq!(proof.cancel.dropped.get(), 1);
        assert!(proof.cancel.followup);
    }
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
        options::Config::default(),
        Disabled,
        deadline::Disabled,
        recover,
        |cause| {
            let mut errors = diagnostics.take();
            errors.push(format!("{cause:?}"));
            diagnostics.set(errors);
        },
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
