pub mod common;

use std::convert::Infallible;

use common::server::h3::{proof::request::Spec, sessions::Scenario};
use dope::core::driver::life::recovery::Recovery;
use sark_core::{life::session::Disabled, manifold::stream::Boundary};
use sark_h3::manifold::server::deadline;

#[test]
fn head_response_paths() {
    let mut instant = Spec::new(b"/body", b"");
    instant.head.method = b"HEAD";
    let mut sync = Spec::new(b"/body", b"sync-body");
    sync.head.method = b"HEAD";
    sync.data.split = Some(4);
    let mut task = Spec::new(b"/async/raw", b"task-body");
    task.head.method = b"HEAD";
    let mut stream = Spec::new(b"/async/stream/fail", b"unpolled-stream-body");
    stream.head.method = b"HEAD";
    let mut guard = Spec::new(b"/guarded", b"unread");
    guard.head.method = b"HEAD";
    guard.data.boundary = Boundary::More;
    guard.expect.status = b"401";
    guard.expect.input_finished = false;
    let proof = Scenario {
        name: "head-response-paths",
        requests: vec![instant, sync, task, stream, guard],
        expected_bodies: vec![Some(b""), Some(b""), Some(b""), Some(b""), Some(b"")],
        task_capacity: 2,
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
        |error: Recovery<'_, '_>| -> Infallible { panic!("{:?}", error.cause()) },
        |cause| eprintln!("lifecycle diagnostic: {cause:?}"),
        sark_core::life::mode::Observed::new,
        |outcome| outcome.expect("HEAD sync, task and guard responses"),
    );
    assert_eq!(proof.stream.polls.get(), 0);
    assert_eq!(proof.stream.drops.get(), 1);
}

#[test]
fn pending_response_head_ownership() {
    use std::{cell::Cell, num::NonZero};

    use common::server::h3::{
        observer::qpack_pressure,
        proof::{CancelAction, Cancellation, Dispatch, Readiness},
    };
    use dope_test::harness::observe::VecCellExt as _;
    use o3::base::time::Positive;
    use sark_core::{
        deadline::{Phase, config::Config},
        life::Event,
    };
    use sark_h3::{conn::options, frame, qpack};

    let unlimited = usize::MAX;
    for (action, fixed, slots, wire, resident) in [
        (
            Some(CancelAction::StopSending),
            false,
            2,
            unlimited,
            unlimited,
        ),
        (
            Some(CancelAction::RequestCancelled),
            false,
            2,
            unlimited,
            unlimited,
        ),
        (
            Some(CancelAction::QpackAcknowledgment),
            false,
            2,
            unlimited,
            unlimited,
        ),
        (Some(CancelAction::QpackIncrement), false, 8, 900, unlimited),
        (None, false, 2, unlimited, unlimited),
        (Some(CancelAction::StopSending), false, 8, 900, unlimited),
        (Some(CancelAction::StopSending), false, 8, unlimited, 9000),
        (Some(CancelAction::StopSending), true, 8, 900, 10000),
    ] {
        let invalid_feedback = matches!(
            action,
            Some(CancelAction::QpackAcknowledgment | CancelAction::QpackIncrement)
        );
        let expired = action.is_none();
        let cancel_head = action != Some(CancelAction::StopSending);
        let followup = cancel_head && !invalid_feedback && !expired;
        let mut blocker = Spec::new(b"/async/stream/two_park", b"");
        blocker.read_response = false;
        let pending = Spec::new(
            if fixed {
                b"/async/qpack/fixed"
            } else {
                b"/async/qpack"
            },
            b"q",
        );
        let (mut requests, mut expected) = if cancel_head {
            (vec![pending, blocker], vec![None, None])
        } else {
            (vec![blocker, pending], vec![None, Some(b"q".as_slice())])
        };
        if expired {
            for request in &mut requests {
                request.expect.error = Some(frame::ErrorCode::RequestCancelled);
            }
        } else if invalid_feedback {
            for request in &mut requests {
                request.expect.connection_closed = true;
            }
        } else if cancel_head {
            requests[1].expect.error = Some(frame::ErrorCode::RequestCancelled);
            let mut next = Spec::new(b"/async/qpack", b"q");
            next.dispatch = Dispatch::AfterCancellation;
            requests.push(next);
            expected.push(Some(b"q"));
        }
        let diagnostics = Cell::new(Vec::new());
        let mut deadline = Config::new(Phase::Total, Positive::from_secs::<1>());
        if expired {
            deadline = deadline.with(Phase::ResponseAdmission, Positive::from_millis::<250>());
        }
        let proof = Scenario {
            name: "pending-response-head",
            requests,
            expected_bodies: expected,
            task_capacity: 2,
            cancellation: action.map(|action| Cancellation {
                action,
                readiness: Readiness::TaskPollReady(usize::from(!cancel_head)),
            }),
            must_finish_before_last: None,
            linked: usize::from(fixed),
        }
        .try_run_with(
            common::server::h3::sessions::plain,
            options::Config {
                egress: options::Egress {
                    write_capacity: slots,
                    wire_bytes: wire,
                    resident_bytes: resident,
                },
                ..Default::default()
            },
            NonZero::new(700),
            Disabled,
            deadline,
            |error: Recovery<'_, '_>| -> Infallible { panic!("{:?}", error.cause()) },
            |cause| diagnostics.push(format!("{cause:?}")),
            qpack_pressure,
            |outcome| {
                outcome.unwrap_or_else(|cause| panic!(
                "pending head {action:?}/{cancel_head}/{fixed}/{slots}/{wire}/{resident}: {cause:?}"
            ))
            },
        );
        let (_, stream) = proof.replies.streams[0].expect("first stream");
        assert_eq!(
            proof.stream.heads.get(),
            1 + usize::from(followup),
            "each response emits its header source once across admission pressure"
        );
        if invalid_feedback || expired {
            let polls = proof
                .trace
                .events
                .iter()
                .filter_map(|event| match event {
                    Event::TaskPollReady(exchange) if exchange.stream() == stream => Some(true),
                    Event::TaskPollPending(exchange) if exchange.stream() == stream => Some(false),
                    _ => None,
                })
                .skip_while(|ready| !ready);
            assert!(polls.eq([true]), "pending head never polls its body");
        }
        assert_eq!(
            proof.stream.drops.get(),
            if followup { 3 } else { 2 - usize::from(fixed) }
        );
        assert!(!proof.stream.moved.get());
        let committed = proof
            .trace
            .events
            .iter()
            .filter(|event| matches!(event, Event::ResponseCommitted { .. }))
            .count();
        assert_eq!(committed, if invalid_feedback || expired { 1 } else { 2 });
        assert_eq!(proof.cancel.followup, followup);
        if !invalid_feedback && !expired {
            assert!(proof.cancel.issued && proof.cancel.retired);
            assert!(!proof.trace.events.iter().any(|event| matches!(event,
                Event::ExchangeDeadlineExpired { exchange, .. }
                    if !cancel_head || exchange.stream() == stream
            )));
        }
        if expired {
            assert!(proof.trace.events.iter().any(|event| matches!(
                event,
                Event::TaskPollReady(exchange) if exchange.stream() == stream
            )));
            assert!(proof.trace.events.iter().any(|event| matches!(
                event,
                Event::ExchangeDeadlineExpired { exchange, phase: Phase::ResponseAdmission, .. }
                    if exchange.stream() == stream
            )));
        }
        let expected = if invalid_feedback {
            let error = if action == Some(CancelAction::QpackIncrement) {
                qpack::stream::Error::IncrementBeyondTable {
                    known: 0,
                    increment: 1,
                    inserted: 0,
                }
            } else {
                qpack::stream::Error::UnknownSection {
                    stream_id: stream.get(),
                }
            };
            vec![format!(
                "{:?}",
                sark_h3::manifold::Error::H3(sark_h3::conn::Error::QpackDecoderStream(error))
            )]
        } else {
            Vec::new()
        };
        assert_eq!(diagnostics.take(), expected);
    }
}
