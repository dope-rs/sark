pub mod common;

use std::convert::Infallible;

use common::server::h3::{
    proof::{Dispatch, request::Spec},
    sessions::Scenario,
};
use dope::core::driver::life::recovery::Recovery;
use o3::base::time::Positive;
use sark_core::{
    deadline::{Phase, config::Config},
    life::{Event, session::Disabled},
};
use sark_h3::frame;

#[test]
fn handler_deadline() {
    for (mut pending, phase) in [
        (Spec::new(b"/async/cancel", b""), Phase::Handler),
        (
            Spec::new(b"/async/stream/park", b"retained-stream"),
            Phase::Total,
        ),
        (Spec::reserved(), Phase::Head),
    ] {
        pending.expect.error = Some(frame::ErrorCode::RequestCancelled);
        let mut quick = Spec::new(b"/async/quick", b"");
        quick.dispatch = Dispatch::AfterFailure;
        let proof = Scenario {
            name: "handler-deadline-then-quick",
            requests: vec![pending, quick],
            expected_bodies: vec![None, Some(b"quick")],
            task_capacity: 1,
            cancellation: None,
            must_finish_before_last: None,
            linked: 0,
        }
        .try_run_with(
            Default::default(),
            None,
            Disabled,
            Config::new(phase, Positive::from_secs::<1>()),
            |error: Recovery<'_, '_>| -> Infallible { panic!("{:?}", error.cause()) },
            |cause| eprintln!("lifecycle diagnostic: {cause:?}"),
            sark_core::life::mode::Observed::new,
            |outcome| outcome.expect("handler deadline and same-connection follow-up"),
        );
        if phase == Phase::Handler {
            assert_eq!(proof.cancel.dropped.get(), 1);
        } else if phase == Phase::Total {
            assert!(proof.stream.polls.get() >= 3);
            assert_eq!(proof.stream.drops.get(), 1);
            assert!(!proof.stream.moved.get());
        }
        assert!(proof.cancel.followup);
        assert_eq!(
            proof
                .trace
                .events
                .iter()
                .filter(|event| matches!(
                    event,
                    Event::ExchangeDeadlineExpired {
                        phase: expired,
                        ..
                    } if *expired == phase
                ))
                .count(),
            1
        );
    }
}
