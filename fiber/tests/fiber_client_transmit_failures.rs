use std::convert::Infallible;

use dope::{
    core::Errno,
    manifold::client::conn::Transmit,
    net::link::{event::SendFailure, slot::transmit::Invalid},
};
use dope_fiber::reply::{Progress, order::Depth};
use o3::{
    base::cell::region,
    depot::{arena::slab::Vacancy, capacity::Count},
};
use sark_client::conn::{Error, Kind};
use sark_fiber::client::config::report::{Arm, Failure, Never, Policy, Retain};

#[test]
fn native_cause_survives_deferred_retirement() {
    for expected in [
        Transmit::Io(SendFailure::Queued(Errno::BAD_DESCRIPTOR)),
        Transmit::Rejected(SendFailure::WriteShutdown(Vacancy::Full.into())),
        Transmit::Invalid(Invalid::SettleStale),
    ] {
        region::Token::scope(|mut token| {
            let state = <Never as Policy<Infallible>>::vacant(&token);
            <Never as Policy<Infallible>>::failed(&state, &mut token, Failure::Transmit(expected));
            <Never as Policy<Infallible>>::failed(
                &state,
                &mut token,
                Failure::Receive(Errno::CANCELED),
            );
            <Never as Policy<Infallible>>::failed(
                &state,
                &mut token,
                Failure::Transmit(Transmit::Io(SendFailure::Terminal(Errno::CANCELED))),
            );
            <Never as Policy<Infallible>>::failed(
                &state,
                &mut token,
                Failure::Receive(Errno::BAD_DESCRIPTOR),
            );
            for progress in [
                Progress::Pending(Depth::ZERO),
                Progress::Complete(Depth::ZERO),
            ] {
                let observed =
                    <Never as Policy<Infallible>>::retire(&state, &mut token, |cause, _| {
                        assert!(matches!(
                            cause,
                            Some(Error::Transmit { error, receive: Some(errno), wire: None })
                                if *error == expected && *errno == Errno::CANCELED
                        ));
                        progress
                    });
                assert!(observed == progress);
            }
            assert!(matches!(
                <Never as Policy<Infallible>>::arm(&Never, &state, &mut token),
                Arm::Ready,
            ));
        });
    }
}

#[test]
fn wire_and_native_causes_retain_the_original_borrow() {
    let text = String::from("original wire failure");
    let expected = Transmit::Rejected(SendFailure::Direct(Vacancy::IdentityExhausted.into()));
    let pool = <Retain as Policy<&str>>::try_pool(Count::new(1)).expect("failure store");
    let source = <Retain as Policy<&str>>::source(&pool);
    for wire_first in [false, true] {
        region::Token::scope(|mut token| {
            let state = <Retain as Policy<&str>>::vacant(&token);
            assert!(matches!(
                <Retain as Policy<&str>>::arm(&source, &state, &mut token),
                Arm::Ready,
            ));
            <Retain as Policy<&str>>::failed(
                &state,
                &mut token,
                Failure::Receive(Errno::BAD_DESCRIPTOR),
            );
            let wire = Failure::Wire(text.as_str());
            let send = Failure::Transmit(expected);
            let causes = if wire_first {
                [wire, send]
            } else {
                [send, wire]
            };
            for cause in causes {
                <Retain as Policy<&str>>::failed(&state, &mut token, cause);
            }
            let mut retained = None;
            for progress in [
                Progress::Pending(Depth::ZERO),
                Progress::Complete(Depth::ZERO),
            ] {
                let observed = <Retain as Policy<&str>>::retire(&state, &mut token, |cause, _| {
                    let cause = cause.expect("connection failure");
                    assert!(cause.kind() == Kind::Wire);
                    let Error::Transmit {
                        error,
                        receive,
                        wire: Some(detail),
                    } = cause
                    else {
                        panic!("missing exact connection causes")
                    };
                    assert_eq!(*error, expected);
                    assert_eq!(*receive, Some(Errno::BAD_DESCRIPTOR));
                    assert_eq!(**detail, text);
                    retained = Some(cause.try_clone().expect("retained response cause"));
                    progress
                });
                assert!(observed == progress);
            }
            let retained = retained.expect("published response failure");
            let Error::Transmit {
                error,
                receive,
                wire: Some(detail),
            } = retained
            else {
                panic!("lost published connection causes")
            };
            assert_eq!(error, expected);
            assert_eq!(receive, Some(Errno::BAD_DESCRIPTOR));
            assert_eq!(*detail, text);
            assert!(std::ptr::eq((*detail).as_ptr(), text.as_ptr()));
        });
    }
}
