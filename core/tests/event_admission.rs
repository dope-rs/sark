use std::{cell::Cell, convert::Infallible};

use dope_core::{
    BranchError,
    driver::{life::recovery, schedule, settings},
};
use dope_test::scope::{self, Driver};
use sark_core::manifold::server::drain;

#[test]
fn retained_event_waits() {
    let mut reports1 = Vec::new();
    Driver::new(settings::Config::default()).with_scope(
        |scope| {
            scope.with_turn(|_, _, mut sched| {
                let active = sched.begin();
                let work = active.turn().app();
                let calls = Cell::new(0);
                drain::Events::fair(work, |mut drain| {
                    assert!(matches!(drain.next_ready::<()>(|| None), Err(drain::Empty)));
                    assert_eq!(work.remaining(), schedule::LIMIT / 2 - 1);
                    for expected in 1..schedule::LIMIT / 2 {
                        let permit = drain.permit().expect("an event turn remains available");
                        let item = permit.run(|| {
                            calls.set(calls.get() + 1);
                            expected
                        });
                        assert_eq!(item, expected);
                    }

                    let deferred = drain.next_ready(|| {
                        calls.set(calls.get() + 1);
                        Some(11)
                    });
                    assert!(matches!(deferred, Ok(None)));
                    assert_eq!(calls.get(), schedule::LIMIT / 2 - 1);

                    let report = drain.finish();
                    assert!(report.has_drained());
                    assert!(report.is_exhausted());
                });
                assert_eq!(work.remaining(), schedule::LIMIT / 2);
            });
        },
        recovery::Policy::new(
            |event| reports1.push(event),
            |error: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("native recovery: {:?}", error.cause())
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(|error| match error {
                    scope::Error::Create(error) => BranchError::First(error.kind()),
                    scope::Error::Close(error) => BranchError::Second(error.cause()),
                })
                .expect("driver lifetime");
        },
    );
    assert!(
        reports1.is_empty(),
        "identity-only scope returned I/O: {reports1:?}"
    );
}
