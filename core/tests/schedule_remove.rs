pub mod common;
use std::{cell::Cell, ptr};

use common::schedule::remove::{Blocked, Failure, Reject};
use dope_test::alloc::Tracker;
use o3::depot::{arena::slab::key, capacity};
use sark_core::manifold::schedule::{self, Runtime, Terminal};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retain_on_failure() {
    let built = Runtime::<key::Handle, Box<usize>, Blocked>::try_bounded(
        capacity::Count::new(2),
        capacity::Count::new(2),
    );
    let mut run = built.expect("test operation must succeed");
    let key = key::Handle::from_parts(key::Parts::new(0, key::Generation::MIN));
    let entry = run.vacant(key);
    let entry = entry.expect("test owner must be present");
    let value = Box::new(7);
    let address = ptr::from_ref(&*value);
    entry.insert(value, Some(Blocked::Stream));

    let (result, allocs) = Tracker::count(|| {
        run.try_remove(key, |value| {
            **value -= 2;
            Err(Failure::Pending { remaining: **value })
        })
    });
    assert_eq!(allocs, 0);
    assert!(matches!(result, Err(Failure::Pending { remaining: 5 })));
    assert_eq!(
        run.get(key).map(|value| ptr::from_ref(&**value)),
        Some(address)
    );
    assert_eq!(run.get(key).map(|value| **value), Some(5));
    assert_eq!(run.len(), 1);
    assert_eq!(run.wake(schedule::Wake::Stream(key)), 1);

    let (result, allocs) = Tracker::count(|| {
        run.try_remove(key, |value| {
            assert_eq!(**value, 5);
            Ok::<(), Failure>(())
        })
    });
    assert_eq!(allocs, 0);
    assert!(matches!(result, Ok(Some(value)) if ptr::from_ref(&*value) == address));
    assert_eq!(run.len(), 0);
    assert!(!run.contains(key));
    assert_eq!(run.wake(schedule::Wake::Stream(key)), 0);
}

#[test]
fn skip_absent_keys() {
    let built = Runtime::<key::Handle, usize, Blocked>::try_bounded(
        capacity::Count::new(2),
        capacity::Count::new(2),
    );
    let mut run = built.expect("test operation must succeed");
    let epoch = key::Generation::MIN;
    let stored = key::Handle::from_parts(key::Parts::new(0, epoch));
    (run.try_insert(stored, 7, schedule::Activation::Keep))
        .as_ref()
        .expect("test operation must succeed");

    let next = epoch.checked_add(1).expect("next epoch is representable");
    for key in [
        key::Handle::from_parts(key::Parts::new(0, next)),
        key::Handle::from_parts(key::Parts::new(1, epoch)),
        key::Handle::from_parts(key::Parts::new(2, epoch)),
    ] {
        let (result, allocs) = Tracker::count(|| {
            run.try_remove(key, |value| Err(Failure::Pending { remaining: *value }))
        });
        assert_eq!(allocs, 0);
        assert!(matches!(result, Ok(None)));
        assert_eq!(run.get(stored), Some(&7));
        assert_eq!(run.len(), 1);
    }
}

#[test]
fn settle_control_failure() {
    for (retain, owned) in [(false, false), (true, false), (false, true)] {
        let mut run = Runtime::<key::Handle, Box<usize>, Blocked>::try_bounded(
            capacity::Count::new(1),
            capacity::Count::new(1),
        )
        .expect("bounded runtime fixture");
        let key = key::Handle::from_parts(key::Parts::new(0, key::Generation::MIN));
        let value = Box::new(7);
        let address = ptr::from_ref(&*value);
        assert!(
            run.try_insert(key, value, schedule::Activation::control(None))
                .is_ok()
        );
        let dropped = Cell::new(false);
        let mut driver = Reject {
            calls: 0,
            settled: None,
            repeat: false,
            data: false,
            retain,
            owned,
            live: false,
            dropped: &dropped,
        };
        let (report, allocs) = Tracker::count(|| run.drive(&mut driver));
        assert_eq!(allocs, 0);
        assert_eq!(driver.calls, 1);
        assert!(driver.live);
        assert!(matches!(
            driver.settled,
            Some((7, 1, Terminal::Failed(13), true))
        ));
        assert!(!driver.repeat);
        assert!(!driver.data);
        assert_eq!(report.turns, 1);
        assert_eq!(report.completed, 0);
        assert!(!run.has_runnable());
        assert_eq!(
            run.get(key).map(|value| ptr::from_ref(&**value)),
            retain.then_some(address)
        );
    }
}
