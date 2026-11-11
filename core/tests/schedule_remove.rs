pub mod common;
use std::{cell::Cell, ptr};

use common::schedule::remove::{Blocked, Reject};
use dope_test::alloc::Tracker;
use o3::depot::{arena::slab::key, capacity};
use sark_core::manifold::schedule::{self, Runtime, Terminal};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn settle_control_failure() {
    for retain in [false, true] {
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
