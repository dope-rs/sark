pub mod common;

use dope_test::alloc::Tracker;
use o3::depot::{arena::slab::key, capacity, queue::indexed::lanes::typed::Value};
use sark_core::manifold::schedule::typed;

use crate::common::schedule::{
    turns::Blocked,
    typed::{Driver, Head},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn fused_barrier_retains() {
    let mut runtime =
        typed::Runtime::<key::Handle, Head, Box<usize>, Box<usize>, Blocked>::try_bounded(
            capacity::Count::new(3),
            capacity::Count::new(3),
        )
        .expect("resident domain");
    let keys = [0, 1, 2]
        .map(|index| key::Handle::from_parts(key::Parts::new(index, key::Generation::MIN)));
    let owner = Box::new(10);
    let address = owner.as_ref() as *const usize;
    let values = [
        typed::Next::Control(Head { owner, fused: true }, None),
        typed::Next::Control(
            Head {
                owner: Box::new(20),
                fused: false,
            },
            None,
        ),
        typed::Next::Data(Box::new(30), None),
    ];
    for (key, value) in keys.into_iter().zip(values) {
        assert!(runtime.try_insert(key, value).is_ok());
    }
    let mut driver = Driver {
        left: 3,
        block: Some(Blocked::Egress),
        ..Driver::default()
    };
    let (report, allocs) = Tracker::count(|| runtime.drive(&mut driver));
    assert_eq!(
        (report.turns, driver.heads, driver.bodies, allocs),
        (2, 2, 1, 0)
    );
    assert!(!runtime.has_runnable());

    runtime.wake_egress();
    driver.left = 3;
    let (report, allocs) = Tracker::count(|| runtime.drive_round(&mut driver));
    assert_eq!((report.turns, report.completed, allocs), (3, 0, 0));
    assert!(!runtime.has_work());
    for (key, expected) in keys.into_iter().zip([12, 21, 31]) {
        assert!(matches!(runtime.get(key), Some(Value::Data(value)) if **value == expected));
    }
    assert!(
        matches!(runtime.get(keys[0]), Some(Value::Data(value)) if std::ptr::eq(value.as_ref(), address))
    );
    let entry = runtime
        .entry(keys[0])
        .and_then(|entry| match entry {
            typed::Entry::Data(entry) => Some(entry),
            _ => None,
        })
        .expect("retained data phase");
    let (entry, prior) = entry.change(|mut owner| {
        let prior = *owner;
        *owner += 1;
        (owner, prior)
    });
    assert_eq!(prior, 12);
    entry.activate(None);
    driver.left = 1;
    driver.failure = Some(73);
    let (report, allocs) = Tracker::count(|| runtime.drive(&mut driver));
    assert_eq!((report.turns, driver.error, allocs), (1, Some(73), 0));
    assert!(!runtime.has_work());
    assert!(
        matches!(runtime.remove(keys[0]), Some(Value::Data(value)) if *value == 13 && std::ptr::eq(value.as_ref(), address))
    );
}

#[test]
fn idle_successor() {
    let mut runtime =
        typed::Runtime::<key::Handle, Head, Box<usize>, Box<usize>, Blocked>::try_bounded(
            capacity::Count::new(1),
            capacity::Count::new(1),
        )
        .expect("resident domain");
    let key = key::Handle::from_parts(key::Parts::new(0, key::Generation::MIN));
    assert!(
        runtime
            .try_insert(key, typed::Next::Idle(Box::new(7)))
            .is_ok()
    );
    assert!(!runtime.has_work());
    let entry = runtime
        .entry(key)
        .and_then(|entry| match entry {
            typed::Entry::Idle(entry) => Some(entry),
            _ => None,
        })
        .expect("idle phase");
    let (result, allocs) = Tracker::count(|| {
        entry.transition(|owner| {
            (
                Some(typed::Next::Control(
                    Head {
                        owner,
                        fused: false,
                    },
                    None,
                )),
                19,
            )
        })
    });
    assert_eq!((result, allocs), (19, 0));
    assert!(runtime.has_runnable());
    let entry = runtime
        .entry(key)
        .and_then(|entry| match entry {
            typed::Entry::Control(entry) => Some(entry),
            _ => None,
        })
        .expect("control phase");
    let owner = entry.transition(|value| (None, value.owner));
    assert_eq!(*owner, 7);
    assert!(runtime.is_empty());
}
