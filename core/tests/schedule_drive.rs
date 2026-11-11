pub mod common;

use dope_test::alloc::Tracker;
use o3::depot::{arena::slab::key, capacity};
use sark_core::manifold::schedule;

use crate::common::schedule::drive::*;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retains_blocked_work() {
    for start in [
        schedule::Activation::control(None),
        schedule::Activation::data(None),
    ] {
        let mut runtime = schedule::Runtime::<key::Handle, usize, Block>::try_bounded(
            capacity::Count::new(2),
            capacity::Count::new(2),
        )
        .expect("two scheduler entries");
        let mut driver = Driver {
            remaining: 1,
            probes: 0,
        };
        assert_eq!(runtime.drive(&mut driver).turns, 0);
        assert_eq!(driver.probes, 0);
        let keys = [0, 1]
            .map(|index| key::Handle::from_parts(key::Parts::new(index, key::Generation::MIN)));
        for key in keys {
            (runtime.try_insert(key, 0, start))
                .as_ref()
                .expect("test operation must succeed");
        }
        *runtime
            .entry(keys[0])
            .expect("first scheduled owner")
            .get_mut() += 4;
        driver = Driver {
            remaining: 1,
            probes: 0,
        };
        let (report, allocs) = Tracker::count(|| runtime.drive(&mut driver));
        assert_eq!(allocs, 0);
        assert_eq!(report.turns, 1);
        assert_eq!(driver.probes, 2);
        assert_eq!(runtime.get(keys[0]), Some(&5));
        assert_eq!(runtime.get(keys[1]), Some(&0));

        driver = Driver {
            remaining: 2,
            probes: 0,
        };
        let (report, allocs) = Tracker::count(|| runtime.drive(&mut driver));
        assert_eq!(allocs, 0);
        assert_eq!(report.turns, 2);
        assert_eq!(driver.probes, 2);
        assert_eq!(runtime.get(keys[0]), Some(&6));
        assert_eq!(runtime.get(keys[1]), Some(&1));
    }
}

#[test]
fn alternates_one_permit() {
    let mut runtime = schedule::Runtime::<key::Handle, usize, Block>::try_bounded(
        capacity::Count::new(2),
        capacity::Count::new(2),
    )
    .expect("two scheduler entries");
    let keys =
        [0, 1].map(|index| key::Handle::from_parts(key::Parts::new(index, key::Generation::MIN)));
    for (key, start) in keys.into_iter().zip([
        schedule::Activation::control(None),
        schedule::Activation::data(None),
    ]) {
        runtime
            .try_insert(key, 0, start)
            .as_ref()
            .expect("test operation must succeed");
    }
    for expected in [[1, 0], [1, 1], [2, 1], [2, 2]] {
        let mut driver = Driver {
            remaining: 1,
            probes: 0,
        };
        assert_eq!(runtime.drive(&mut driver).turns, 1);
        assert_eq!(driver.probes, 2);
        assert_eq!(
            keys.map(|key| *runtime.get(key).expect("scheduled owner")),
            expected
        );
    }
}
