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
        assert_eq!(runtime.drive_round(&mut driver).turns, 0);
        assert_eq!(driver.probes, 0);
        let keys = [0, 1]
            .map(|index| key::Handle::from_parts(key::Parts::new(index, key::Generation::MIN)));
        for key in keys {
            (runtime.try_insert(key, 0, start))
                .as_ref()
                .expect("test operation must succeed");
        }
        let (result, allocs) = Tracker::count(|| {
            let (entry, result) = runtime
                .entry(keys[0])
                .expect("first scheduled owner")
                .change(|value| (value + 4, 11));
            assert_eq!(result, 11);
            entry.try_control::<(), _>(|value| Err((value, 7)))
        });
        assert_eq!(result, Err(7));
        assert_eq!(allocs, 0);
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
    for drive in [
        schedule::Runtime::drive::<Driver>,
        schedule::Runtime::drive_round::<Driver>,
    ] {
        let mut runtime = schedule::Runtime::<key::Handle, usize, Block>::try_bounded(
            capacity::Count::new(2),
            capacity::Count::new(2),
        )
        .expect("two scheduler entries");
        let keys = [0, 1]
            .map(|index| key::Handle::from_parts(key::Parts::new(index, key::Generation::MIN)));
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
            assert_eq!(drive(&mut runtime, &mut driver).turns, 1);
            assert_eq!(driver.probes, 2);
            assert_eq!(
                keys.map(|key| *runtime.get(key).expect("scheduled owner")),
                expected
            );
        }
    }
}

#[test]
fn decision_provenance() -> Result<(), dope_test::checks::Error> {
    use std::env;

    use common::schedule::contracts::{CONTRACTS, PROOF};
    use dope_test::checks::Suite;

    let suite = Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &["sark-core", "o3"]);
    let exe = env::current_exe()?;
    let output = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.output()?;
    let artifacts = suite.try_collect(&exe, output)?;
    for &(body, expected, site) in CONTRACTS {
        let source = format!(
            "{}\n{PROOF}\n{body}\nfn main() {{}}",
            include_str!("common/schedule/drive.rs")
        );
        let output = artifacts.try_compile(&source)?;
        let diag = String::from_utf8(output.stderr).expect("diagnostic UTF-8");
        assert!(
            output.status.success() == expected.is_empty()
                && diag.contains(expected)
                && diag.contains(site),
            "{body}\n{diag}"
        );
    }
    Ok(())
}
