pub mod common;
use std::mem;

use dope_test::alloc::Tracker;
use o3::depot::{arena::slab::key, capacity};
use sark_core::manifold::{schedule, schedule::Runtime};

use crate::common::schedule::turns::*;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn preserves_continuations() {
    let mut run = Runtime::try_bounded(capacity::Count::new(1), capacity::Count::new(1))
        .expect("one resident slot");
    let key = key::Handle::from_parts(key::Parts::new(0, key::Generation::MIN));
    (run.try_insert(key, Stage::Head, schedule::Activation::control(None)))
        .as_ref()
        .expect("test operation must succeed");
    let mut writer = Writer {
        remaining: 5,
        ..Writer::default()
    };
    let ((), allocs) = Tracker::count(|| {
        assert!(matches!(
            run.drive(&mut writer),
            schedule::Report {
                turns: 1,
                completed: 0
            }
        ));
        assert!(!run.has_runnable());
        run.resume(|| None, schedule::Egress::Released);
        assert!(matches!(
            run.drive(&mut writer),
            schedule::Report {
                turns: 1,
                completed: 0
            }
        ));
        assert!(matches!(run.get(key), Some(Stage::Body)));
        assert!(matches!(
            run.drive(&mut writer),
            schedule::Report {
                turns: 1,
                completed: 0
            }
        ));
        assert!(!run.has_runnable());
        assert_eq!(run.wake(schedule::Wake::Stream(key)), 1);
        assert!(matches!(
            run.drive(&mut writer),
            schedule::Report {
                turns: 1,
                completed: 0
            }
        ));
        assert!(matches!(run.get(key), Some(Stage::Trailer)));
        assert!(matches!(
            run.drive(&mut writer),
            schedule::Report {
                turns: 1,
                completed: 1
            }
        ));
    });
    assert_eq!(allocs, 0);
    assert!(!writer.invalid);
    assert!(run.is_empty());
    assert_eq!(
        (writer.control, writer.data, writer.closed, writer.idle),
        (3, 2, 1, 0)
    );
    assert_eq!(mem::size_of::<schedule::ControlTurn<Blocked>>(), 3);
    assert_eq!(mem::size_of::<schedule::DataTurn<Blocked>>(), 2);
}

#[test]
fn preserves_idle_owner() {
    for (ready, control, data) in [
        (None, 0, 0),
        (Some(schedule::WorkClass::Control), 1, 0),
        (Some(schedule::WorkClass::Data), 0, 1),
    ] {
        let mut run = Runtime::try_bounded(capacity::Count::new(1), capacity::Count::new(1))
            .expect("one resident slot");
        let key = key::Handle::from_parts(key::Parts::new(0, key::Generation::MIN));
        let mut writer = Writer::default();
        let ((), allocs) = Tracker::count(|| {
            run.vacant(key)
                .expect("vacant owner")
                .store(Stage::Body, ready);
            assert!(matches!(
                run.drive(&mut writer),
                schedule::Report {
                    turns: 0,
                    completed: 0
                }
            ));
            assert_eq!(run.has_runnable(), ready.is_some());
            writer.remaining = 1;
            assert_eq!(run.drive(&mut writer).turns, control + data);
        });
        assert_eq!(allocs, 0);
        assert!(!run.has_runnable());
        assert!(matches!(run.get(key), Some(Stage::Body)));
        assert_eq!(
            (writer.control, writer.data, writer.idle),
            (control, data, control)
        );
        assert_eq!(writer.closed, 0);
        assert!(!writer.invalid);
    }
}
