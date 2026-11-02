pub mod common;

use common::schedule::turns::{Blocked, Stage, Writer};
use o3::depot::{arena::slab::key, capacity};
use sark_core::manifold::schedule::{Activation, Egress, Report, Runtime, Wake};

#[test]
fn resumes_blocked_control() {
    let key = key::Handle::from_parts(key::Parts::new(0, key::Generation::MIN));
    for (block, wake) in [
        (Blocked::Connection, Wake::Connection),
        (Blocked::Stream, Wake::Stream(key)),
    ] {
        let mut run = Runtime::try_bounded(capacity::Count::new(1), capacity::Count::new(1))
            .expect("one resident slot");
        run.try_insert(key, Stage::Head, Activation::control(None))
            .expect("control insertion");
        let mut writer = Writer {
            block: Some(block),
            remaining: 2,
            ..Writer::default()
        };
        run.drive(&mut writer);
        assert!(!run.has_runnable());
        assert_eq!(run.wake(wake), 1);
        run.drive(&mut writer);
        assert_eq!((writer.control, writer.data), (2, 0));
        assert!(!writer.invalid);
        assert!(matches!(run.get(key), Some(Stage::Body)));
    }
}

#[test]
fn resumes_inserted_control() {
    let key = key::Handle::from_parts(key::Parts::new(0, key::Generation::MIN));
    for (block, wake) in [
        (Blocked::Connection, Wake::Connection),
        (Blocked::Stream, Wake::Stream(key)),
    ] {
        let mut run = Runtime::try_bounded(capacity::Count::new(1), capacity::Count::new(1))
            .expect("one resident slot");
        run.vacant(key)
            .expect("vacant control slot")
            .insert(Stage::Trailer, Some(block));
        assert_eq!(run.wake(wake), 1);
        let mut writer = Writer {
            remaining: 1,
            ..Writer::default()
        };
        assert_eq!(run.drive(&mut writer).completed, 1);
        assert_eq!((writer.control, writer.data), (1, 0));
        assert!(!writer.invalid);
    }
}

#[test]
fn preserves_egress_order() {
    let keys =
        [0, 1].map(|index| key::Handle::from_parts(key::Parts::new(index, key::Generation::MIN)));
    for fused in [false, true] {
        let mut run = Runtime::try_bounded(capacity::Count::new(2), capacity::Count::new(2))
            .expect("two resident slots");
        let (stage, start) = if fused {
            (Stage::Fused, Activation::control(None))
        } else {
            (Stage::Body, Activation::data(None))
        };
        for (key, stage, start) in [
            (keys[0], stage, start),
            (keys[1], Stage::Body, Activation::data(None)),
        ] {
            run.try_insert(key, stage, start)
                .expect("resident insertion");
        }
        let mut writer = Writer {
            block: Some(Blocked::Egress),
            remaining: 1,
            ..Writer::default()
        };
        assert!(matches!(
            run.drive(&mut writer),
            Report {
                turns: 1,
                completed: 0
            }
        ));
        assert!(!run.has_runnable());
        assert_eq!((writer.control, writer.data), (usize::from(fused), 1));
        run.resume(|| None, Egress::Released);
        writer.remaining = 1;
        assert!(matches!(
            run.drive(&mut writer),
            Report {
                turns: 1,
                completed: 0
            }
        ));
        assert!(matches!(
            run.get(keys[usize::from(fused)]),
            Some(Stage::Trailer)
        ));
        assert!(matches!(
            run.get(keys[usize::from(!fused)]),
            Some(Stage::Body)
        ));
        assert_eq!((writer.control, writer.data), (usize::from(fused), 2));
        assert!(!writer.invalid);
    }
}
