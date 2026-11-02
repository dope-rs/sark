use std::ops::ControlFlow;
pub mod common;

use common::requests::{FIELDS, QUERIES, RAW};
use sark_core::route::field::query::{Entries, Error};

#[test]
fn tokens_keep_offsets() {
    for &(end, count) in QUERIES {
        let mut entries = Entries::try_new(RAW, 2..end).expect("query range");
        for [(name, range), (value, span)] in FIELDS.iter().take(count) {
            let field = entries.next().expect("query field");
            assert_eq!(field.name(), *name);
            assert_eq!(field.name_range(), *range);
            let actual = field.try_value().expect("value");
            assert_eq!(actual.as_ref(), *value);
            assert_eq!(actual.as_ref().as_ptr(), RAW[span.start..].as_ptr());
            let actual = field.try_span().expect("span");
            assert_eq!(actual.start()..actual.end(), *span);
        }
        assert!(entries.next().is_none());
    }
}

#[test]
fn fold_resumes() {
    let (end, count) = QUERIES[1];
    let mut entries = Entries::try_new(RAW, 2..end).expect("query range");
    let (seen, stopped) = entries
        .by_ref()
        .try_fold(0, |seen, entry| {
            if entry.name() == FIELDS[1][0].0 {
                ControlFlow::Break((seen, entry))
            } else {
                ControlFlow::Continue(seen + 1)
            }
        })
        .break_value()
        .expect("stop");
    assert_eq!(seen, 1);
    assert_eq!(stopped.name_range(), FIELDS[1][0].1);
    assert_eq!(
        entries.try_for_each(|entry| Err::<(), _>(entry.name_range())),
        Err(FIELDS[2][0].1.clone())
    );
    assert_eq!(
        entries.next().expect("resumed").name_range(),
        FIELDS[3][0].1
    );
    assert_eq!(
        stopped.name().as_ptr(),
        RAW[FIELDS[1][0].1.start..].as_ptr()
    );
    assert_eq!(
        entries.try_fold(0, |seen, _| Some(seen + 1)),
        Some(count - 4)
    );
    assert!(entries.next().is_none());
}

#[test]
fn range_errors_are_exact() {
    assert!(matches!(
        Entries::try_new(RAW, core::ops::Range { start: 2, end: 1 }),
        Err(Error::Order { start: 2, end: 1, capacity }) if capacity == RAW.len()
    ));
    assert!(matches!(
        Entries::try_new(RAW, 0..RAW.len() + 1),
        Err(Error::Bounds { start: 0, end, capacity })
            if end == RAW.len() + 1 && capacity == RAW.len()
    ));
}
