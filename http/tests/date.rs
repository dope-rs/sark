pub mod common;

use common::wire::date::{CASES, MAX_SECONDS, reference};
use sark_http::{field::date::Stamp, o3::base::time};

#[test]
fn known_imf_fixdates() {
    let epoch = const { Stamp::from_unix(time::Unix::ZERO) };
    assert_eq!(epoch.map(|stamp| *stamp.as_bytes()), Some(*CASES[0].1));
    for (seconds, expected) in CASES {
        let stamp = Stamp::from_unix(time::Unix::new(seconds)).expect("supported civil year");
        assert_eq!(stamp.as_bytes(), expected);
    }
}

#[test]
fn every_day_matches_ref() {
    let last_day = MAX_SECONDS / 86_400;
    for day in 0..=last_day {
        for daytime in [0, 43_210, 86_399] {
            let seconds = (day * 86_400 + daytime).min(MAX_SECONDS);
            let stamp = Stamp::from_unix(time::Unix::new(seconds)).expect("clock range");
            assert_eq!(*stamp.as_bytes(), reference(seconds));
        }
    }
}

#[test]
fn rejects_unsupported_year() {
    for seconds in [253_402_300_800, u64::MAX] {
        assert!(Stamp::from_unix(time::Unix::new(seconds)).is_none());
    }
}
