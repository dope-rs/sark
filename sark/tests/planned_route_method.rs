pub mod common;

use common::router::{ProbeRouter, planned};
use sark_core::{
    body,
    decode::{Decode, HeadError, request::Head as _},
    manifold::stream::Boundary,
};
use sark_http::field::metadata::{block::Version, section::rules::RetainRaw};

#[test]
fn misses_valid_method() {
    let router = ProbeRouter::new();
    for version in [Version::Two, Version::Three] {
        for method in planned::UNMATCHED_METHODS
            .into_iter()
            .chain([b"EXTENSION-METHOD-LONG".as_slice()])
        {
            let selected = planned::try_head(version, method, planned::PATH)
                .expect("retained request")
                .try_request()
                .ok()
                .expect("valid request control");
            let raw = planned::try_fields::<RetainRaw>(version, method, planned::PATH, &[])
                .expect("retained raw head")
                .try_request()
                .ok()
                .expect("valid raw request control");
            let prefixed =
                planned::try_fields::<RetainRaw<26>>(version, method, planned::PATH, &[])
                    .expect("retained prefixed head")
                    .try_request()
                    .ok()
                    .expect("valid prefixed request control");
            for result in [
                router.try_head(selected),
                router.try_full(raw),
                sark_core::decode::Full::try_prefixed::<26>(&router, prefixed),
            ] {
                assert!(matches!(result, Err(HeadError::NotFound)));
            }
        }
    }
}

#[test]
fn rejects_invalid_path() {
    for version in [Version::Two, Version::Three] {
        for method in planned::UNMATCHED_METHODS {
            let head = planned::try_head(version, method, planned::INVALID_PATH)
                .expect("retained request");
            assert!(head.try_request().is_err());
        }
    }
}

#[test]
fn rejects_invalid_method() {
    for version in [Version::Two, Version::Three] {
        let head = planned::try_head(version, planned::INVALID_METHOD, planned::PATH)
            .expect("retained request");
        assert!(head.try_request().is_err());
    }
}

#[test]
fn admitted_content_length() {
    let router = ProbeRouter::new();
    for version in [Version::Two, Version::Three] {
        for (limit, expected) in [
            (7, None),
            (
                6,
                Some(body::Error::TooLarge {
                    actual: 7,
                    limit: 6,
                }),
            ),
        ] {
            let lengths: &[&[u8]] = &[b"7, 7", b"07"];
            let selected = planned::try_fields::<<ProbeRouter as Decode>::Plan>(
                version,
                b"POST",
                planned::PATH,
                lengths,
            )
            .expect("retained selected head")
            .try_request()
            .ok()
            .expect("valid request control");
            let raw = planned::try_fields::<RetainRaw>(version, b"POST", planned::PATH, lengths)
                .expect("retained raw head")
                .try_request()
                .ok()
                .expect("valid request control");
            let prefixed =
                planned::try_fields::<RetainRaw<26>>(version, b"POST", planned::PATH, lengths)
                    .expect("retained prefixed head")
                    .try_request()
                    .ok()
                    .expect("valid prefixed request control");
            for result in [
                router.try_head(selected),
                router.try_full(raw),
                sark_core::decode::Full::try_prefixed::<26>(&router, prefixed),
            ] {
                assert_eq!(
                    result
                        .expect("admitted request length")
                        .try_start(limit, Boundary::More)
                        .err()
                        .map(|failure| failure.into_parts().1),
                    expected,
                );
            }
        }
    }
}
