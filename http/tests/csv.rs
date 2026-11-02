pub mod common;

use common::framing::{INVALID_CONNECTION_LINE, expectation};
use dope_test::alloc::Tracker;
use sark_http::{
    field::head::{self, Flags, headers::Known},
    h1::{
        field::{self, expect::Decision},
        head::{
            line::Version,
            request::{Coding, Fields},
        },
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retains_empty_members() {
    for (known, value) in [
        (Known::Connection, b"keep-alive, close".as_slice()),
        (Known::Expect, b"100-continue".as_slice()),
        (Known::AcceptEncoding, b"br, GZIP;q=1".as_slice()),
    ] {
        for (prefix, suffix, trim) in [
            (b"".as_slice(), b"".as_slice(), 0),
            (b" ,\t,, ", b" ,\t, ", 1),
        ] {
            let raw = [prefix, value, suffix, b"\r\n"].concat();
            let mut scan = Fields::default();
            let mut flags = Flags::default();
            assert!(
                (known
                    .scan_line(&mut scan, &mut flags, &raw, Version::Eleven)
                    .map(|bounds| {
                        bounds.map(|bounds| (bounds.tail(), bounds.start(), bounds.end()))
                    }))
                    == (Ok(Some((raw.len() - 2, trim, raw.len() - 2 - trim)))),
                "field {raw:?}"
            );
            assert_eq!(
                scan.routing.connection().is_keep(),
                known == Known::Connection
            );
            assert_eq!(
                scan.routing.connection().is_close(),
                known == Known::Connection
            );
            assert_eq!(
                scan.message.coding() == Coding::Gzip,
                known == Known::AcceptEncoding
            );
            assert_eq!(
                expectation(scan).map(Decision::is_continue),
                Ok(known == Known::Expect)
            );
        }
    }
}

#[test]
fn waits_for_complete_line() {
    for (known, raw) in [
        (Known::Connection, b" , close, keep-alive, \r\n".as_slice()),
        (Known::Expect, b" , 100-continue, \r\n".as_slice()),
        (Known::AcceptEncoding, b" , gzip, br, \r\n".as_slice()),
    ] {
        for end in 0..raw.len() {
            let mut scan = Fields::default();
            let mut flags = Flags::default();
            assert!(
                (known.scan_line(&mut scan, &mut flags, &raw[..end], Version::Eleven))
                    == (Ok(None)),
                "field {raw:?} at {end}"
            );
            assert!((flags) == (Flags::default()));
            assert!((scan.message.coding()) == (Coding::Plain));
            assert!(!scan.routing.connection().is_close());
            assert!(!scan.routing.connection().is_keep());
            assert_eq!(expectation(scan), Ok(Decision::None));
        }
    }
}

#[test]
fn rejects_bad_line_endings() {
    for (known, value) in [
        (Known::Connection, b" , close, keep-alive, ".as_slice()),
        (Known::Expect, b" , 100-continue, ".as_slice()),
        (Known::AcceptEncoding, b" , gzip, br, ".as_slice()),
    ] {
        for suffix in [b"\n".as_slice(), b"\rX".as_slice(), b"\r\r\n".as_slice()] {
            let raw = [value, suffix].concat();
            let mut scan = Fields::default();
            let mut flags = Flags::default();
            assert!(
                (known.scan_line(&mut scan, &mut flags, &raw, Version::Eleven))
                    == (Err(head::Error::Value)),
                "field {raw:?}"
            );
            assert!((flags) == (Flags::default()));
            assert!((scan.message.coding()) == (Coding::Plain));
            assert!(!scan.routing.connection().is_close());
            assert!(!scan.routing.connection().is_keep());
            assert_eq!(expectation(scan), Ok(Decision::None));
        }
    }
}

#[test]
fn retains_empty_list() {
    for known in [Known::Connection, Known::Expect, Known::AcceptEncoding] {
        for (raw, start, end) in [
            (b"\r\n".as_slice(), 0, 0),
            (b" \t,, ,\t \r\n".as_slice(), 2, 6),
        ] {
            let mut scan = Fields::default();
            let mut flags = Flags::default();
            assert!(
                (known
                    .scan_line(&mut scan, &mut flags, raw, Version::Eleven)
                    .map(|bounds| {
                        bounds.map(|bounds| (bounds.tail(), bounds.start(), bounds.end()))
                    }))
                    == (Ok(Some((raw.len() - 2, start, end)))),
                "field {raw:?}"
            );
            assert!((flags) == (Flags::default()));
            assert!((scan.message.coding()) == (Coding::Plain));
            assert!(!scan.routing.connection().is_close());
            assert!(!scan.routing.connection().is_keep());
            assert_eq!(expectation(scan), Ok(Decision::None));
        }
    }
}

#[test]
fn retains_connection_error_offset() {
    let mut scan = Fields::default();
    let mut flags = Flags::default();
    let expected = Some(head::Error::Connection(field::TokenError {
        index: 5,
        byte: b'/',
    }));
    assert_eq!(
        Known::Connection
            .scan_line(
                &mut scan,
                &mut flags,
                INVALID_CONNECTION_LINE,
                Version::Eleven
            )
            .err(),
        expected
    );
    let value = &INVALID_CONNECTION_LINE[2..INVALID_CONNECTION_LINE.len() - 4];
    assert_eq!(
        Known::Connection
            .apply(&mut scan, &mut flags, value, Version::Eleven)
            .err(),
        expected
    );
}

#[test]
fn allocates_nothing() {
    for (known, raw) in [
        (Known::Connection, b" , close, keep-alive, \r\n".as_slice()),
        (Known::Expect, b" , 100-continue, \r\n".as_slice()),
        (Known::AcceptEncoding, b" , gzip, br, \r\n".as_slice()),
    ] {
        let mut scan = Fields::default();
        let mut flags = Flags::default();
        let (result, activity) =
            Tracker::measure(|| known.scan_line(&mut scan, &mut flags, raw, Version::Eleven));
        result.expect("field syntax").expect("complete field");
        assert_eq!(activity, [0; 4]);
    }
}
