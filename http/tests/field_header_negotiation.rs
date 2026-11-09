pub mod common;

use sark_http::{
    field::head::{self, Flags, headers::Known},
    h1::head::{
        line::Version,
        request::{Coding, Fields},
    },
};

#[test]
fn rejects_literal_controls() {
    for (known, raw) in [
        (Known::Connection, b"keep\ralive\r\n".as_slice()),
        (Known::Expect, b"100\rcontinue\r\n".as_slice()),
        (Known::Expect, b"\x11\x10\x10-continue\r\n".as_slice()),
    ] {
        assert!(
            known
                .scan_line(
                    &mut Fields::default(),
                    &mut Flags::default(),
                    raw,
                    Version::Eleven
                )
                .is_err()
        );
    }
}

#[test]
fn admits_gzip_quality() {
    for (raw, gzip) in [
        (b"gzip, br".as_slice(), true),
        (b"br".as_slice(), false),
        (b"gzip;q=1.0, deflate;q=0.5".as_slice(), true),
        (b"gzip;q=0".as_slice(), false),
        (b"gzip; Q=0.000".as_slice(), false),
        (b"gzip;q=0.001".as_slice(), true),
        (b"gzip;q=1.000".as_slice(), true),
        (b"*;q=0.5".as_slice(), true),
        (b"gzip;q=0, *;q=1".as_slice(), false),
        (b"*;q=0, gzip;q=1".as_slice(), true),
    ] {
        let mut fields = Fields::default();
        Known::AcceptEncoding
            .apply(&mut fields, &mut Flags::default(), raw, Version::Eleven)
            .expect("quality");
        assert_eq!(fields.message.coding() == Coding::Gzip, gzip);
        let line = [raw, b"\r\n"].concat();
        let mut fields = Fields::default();
        Known::AcceptEncoding
            .scan_line(&mut fields, &mut Flags::default(), &line, Version::Eleven)
            .expect("line")
            .expect("complete");
        assert_eq!(fields.message.coding() == Coding::Gzip, gzip);
    }
}

#[test]
fn retains_line_exclusions() {
    for values in [
        [b"gzip;q=0".as_slice(), b"*;q=1", b"gzip;q=1"],
        [b"*;q=1".as_slice(), b"gzip;q=1", b"gzip;q=0"],
    ] {
        let mut fields = Fields::default();
        let mut flags = Flags::default();
        for value in values {
            Known::AcceptEncoding
                .apply(&mut fields, &mut flags, value, Version::Eleven)
                .expect("field");
        }
        assert!(fields.message.coding() == Coding::Plain);
    }
}

#[test]
fn retains_encoding_quality() {
    let mut encoding = head::AcceptEncoding::default();
    for (raw, expected) in common::framing::ENCODING_QUALITY {
        encoding.append(head::AcceptEncoding::try_parse(raw).expect("encoding"));
        assert_eq!(
            (encoding.br(), encoding.gzip(), encoding.identity()),
            expected
        );
    }
}

#[test]
fn rejects_invalid_values() {
    for raw in [
        b"gzip;\0".as_slice(),
        b"gzip;q=1.001".as_slice(),
        b"gzip;q=0.0001".as_slice(),
        b"gzip;q=2".as_slice(),
        b"gzip;\x7f".as_slice(),
    ] {
        let mut fields = Fields::default();
        let mut flags = Flags::default();
        let line = [raw, b"\r\n"].concat();
        assert!(
            Known::AcceptEncoding.scan_line(&mut fields, &mut flags, &line, Version::Eleven)
                == Err(head::Error::Value)
        );
        assert!(flags == Flags::default());
        assert!(fields.message.coding() == Coding::Plain);
    }
}
