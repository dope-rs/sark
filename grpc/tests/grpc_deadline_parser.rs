pub mod common;

use std::time::Duration;

use common::headers;
use sark_grpc::{
    Code, Status,
    details::Diagnostic,
    protocol::{request, request::timeout::Error, response},
};
use sark_http::field::metadata::map::Owned;

#[test]
fn units() {
    let fields = headers::try_request([]).expect("request fields");
    assert_eq!(
        request::Head::from_fields(&fields)
            .expect("absent timeout")
            .timeout(),
        None
    );
    for (value, duration) in [
        (b"1H".as_slice(), Duration::from_secs(3_600)),
        (b"2M", Duration::from_secs(120)),
        (b"3S", Duration::from_secs(3)),
        (b"4m", Duration::from_millis(4)),
        (b"5u", Duration::from_micros(5)),
        (b"6n", Duration::from_nanos(6)),
        (b"00000000n", Duration::ZERO),
        (b"00000001S", Duration::from_secs(1)),
        (b"99999999H", Duration::from_secs(359_999_996_400)),
    ] {
        let fields = headers::try_request([
            Owned::try_new(b"grpc-timeout", value).expect("timeout field"),
            Owned::try_new(b"x-kept", b"yes").expect("metadata field"),
        ])
        .expect("request fields");
        let head = request::Head::from_fields(&fields).expect("valid timeout");
        assert_eq!(head.timeout(), Some(duration));
        assert_eq!(head.metadata().values(b"grpc-timeout").next(), None);
        assert_eq!(
            head.metadata().values(b"x-kept").next(),
            Some(b"yes".as_slice())
        );
    }
    let first = headers::try_request([Owned::try_new(b"grpc-timeout", b"1S").expect("timeout")])
        .expect("first request");
    let second = headers::try_request([Owned::try_new(b"grpc-timeout", b"2S").expect("timeout")])
        .expect("second request");
    assert!(
        request::Head::from_fields(&first).expect("first head")
            != request::Head::from_fields(&second).expect("second head")
    );
}

#[test]
fn invalid() {
    for (value, expected) in [
        (b"".as_slice(), Error::Length { actual: 0 }),
        (b"S", Error::Length { actual: 1 }),
        (b"123456789S", Error::Length { actual: 10 }),
        (
            b"xS",
            Error::Digit {
                index: 0,
                byte: b'x',
            },
        ),
        (b"1s", Error::Unit { byte: b's' }),
    ] {
        let fields = headers::try_request([Owned::try_new(b"grpc-timeout", value).expect("field")])
            .expect("request fields");
        let status = request::Head::from_fields(&fields)
            .err()
            .expect("invalid timeout");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(
            match (Diagnostic::from(&status), expected) {
                (
                    Diagnostic::Timeout(Error::Length { actual }),
                    Error::Length { actual: expected },
                ) => {
                    *actual == expected
                }
                (
                    Diagnostic::Timeout(Error::Digit { index, byte }),
                    Error::Digit {
                        index: wantidx,
                        byte: wantbyte,
                    },
                ) => (*index, *byte) == (wantidx, wantbyte),
                (Diagnostic::Timeout(Error::Unit { byte }), Error::Unit { byte: expected }) => {
                    *byte == expected
                }
                _ => false,
            },
            "wrong timeout diagnostic"
        );
    }
    let fields = headers::try_request([
        Owned::try_new(b"grpc-timeout", b"1S").expect("first timeout"),
        Owned::try_new(b"grpc-timeout", b"2S").expect("second timeout"),
    ])
    .expect("request fields");
    let status = request::Head::from_fields(&fields)
        .err()
        .expect("duplicate timeout");
    assert!(matches!(
        Diagnostic::from(&status),
        Diagnostic::Timeout(Error::Duplicate)
    ));
}

#[test]
fn reserved() {
    let fields = headers::try_response([Owned::try_new(b"grpc-timeout", b"bad").expect("field")])
        .expect("response fields");
    let head = response::Head::from_fields(&fields).expect("ignored response timeout");
    assert_eq!(head.metadata().values(b"grpc-timeout").next(), None);
    let fields = headers::try_trailers([Owned::try_new(b"grpc-timeout", b"bad").expect("field")])
        .expect("trailer fields");
    assert_eq!(
        Status::from_fields(&fields)
            .expect("ignored trailer timeout")
            .code(),
        Code::Ok
    );
}
