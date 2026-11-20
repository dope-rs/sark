pub mod common;

use common::framing::*;
use dope_test::alloc::Tracker;
use sark_http::{
    field::{
        head::{self, Flags, headers::Known},
        metadata::map::record::Entry,
        text::IntError,
    },
    h1::{
        codec::decode::response::Decoder,
        framing::RequestBody,
        head::{
            line::Version,
            request::{Coding, Fields},
        },
        target::Policy::Require,
    },
    payload::response::Method,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn empty() {
    let headers: &[Entry<'_>] = &[];
    let scan = try_scan(headers).expect("valid request fields");
    assert_eq!(scan.message.length(), None);
    assert!(!scan.message.has_coding());
    assert!(!scan.message.is_chunked());
    assert_eq!(try_body(scan, Require), Ok(RequestBody::Empty));
}

#[test]
fn length() {
    for (value, length) in [
        (b"42".as_slice(), 42),
        (b"  42  ", 42),
        (b"0", 0),
        (b"0042", 42),
    ] {
        let headers = &[Entry::new(b"Content-Length", value)];
        let scan = try_scan(headers).expect("valid request fields");
        assert_eq!(scan.message.length(), Some(length));
    }
}

#[test]
fn duplicate_length() {
    for value in [b"42", b"99"] {
        let headers = &[
            Entry::new(b"Content-Length", b"42"),
            Entry::new(b"Content-Length", value),
        ];
        assert!(try_scan(headers).is_err());
    }
}

#[test]
fn transfer_coding() {
    for (value, chunked) in [(b"chunked".as_slice(), true), (b"gzip", false)] {
        let headers = [Entry::new(b"Transfer-Encoding", value)];
        let scan = try_scan(&headers).expect("valid request fields");
        assert!(scan.message.has_coding());
        assert_eq!(scan.message.is_chunked(), chunked);
    }
}

#[test]
fn continue_expectation() {
    let headers = &[Entry::new(b"Expect", b"100-continue")];
    let scan = try_scan(headers).expect("valid request fields");
    assert_eq!(try_body(scan, Require), Ok(RequestBody::Empty));
}

#[test]
fn other_expectation() {
    let headers = &[Entry::new(b"Expect", b"some-other")];
    let scan = try_scan(headers).expect("valid request fields");
    assert!(try_body(scan, Require).is_err());
}

#[test]
fn invalid_lengths() {
    for value in [
        &[0xff, 0xfe][..],
        b"abc",
        b"-5",
        b"99999999999999999999999999999",
        b"",
        b"   ",
    ] {
        let headers = &[Entry::new(b"Content-Length", value)];
        assert!(try_scan(headers).is_err(), "accepted {value:?}");
    }
}

#[test]
fn empty_name() {
    let headers = &[
        Entry::new(b"", b"ignored"),
        Entry::new(b"Content-Length", b"5"),
    ];
    let scan = try_scan(headers).expect("valid request fields");
    assert_eq!(scan.message.length(), Some(5));
}

#[test]
fn case_insensitive() {
    let headers = &[
        Entry::new(b"TRANSFER-ENCODING", b"CHUNKED"),
        Entry::new(b"EXPECT", b"100-Continue"),
    ];
    let fields = try_scan(headers).expect("valid request fields");
    assert!(fields.message.has_coding());
    assert!(fields.message.is_chunked());
    assert_eq!(try_body(fields, Require), Ok(RequestBody::Chunked));

    let headers = &[Entry::new(b"CONTENT-LENGTH", b"42")];
    let fields = try_scan(headers).expect("valid request fields");
    assert_eq!(fields.message.length(), Some(42));
}

#[test]
fn other_field() {
    let headers = &[
        Entry::new(b"X-Custom", b"123"),
        Entry::new(b"Content-Length", b"77"),
    ];
    let scan = try_scan(headers).expect("valid request fields");
    assert_eq!(scan.message.length(), Some(77));
}

#[test]
fn transfer_encoding_framing() {
    use sark_http::h1::{field::value, framing, head::Semantic};

    let encoding = Known::TransferEncoding;
    let chunked = (encoding, b"chunked".as_slice());
    let gzip = (encoding, b"gzip".as_slice());
    let combined = (encoding, b"gzip, chunked".as_slice());
    for headers in [&[chunked][..], &[combined], &[gzip, chunked]] {
        assert_eq!(try_headers(headers), Ok(RequestBody::Chunked));
    }
    let order = head::Error::TransferEncoding(value::Error::InvalidOrdering);
    for invalid in [b"chunked, gzip".as_slice(), b"chunked, chunked"] {
        assert_eq!(
            try_headers(&[(encoding, invalid)]),
            Err(Failure::Field(order))
        );
    }
    let framing = framing::RequestError::Framing(framing::Error::UnframedRequestTransferEncoding);
    assert_eq!(
        try_headers(&[gzip]),
        Err(Failure::Semantic(Semantic::Admission(framing)))
    );
}

#[test]
fn content_length_framing() {
    use sark_http::h1::field;

    let length = (Known::ContentLength, b"5".as_slice());
    assert_eq!(try_headers(&[length]), Ok(RequestBody::Fixed(5)));
    let duplicate = head::Error::Message(field::Error::DuplicateContentLength);
    for value in [b"7", b"5"] {
        assert_eq!(
            try_headers(&[length, (Known::ContentLength, value)]),
            Err(Failure::Field(duplicate))
        );
    }
}

#[test]
fn length_completes_once() {
    for &(raw, start, end, tail) in LENGTH_LINES {
        let mut scan = Fields::default();
        let mut flags = Flags::default();
        for split in 0..raw.len() {
            assert!(matches!(
                Known::ContentLength.scan_line(
                    &mut scan,
                    &mut flags,
                    &raw[..split],
                    Version::Eleven
                ),
                Ok(None)
            ));
        }
        assert_eq!(scan.message.length(), None);
        let bounds = Known::ContentLength
            .scan_line(&mut scan, &mut flags, raw, Version::Eleven)
            .expect("valid length")
            .expect("complete length");
        assert_eq!(
            (bounds.start(), bounds.end(), bounds.tail()),
            (start, end, tail)
        );
        assert!(
            Known::ContentLength.scan_line(&mut scan, &mut flags, raw, Version::Eleven)
                == Err(head::Error::Message(
                    sark_http::h1::field::Error::DuplicateContentLength
                ))
        );
        assert_eq!(scan.message.length(), Some(123));
        assert!(flags == Flags::default());
    }
}

#[test]
fn length_error_keeps_state() {
    let mut scan = Fields::default();
    scan.message = scan.message.try_length(7).expect("prior length");
    let mut flags = Flags::default();
    for &(raw, error) in INVALID_LENGTH_LINES {
        assert!(
            Known::ContentLength.scan_line(&mut scan, &mut flags, raw, Version::Eleven)
                == Err(head::Error::ContentLength(error))
        );
        assert_eq!(scan.message.length(), Some(7));
        assert!(flags == Flags::default());
    }
}

#[test]
fn failure_is_transactional() {
    let mut scan = Fields::default();
    let mut flags = Flags::default();
    assert!(
        Known::ContentLength.apply(&mut scan, &mut flags, b"x", Version::Eleven)
            == Err(head::Error::ContentLength(IntError::InvalidByte))
    );
    assert_eq!(scan.message.length(), None);
    assert!((flags) == (Flags::default()));

    assert!(
        Known::Connection
            .apply(&mut scan, &mut flags, b"close,\0", Version::Eleven)
            .is_err()
    );
    assert!(!scan.routing.connection().is_close());
    assert!((flags) == (Flags::default()));

    Known::AcceptEncoding
        .apply(&mut scan, &mut flags, ACCEPT_GZIP, Version::Eleven)
        .expect("gzip");
    let prior = flags;
    assert!(
        Known::AcceptEncoding.scan_line(&mut scan, &mut flags, INVALID_GZIP_LINE, Version::Eleven)
            == Err(head::Error::Value)
    );
    assert!(scan.message.coding() == Coding::Gzip);
    assert!(flags == prior);
}

#[test]
fn rejects_cl_te_second() {
    for headers in [
        [
            (Known::ContentLength, b"5".as_slice()),
            (Known::TransferEncoding, b"chunked".as_slice()),
        ],
        [
            (Known::TransferEncoding, b"chunked".as_slice()),
            (Known::ContentLength, b"5".as_slice()),
        ],
    ] {
        let mut scan = Fields::default();
        let mut flags = Flags::default();
        (headers[0]
            .0
            .apply(&mut scan, &mut flags, headers[0].1, Version::Eleven))
        .expect("test operation must succeed");
        assert!(
            headers[1]
                .0
                .apply(&mut scan, &mut flags, headers[1].1, Version::Eleven)
                .is_err()
        );
    }
}

#[test]
fn accumulates_connection() {
    for (lines, keep, close) in [
        (
            &[b" Upgrade\r\n".as_slice(), b" keep-alive, close\r\n"][..],
            true,
            true,
        ),
        (
            &[b" Upgrade\r\n".as_slice(), b"keep-alive\r\n"][..],
            true,
            false,
        ),
        (
            &[b" Upgrade\r\n".as_slice(), b" ClOsE\r\n"][..],
            false,
            true,
        ),
    ] {
        let mut scan = Fields::default();
        let mut flags = Flags::default();
        for line in lines {
            assert!(
                (Known::Connection.scan_line(&mut scan, &mut flags, line, Version::Eleven))
                    .expect("test operation must succeed")
                    .is_some()
            );
        }

        let state = scan.routing.connection();
        assert!(state.is_upgrade());
        assert_eq!(state.is_keep(), keep);
        assert_eq!(state.is_close(), close);
    }
}

#[test]
fn admits_generated_upgrade() {
    use sark_http::h1::{head::Semantic, switch};

    let host = (Known::Host, b"origin.example".as_slice());
    let connection = (Known::Connection, b"Upgrade".as_slice());
    let upgrade = (Known::Upgrade, b"websocket".as_slice());
    let incomplete = Semantic::Transition(switch::Error::IncompleteUpgradeRequest);
    for field in [connection, upgrade] {
        assert_eq!(
            try_request(b"GET", b"/", &[host, field]),
            Err(Failure::Semantic(incomplete))
        );
    }
    assert_eq!(
        try_request(b"GET", b"/", &[host, connection, upgrade]),
        Ok(RequestBody::Empty)
    );
}

#[test]
fn rejects_bad_authority() {
    use Known::Host;
    use sark_http::{
        h1::{
            framing::RequestError,
            head::Semantic,
            target::{Error::MissingHost, HostError},
        },
        uri,
    };

    let missing = Semantic::Admission(RequestError::Authority(MissingHost));
    assert_eq!(
        try_request(b"GET", b"/", &[]),
        Err(Failure::Semantic(missing))
    );
    let duplicate = [(Host, b"one".as_slice()), (Host, b"two")];
    let user = [(Host, b"user@origin".as_slice())];
    let port = [(Host, b"origin:http".as_slice())];
    for (headers, cause) in [
        (duplicate.as_slice(), HostError::Duplicate),
        (&user, HostError::Authority(uri::Error::InvalidHost)),
        (&port, HostError::Authority(uri::Error::InvalidPort)),
    ] {
        assert_eq!(
            try_request(b"GET", b"/", headers),
            Err(Failure::Field(head::Error::Host(cause)))
        );
    }
}

#[test]
fn uses_target_authority() {
    use sark_http::h1::{head::Semantic, target};

    let absolute = b"https://origin.example/items";
    let empty = [(Known::Host, b"".as_slice())];
    let origin = [(Known::Host, b"origin.example".as_slice())];
    for (method, target, headers) in [
        (b"GET".as_slice(), absolute.as_slice(), &empty),
        (b"OPTIONS", b"*", &origin),
    ] {
        assert_eq!(try_request(method, target, headers), Ok(RequestBody::Empty));
    }
    let invalid = Semantic::Authority(target::Error::InvalidTarget);
    assert_eq!(
        try_request(b"GET", b"*", &origin),
        Err(Failure::Semantic(invalid))
    );
}

#[test]
fn rejects_h1_trailers() {
    let raw: &[u8] = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
0\r\nContent-Length: 999\r\nHost: evil\r\nConnection: close\r\nX-Good: yes\r\n\r\n";
    assert!(Decoder::new(Method::Other).decode(raw).is_err());
}

#[test]
fn authority_has_no_alloc() {
    use sark_http::{
        field::head::{Flags, headers::Known},
        h1::{framing::RequestBody, head::request::Fields, target},
    };

    let (frame, alloc) = Tracker::count(|| {
        let mut scan = Fields::default();
        let mut flags = Flags::default();
        let result = Known::Host.apply(&mut scan, &mut flags, AUTHORITY, Version::Eleven);
        let () = result.expect("one valid Host field must apply");
        let result = target::Target::parse(b"GET", AUTHORITY_TARGET);
        let target = result.expect("origin-form target must parse");
        let result = scan.admit_target(target, b"HTTP/1.1", target::Policy::Require);
        let target = result.expect("Host must satisfy required authority");
        target.body()
    });

    assert!((frame) == (RequestBody::Empty));
    assert_eq!(alloc, 0);
}
