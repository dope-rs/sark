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
    },
    payload::response::Method,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn empty() {
    let headers: &[Entry<'_>] = &[];
    let scan = (Fields::try_scan(headers)).expect("valid request fields");
    assert_eq!(scan.message.length(), None);
    assert!(!scan.message.has_coding());
    assert!(!scan.message.is_chunked());
    assert!(matches!(scan.try_body(), Ok(RequestBody::Empty)));
}

#[test]
fn length() {
    let headers = &[Entry::new(b"Content-Length", b"42")];
    let scan = (Fields::try_scan(headers)).expect("valid request fields");
    assert_eq!(scan.message.length(), Some(42));
}

#[test]
fn duplicate_length() {
    let headers = &[
        Entry::new(b"Content-Length", b"42"),
        Entry::new(b"Content-Length", b"42"),
    ];
    assert!(Fields::try_scan(headers).is_err());
}

#[test]
fn conflicting_lengths() {
    let headers = &[
        Entry::new(b"Content-Length", b"42"),
        Entry::new(b"Content-Length", b"99"),
    ];
    assert!(Fields::try_scan(headers).is_err());
}

#[test]
fn chunked() {
    let headers = &[Entry::new(b"Transfer-Encoding", b"chunked")];
    let scan = (Fields::try_scan(headers)).expect("valid request fields");
    assert!(scan.message.has_coding());
    assert!(scan.message.is_chunked());
}

#[test]
fn non_chunked() {
    let headers = &[Entry::new(b"Transfer-Encoding", b"gzip")];
    let scan = (Fields::try_scan(headers)).expect("valid request fields");
    assert!(scan.message.has_coding());
    assert!(!scan.message.is_chunked());
}

#[test]
fn continue_expectation() {
    let headers = &[Entry::new(b"Expect", b"100-continue")];
    let scan = (Fields::try_scan(headers)).expect("valid request fields");
    assert!(matches!(scan.try_body(), Ok(RequestBody::Empty)));
}

#[test]
fn other_expectation() {
    let headers = &[Entry::new(b"Expect", b"some-other")];
    let scan = (Fields::try_scan(headers)).expect("valid request fields");
    assert!(scan.try_body().is_err());
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
        assert!(Fields::try_scan(headers).is_err(), "accepted {value:?}");
    }
}

#[test]
fn trimmed_length() {
    let headers = &[Entry::new(b"Content-Length", b"  42  ")];
    let scan = (Fields::try_scan(headers)).expect("valid request fields");
    assert_eq!(scan.message.length(), Some(42));
}

#[test]
fn empty_name() {
    let headers = &[
        Entry::new(b"", b"ignored"),
        Entry::new(b"Content-Length", b"5"),
    ];
    let scan = (Fields::try_scan(headers)).expect("valid request fields");
    assert_eq!(scan.message.length(), Some(5));
}

#[test]
fn case_insensitive() {
    let headers = &[
        Entry::new(b"TRANSFER-ENCODING", b"CHUNKED"),
        Entry::new(b"EXPECT", b"100-Continue"),
    ];
    let fields = (Fields::try_scan(headers)).expect("valid request fields");
    assert!(fields.message.has_coding());
    assert!(fields.message.is_chunked());
    assert!(matches!(fields.try_body(), Ok(RequestBody::Chunked)));

    let headers = &[Entry::new(b"CONTENT-LENGTH", b"42")];
    let fields = (Fields::try_scan(headers)).expect("valid request fields");
    assert_eq!(fields.message.length(), Some(42));
}

#[test]
fn zero_length() {
    let headers = &[Entry::new(b"Content-Length", b"0")];
    let scan = (Fields::try_scan(headers)).expect("valid request fields");
    assert_eq!(scan.message.length(), Some(0));
}

#[test]
fn leading_zeros() {
    let headers = &[Entry::new(b"Content-Length", b"0042")];
    let scan = (Fields::try_scan(headers)).expect("valid request fields");
    assert_eq!(scan.message.length(), Some(42));
}

#[test]
fn other_field() {
    let headers = &[
        Entry::new(b"X-Custom", b"123"),
        Entry::new(b"Content-Length", b"77"),
    ];
    let scan = (Fields::try_scan(headers)).expect("valid request fields");
    assert_eq!(scan.message.length(), Some(77));
}

#[test]
fn rejects_nonfinal_chunked() {
    assert!(Fields::try_headers(&[(Known::TransferEncoding, b"chunked, gzip")]).is_err());
}

#[test]
fn accepts_final_chunked() {
    assert!(
        ((Fields::try_headers(&[(Known::TransferEncoding, b"gzip, chunked")]))
            .expect("test operation must succeed"))
            == (RequestBody::Chunked)
    );
}

#[test]
fn rejects_unknown_coding() {
    assert!(Fields::try_headers(&[(Known::TransferEncoding, b"gzip")]).is_err());
}

#[test]
fn te_bare_chunked_accepted() {
    assert!(
        ((Fields::try_headers(&[(Known::TransferEncoding, b"chunked")]))
            .expect("test operation must succeed"))
            == (RequestBody::Chunked)
    );
}

#[test]
fn rejects_double_chunked() {
    assert!(Fields::try_headers(&[(Known::TransferEncoding, b"chunked, chunked")]).is_err());
}

#[test]
fn rejects_conflicting_len() {
    assert!(
        Fields::try_headers(&[(Known::ContentLength, b"5"), (Known::ContentLength, b"7"),])
            .is_err()
    );
}

#[test]
fn rejects_duplicate_len() {
    assert!(
        Fields::try_headers(&[(Known::ContentLength, b"5"), (Known::ContentLength, b"5"),])
            .is_err()
    );
}

#[test]
fn accepts_single_length() {
    assert!(
        ((Fields::try_headers(&[(Known::ContentLength, b"5")]))
            .expect("test operation must succeed"))
            == (RequestBody::Fixed(5))
    );
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
fn accepts_split_transfer() {
    assert!(
        ((Fields::try_headers(&[
            (Known::TransferEncoding, b"gzip"),
            (Known::TransferEncoding, b"chunked"),
        ]))
        .expect("test operation must succeed"))
            == (RequestBody::Chunked)
    );
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
    assert!(Fields::try_upgrade(&[(Known::Connection, b"Upgrade")]).is_err());
    assert!(Fields::try_upgrade(&[(Known::Upgrade, b"websocket")]).is_err());
    (Fields::try_upgrade(&[
        (Known::Connection, b"Upgrade"),
        (Known::Upgrade, b"websocket"),
    ]))
    .as_ref()
    .expect("test operation must succeed");
}

#[test]
fn rejects_bad_authority() {
    assert!(Fields::try_hosts(b"GET", b"/", &[]).is_err());
    assert!(Fields::try_hosts(b"GET", b"/", &[b"one", b"two"]).is_err());
    assert!(Fields::try_hosts(b"GET", b"/", &[b"user@origin"]).is_err());
    assert!(Fields::try_hosts(b"GET", b"/", &[b"origin:http"]).is_err());
}

#[test]
fn uses_target_authority() {
    assert!(
        ((Fields::try_hosts(b"GET", b"https://origin.example/items", &[b""],))
            .expect("test operation must succeed"))
            == (RequestBody::Empty)
    );
    assert!(
        ((Fields::try_hosts(b"OPTIONS", b"*", &[b"origin.example"]))
            .expect("test operation must succeed"))
            == (RequestBody::Empty)
    );
    assert!(Fields::try_hosts(b"GET", b"*", &[b"origin.example"]).is_err());
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
