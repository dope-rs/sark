pub mod common;

use common::{
    framing::{EXPECT_VERSIONS, NO_AUTHORITY},
    head::{
        Capture,
        budget::{LIMIT, REQUEST, RESPONSE},
    },
    wire::version,
};
use sark_http::{
    h1::{
        self,
        codec::decode::{request::Head, response::Decoder},
        field::{self, expect::Decision},
        framing::{self, ResponseBody},
        head::{
            Boundary, Limit, ParseError, Semantic,
            request::{self, scan},
            response,
        },
        target,
    },
    payload::response::{Method, status::Code},
};

#[test]
fn authority() {
    let cause = ParseError::Semantic(Semantic::Admission(framing::RequestError::Authority(
        target::Error::EmptyAuthority,
    )));
    assert!(
        matches!(request::Parts::scan(NO_AUTHORITY, Limit::DEFAULT), Err(error) if error == cause)
    );
    let state = scan::State::new(NO_AUTHORITY, Limit::DEFAULT);
    assert!(
        matches!(state.advance(|_| Ok::<_, core::convert::Infallible>(())),
        Err((wire, error)) if core::ptr::eq(wire, NO_AUTHORITY) && error == cause)
    );
    let target = target::Target::parse(b"GET", b"/").expect("target");
    let admit = request::Fields::default()
        .admit_target(target, b"HTTP/1.0", target::Policy::AllowEmpty)
        .expect("optional authority");
    assert!(matches!(admit.authority().source(), target::Source::Empty));
}

#[test]
fn head_lends_fields() {
    let request = b"GET /socket HTTP/1.1\r\nHost: origin.example\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nContent-Length: 0\r\n\r\nframe";
    let head = (Head::parse(request, request.len(), Capture::default()))
        .expect("complete request must parse")
        .expect("complete request must parse");

    assert_eq!(head.method, b"GET");
    assert_eq!(head.target, b"/socket");
    assert_eq!(head.version, 1);
    assert!((head.admission.body()) == (h1::framing::RequestBody::Empty));
    assert!(head.connection.is_upgrade());
    assert_eq!(&request[head.header_len.get() as usize..], b"frame");
    assert_eq!(head.target.as_ptr(), request[4..].as_ptr());
    assert_eq!(
        head.fields.fields,
        [
            (b"Host".as_slice(), b"origin.example".as_slice()),
            (b"Connection".as_slice(), b"Upgrade".as_slice()),
            (b"Upgrade".as_slice(), b"websocket".as_slice()),
            (b"Content-Length".as_slice(), b"0".as_slice()),
        ]
    );
}

#[test]
fn bad_head_is_rejected() {
    for request in [
        b"GET / HTTP/1.1\r\nConnection: Upgrade\r\n\r\n".as_slice(),
        b"GET / HTTP/1.1\r\nHost: one\r\nHost: two\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: x\r\nX-Bad: safe\0shadow\r\n\r\n",
    ] {
        assert!(Head::scan(request, request.len()).is_err());
    }
}

#[test]
fn partial_is_incomplete() {
    let request = b"GET / HTTP/1.1\r\nHost: x\r\n";
    assert!(matches!(Head::scan(request, request.len()), Ok(None)));
}

#[test]
fn expectation_version() {
    for &(request, expected) in EXPECT_VERSIONS {
        let head = Head::scan(request, request.len())
            .expect("request head")
            .expect("complete head");
        assert_eq!(
            matches!(head.admission.expectation(), Decision::Continue),
            expected,
        );
    }
}

#[test]
fn bound_ends_at_head() {
    let request = b"GET / HTTP/1.1\r\nHost: origin.example\r\n\r\nframe";
    let head_len = (request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| offset + 4))
    .expect("request fixture must contain a complete head");

    assert!(matches!(Head::scan(request, head_len), Ok(Some(_))));
    assert!(Head::scan(request, head_len - 1).is_err());
}

#[test]
fn lends_fields() {
    let response = b"HTTP/1.1 200 Any reason\r\nX-First: one\r\nContent-Length: 0\r\n\r\nnext";
    {
        let head =
            (Decoder::new(Method::Other).parse(response, response.len(), Capture::default()))
                .expect("response must decode")
                .expect("test input must contain a complete head");
        assert_eq!(head.version, 1);
        assert!(head.status == Code::OK);
        assert!(matches!(head.payload, ResponseBody::Fixed(0)));
        assert_eq!(&response[head.header_len.get() as usize..], b"next");
        assert_eq!(
            head.fields.fields,
            [
                (b"X-First".as_slice(), b"one".as_slice()),
                (b"Content-Length".as_slice(), b"0".as_slice()),
            ]
        );
    };
}

#[test]
fn shares_framing() {
    let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 1\r\n\r\n";

    assert!(
        Decoder::new(Method::Other)
            .parse(response, response.len(), Capture::default())
            .is_err()
    );
    assert!(Decoder::new(Method::Other).collect(response).is_err());
}

#[test]
fn rejects_malformed() {
    let nul = b"HTTP/1.1 101 Switching Protocols\r\nX-Probe: safe\0shadow\r\n\r\n";
    assert!(
        Decoder::new(Method::Other)
            .parse(nul, nul.len(), Capture::default())
            .is_err()
    );

    let obs_text = b"HTTP/1.1 101 opaque\r\nX-Probe: \xff\r\n\r\n";
    assert!(matches!(
        Decoder::new(Method::Other).parse(obs_text, obs_text.len(), Capture::default(),),
        Ok(Some(_))
    ));
}

#[test]
fn emits_control() {
    let h11 = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nKeep-Alive: timeout=30\r\nKeep-Alive: timeout=5, max=10\r\nKeep-Alive: timeout=invalid, timeout=18446744073709551616\r\n\r\n";
    {
        let head = (Decoder::new(Method::Other).scan(h11, h11.len()))
            .expect("response must decode")
            .expect("test input must contain a complete head");
        assert!(head.control.is_reusable());
        assert_eq!(head.control.timeout(), Some(5));
    };

    let max =
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nKeep-Alive: timeout=18446744073709551615\r\n\r\n";
    {
        let head = (Decoder::new(Method::Other).scan(max, max.len()))
            .expect("response must decode")
            .expect("test input must contain a complete head");
        assert_eq!(head.control.timeout(), Some(u64::MAX));
    };

    let h10 = b"HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n";
    {
        let head = (Decoder::new(Method::Other).scan(h10, h10.len()))
            .expect("response must decode")
            .expect("test input must contain a complete head");
        assert!(!head.control.is_reusable());
    };

    let keep = b"HTTP/1.0 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    {
        let head = (Decoder::new(Method::Other).scan(keep, keep.len()))
            .expect("response must decode")
            .expect("test input must contain a complete head");
        assert!(head.control.is_reusable());
    };

    let eof = b"HTTP/1.1 200 OK\r\nConnection: keep-alive\r\n\r\n";
    {
        let head = (Decoder::new(Method::Other).scan(eof, eof.len()))
            .expect("response must decode")
            .expect("test input must contain a complete head");
        assert!(!head.control.is_reusable());
    };
}

#[test]
fn rejects_bad_connection() {
    let bad = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive, bad token\r\n\r\n";
    assert!(Decoder::new(Method::Other).scan(bad, bad.len()).is_err());
}

#[test]
fn rejects_bad_upgrade() {
    let bad =
        b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: web socket\r\n\r\n";
    assert!(Decoder::new(Method::Other).scan(bad, bad.len()).is_err());
}

#[test]
fn bounds_head() {
    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let head_len = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| offset + 4)
        .expect("complete response head");

    assert!(matches!(
        Decoder::new(Method::Other).scan(response, head_len),
        Ok(Some(_))
    ));
    assert!(
        Decoder::new(Method::Other)
            .scan(response, head_len - 1)
            .is_err()
    );
}

#[test]
fn request_bounds() {
    for split in 0..=LIMIT {
        let state = scan::State::new(REQUEST[..split].to_vec(), Limit::DEFAULT.with_bytes(LIMIT));
        let mut emitted = 0;
        let mut sink = |_: field::Entry<'_>| {
            emitted += 1;
            Ok::<(), ()>(())
        };
        let mut state = match state.advance(&mut sink).expect("prefix") {
            scan::Progress::More(state) => Some(state),
            scan::Progress::Complete(_) => None,
        }
        .expect("incomplete prefix");
        state.try_extend(&REQUEST[split..]).expect("append");
        assert!(matches!(state.advance(&mut sink),
            Err((_, ParseError::Boundary(Boundary::TooLarge { actual, limit })))
                if actual == REQUEST.len() && limit == LIMIT));
        assert_eq!(emitted, 1);
    }
}

#[test]
fn response_bounds() {
    for split in 0..=LIMIT {
        response::State::with(
            RESPONSE[..split].to_vec(),
            Limit::RESPONSE.with_bytes(LIMIT),
            |mut source, progress| {
                let mut emitted = 0;
                let mut sink = |_: field::Entry<'_>| {
                    emitted += 1;
                    Ok::<(), ()>(())
                };
                let (progress, head) = progress.advance(&source, &mut sink).expect("prefix");
                assert!(head.is_none());
                source.try_extend(&RESPONSE[split..]).expect("append");
                assert!(matches!(progress.advance(&source, &mut sink),
                    Err(ParseError::Boundary(Boundary::TooLarge { actual, limit }))
                        if actual == RESPONSE.len() && limit == LIMIT));
                assert_eq!(emitted, 1);
            },
        );
    }
}

#[test]
fn ends_head_205() {
    let response = response::Parts::scan(version::HEAD_205, Limit::RESPONSE)
        .expect("valid response head")
        .expect("complete response head")
        .admit(Method::Head)
        .expect("bodyless HEAD response");
    assert!(response.body() == framing::ResponseBody::None);
    assert!(response.control().is_reusable());
}

#[test]
fn rejects_http10_transfer() {
    assert!(matches!(
        response::Parts::scan(version::RESPONSE_TE_10, Limit::RESPONSE),
        Err(ParseError::Semantic(Semantic::Framing(
            framing::Error::Http10TransferEncoding
        )))
    ));
    assert!(matches!(
        request::Parts::scan(version::REQUEST_TE_10, Limit::DEFAULT,),
        Err(ParseError::Semantic(Semantic::Admission(
            framing::RequestError::Framing(framing::Error::Http10TransferEncoding)
        )))
    ));
}
