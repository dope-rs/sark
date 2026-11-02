use dope_quic::{errors::Code, wire::stream::Id};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    VarInt,
    conn::{
        Connection, Error,
        event::{Event, Role},
        io::{Receive, Send},
        options::{self, plan},
        push,
    },
    frame::ErrorCode,
    link::Pump,
    qpack::encoder::Encoder,
};
use sark_http::field::{
    literal,
    metadata::{section::rules, strings::Encoding},
};

use crate::common::{
    conn::{Events, build::Fields as _},
    field::entry,
    ledger::owner::Held,
    message::promise::{Authority, Push as _},
    stream::Stream as _,
};

pub mod common;

#[test]
fn server_bidi_admission() {
    let headers = [literal!(entry(b":status", b"200"))]
        .try_headers()
        .expect("response headers");
    let mut config = options::Config::default();
    config.streams.capacity = 1;
    config.ingress.retained_bytes = headers.len();
    let mut conn = Connection::with_config(Role::Client, config).expect("connection");
    let append = Receive::new(&mut conn).borrowed(
        Id::new(1).expect("server stream"),
        &headers,
        Boundary::More,
    );
    let mut peer = Held::default();
    peer.finished.insert(5);
    let finish = Pump::new(&mut peer, &mut conn).event(Id::new(5).expect("server stream"));
    for (raw, result) in [(1, append), (5, finish)] {
        let error = result.expect_err("server-initiated bidirectional stream");
        assert_eq!(error, Error::RequestStream { stream_id: raw });
        assert_eq!(error.error_code(), ErrorCode::StreamCreation);
    }
    let reset =
        Receive::new(&mut conn).reset(Id::new(9).expect("server stream"), None, Code::from_u16(0));
    let stop =
        Receive::new(&mut conn).stop(Id::new(13).expect("server stream"), None, Code::from_u16(0));
    for (raw, result) in [(9, reset), (13, stop)] {
        let (error, charge) = result.expect_err("server-initiated bidirectional stream");
        assert_eq!(error, Error::RequestStream { stream_id: raw });
        assert_eq!(error.error_code(), ErrorCode::StreamCreation);
        assert!(charge.is_empty());
    }
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("no events")
            .is_none()
    );
    Send::new(&mut conn)
        .headers(
            Id::ZERO,
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":path", b"/asset")),
                literal!(entry(b":authority", b"example.test")),
            ],
            Boundary::End,
        )
        .expect("request method");
    Receive::new(&mut conn)
        .borrowed(Id::ZERO, &headers, Boundary::End)
        .expect("rejected streams retain no capacity or receive budget");
}

#[test]
fn push_id_retirement() {
    let headers = [literal!(entry(b":status", b"200"))]
        .try_headers()
        .expect("response headers");
    let prefix = VarInt::MAX_VALUE.try_stream(&[]).expect("push prefix");
    let promise = VarInt::MAX_VALUE
        .try_promise(&Authority.try_request().expect("promised request"))
        .expect("promise");
    for (boundary, reset) in [
        (Boundary::More, false),
        (Boundary::End, false),
        (Boundary::More, true),
    ] {
        let mut conn = Connection::with_push::<Authority>(Role::Client).expect("connection");
        conn.allow_push(Role::Client, VarInt::MAX)
            .expect("push grant");
        Receive::new(&mut conn)
            .borrowed(Id::ZERO, &promise, Boundary::More)
            .expect("promise before fragmented stream");
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("promise event"),
            Some(Event::PushPromise {
                push_id: VarInt::MAX,
                ..
            })
        ));
        let id = Id::new(3).expect("push stream");
        for fragment in prefix.chunks(1) {
            Receive::new(&mut conn)
                .borrowed(id, fragment, Boundary::More)
                .expect("fragmented push prefix");
        }
        Receive::new(&mut conn)
            .borrowed(id, &headers, boundary)
            .expect("admitted push continuation");
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("push headers"),
            Some(Event::PushHeaders {
                push_id: VarInt::MAX,
                ..
            })
        ));
        if reset {
            assert!(
                Receive::new(&mut conn)
                    .reset(id, None, Code::from_u16(0))
                    .is_ok()
            );
        }
        let error = Receive::new(&mut conn)
            .borrowed(
                Id::new(7).expect("second push stream"),
                &prefix,
                Boundary::More,
            )
            .expect_err("push id reuse");
        assert_eq!(
            error,
            Error::Push(push::Error::Duplicate {
                push_id: VarInt::MAX
            })
        );
        assert_eq!(error.error_code(), ErrorCode::Id);
    }
}

#[test]
fn push_history_capacity() {
    let headers = [literal!(entry(b":status", b"200"))]
        .try_headers()
        .expect("response headers");
    let mut config = options::Config::default();
    config.streams.capacity = 1;
    let mut conn = plan::Config::new(config)
        .expect("config")
        .build::<rules::RetainRaw, Authority>(Role::Client)
        .expect("connection");
    conn.allow_push(Role::Client, 1).expect("push grant");
    Send::new(&mut conn)
        .headers(Id::ZERO, Authority, Boundary::End)
        .expect("parent request");
    Receive::new(&mut conn)
        .borrowed(Id::ZERO, &headers, Boundary::More)
        .expect("parent response");
    Receive::new(&mut conn)
        .owned(
            Id::ZERO,
            VarInt::ZERO
                .try_promise(&Authority.try_request().expect("promised request"))
                .expect("promise"),
            Boundary::End,
        )
        .expect("parent completion");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("parent headers"),
        Some(Event::Headers { .. })
    ));
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("promise event"),
        Some(Event::PushPromise { push_id: 0, .. })
    ));
    Receive::new(&mut conn)
        .owned(
            Id::new(3).expect("push stream"),
            VarInt::ZERO.try_stream(&headers).expect("push response"),
            Boundary::End,
        )
        .expect("first push");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("push headers"),
        Some(Event::PushHeaders { push_id: 0, .. })
    ));
    let error = Receive::new(&mut conn)
        .owned(
            Id::new(7).expect("second push stream"),
            VarInt::new(1)
                .expect("push id")
                .try_stream(&[])
                .expect("push prefix"),
            Boundary::More,
        )
        .expect_err("connection push history capacity");
    assert_eq!(
        error,
        Error::Push(push::Error::Capacity {
            push_id: 1,
            capacity: 1
        })
    );
    assert_eq!(error.error_code(), ErrorCode::ExcessiveLoad);
}

#[test]
fn repeated_push_promises() {
    let promise = VarInt::ZERO
        .try_promise(&Authority.try_request().expect("promised request"))
        .expect("promise");
    let mut conn = Connection::with_push::<Authority>(Role::Client).expect("connection");
    conn.allow_push(Role::Client, 0).expect("push grant");
    for raw in [0, 4] {
        Receive::new(&mut conn)
            .borrowed(
                Id::new(raw).expect("request stream"),
                &promise,
                Boundary::More,
            )
            .expect("repeated promise");
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("promise event"),
            Some(Event::PushPromise { push_id: 0, .. })
        ));
    }
    let headers = [literal!(entry(b":status", b"200"))]
        .try_headers()
        .expect("response headers");
    Receive::new(&mut conn)
        .owned(
            Id::new(3).expect("push stream"),
            VarInt::ZERO.try_stream(&headers).expect("push response"),
            Boundary::End,
        )
        .expect("push after repeated promises");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("push headers"),
        Some(Event::PushHeaders { push_id: 0, .. })
    ));
    Receive::new(&mut conn)
        .borrowed(
            Id::new(8).expect("request stream"),
            &promise,
            Boundary::More,
        )
        .expect("promise after received push");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("promise event"),
        Some(Event::PushPromise { push_id: 0, .. })
    ));
}

#[test]
fn promise_fields_match() {
    let mut conn = Connection::with_push::<Authority>(Role::Client).expect("connection");
    conn.allow_push(Role::Client, 0).expect("push grant");
    for (raw, encoding, value) in [
        (0, Encoding::Plain, b"original".as_slice()),
        (4, Encoding::Huffman, b"original".as_slice()),
        (8, Encoding::Plain, b"changed".as_slice()),
    ] {
        let mut block = Vec::new();
        let mut encoder = Encoder::new();
        encoder.set_encoding(encoding);
        encoder
            .encode(
                raw,
                [
                    literal!(entry(b":method", b"GET")),
                    literal!(entry(b":scheme", b"https")),
                    literal!(entry(b":path", b"/asset")),
                    literal!(entry(b":authority", b"example.test")),
                    entry(b"x-tag", value).expect("field"),
                ],
                &mut block,
            )
            .expect("field section");
        let result = Receive::new(&mut conn).owned(
            Id::new(raw).expect("request stream"),
            VarInt::ZERO.try_promise(&block).expect("promise"),
            Boundary::More,
        );
        if raw == 8 {
            let error = result.expect_err("different promise fields");
            assert_eq!(error, Error::Push(push::Error::Mismatch { push_id: 0 }));
            assert_eq!(error.error_code(), ErrorCode::GeneralProtocol);
        } else {
            result.expect("same decompressed fields");
            assert!(matches!(
                Events::new(&mut conn).poll_token().expect("promise event"),
                Some(Event::PushPromise { push_id: 0, .. })
            ));
        }
    }
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("no mismatched event")
            .is_none()
    );
}

#[test]
fn discarded_promise_match() {
    let mut conn: Connection<'_, rules::Discard, Authority> = plan::Config::new(Default::default())
        .expect("plan")
        .build(Role::Client)
        .expect("connection");
    conn.allow_push(Role::Client, 0).expect("push grant");
    for (raw, encoding, value) in [
        (0, Encoding::Plain, b"original".as_slice()),
        (4, Encoding::Huffman, b"original".as_slice()),
        (8, Encoding::Huffman, b"changed".as_slice()),
    ] {
        let mut block = Vec::new();
        let mut encoder = Encoder::new();
        encoder.set_encoding(encoding);
        encoder
            .encode(
                raw,
                [
                    literal!(entry(b":method", b"GET")),
                    literal!(entry(b":scheme", b"https")),
                    literal!(entry(b":path", b"/asset")),
                    literal!(entry(b":authority", b"example.test")),
                    entry(b"x-tag", value).expect("field"),
                ],
                &mut block,
            )
            .expect("field section");
        let result = Receive::new(&mut conn).owned(
            Id::new(raw).expect("request stream"),
            VarInt::ZERO.try_promise(&block).expect("promise"),
            Boundary::More,
        );
        if raw == 8 {
            assert_eq!(
                result,
                Err(Error::Push(push::Error::Mismatch { push_id: 0 }))
            );
        } else {
            result.expect("same discarded fields");
            assert!(matches!(
                Events::new(&mut conn).poll_token().expect("promise event"),
                Some(Event::PushPromise { push_id: 0, .. })
            ));
        }
    }
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("no mismatched event")
            .is_none()
    );
}

#[test]
fn stream_before_promise() {
    let block = [
        literal!(entry(b":method", b"HEAD")),
        literal!(entry(b":scheme", b"https")),
        literal!(entry(b":path", b"/asset")),
        literal!(entry(b":authority", b"example.test")),
    ]
    .try_block()
    .expect("promised HEAD request");
    let promise = VarInt::ZERO.try_promise(&block).expect("promise");
    let headers = [
        literal!(entry(b":status", b"200")),
        literal!(entry(b"content-length", b"42")),
    ]
    .try_headers()
    .expect("HEAD response headers");
    let id = Id::new(3).expect("push stream");
    for reset in [false, true] {
        let mut conn = Connection::with_push::<Authority>(Role::Client).expect("connection");
        conn.allow_push(Role::Client, 0).expect("push grant");
        Receive::new(&mut conn)
            .owned(
                id,
                VarInt::ZERO.try_stream(&headers).expect("push response"),
                Boundary::End,
            )
            .expect("push before promise");
        assert!(Receive::new(&mut conn).is_blocked(id));
        assert!(
            Events::new(&mut conn)
                .poll_token()
                .expect("wait for promised method")
                .is_none()
        );
        if reset {
            Receive::new(&mut conn)
                .reset(id, None, Code::from_u16(0))
                .expect("reset before promise");
            assert!(matches!(
                Events::new(&mut conn).poll_token().expect("reset event"),
                Some(Event::Reset { .. })
            ));
        }
        for raw in [0, 4] {
            Receive::new(&mut conn)
                .borrowed(
                    Id::new(raw).expect("request stream"),
                    &promise,
                    Boundary::More,
                )
                .expect("identical promises before wake");
        }
        for _ in 0..2 {
            assert!(matches!(
                Events::new(&mut conn).poll_token().expect("promise event"),
                Some(Event::PushPromise { push_id: 0, .. })
            ));
        }
        if !reset {
            assert!(matches!(
                Events::new(&mut conn)
                    .poll_token()
                    .expect("HEAD push without DATA"),
                Some(Event::PushHeaders { push_id: 0, .. })
            ));
        }
        assert!(
            Events::new(&mut conn)
                .poll_token()
                .expect("single wake and no retired push")
                .is_none()
        );
    }
}
