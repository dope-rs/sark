pub mod common;

use std::cell::Cell;

use common::{
    atomic::content::Body,
    conn::{Events, build::Fields as _, payload::Write as _},
    field::entry,
    message::{
        Frame as _,
        promise::{Authority, Push as _},
    },
};
use dope_quic::wire::{stream::Id, varint::VarInt};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection, Error,
        event::{Event, Frame, Phase, Role, Site, Violation},
        io::{Receive, Send},
    },
    frame::{Payload, TYPE_DATA, TYPE_PUSH_PROMISE},
    manifold::encoder::Encoder,
    stream::Target,
};
use sark_http::{
    field::{literal, metadata::block::ContentError},
    payload::{
        fixed,
        response::{Body as _, Method, dispatch::Accept, status::Code},
    },
};

#[test]
fn suppress_content() {
    for (method, status) in [
        (Method::Head, Code::OK),
        (Method::Other, Code::NO_CONTENT),
        (Method::Other, Code::NOT_MODIFIED),
        (Method::Other, Code::RESET_CONTENT),
    ] {
        for kind in 0..4 {
            let mut conn = Connection::with_role(Role::Server).expect("server");
            let calls = Cell::new(0);
            let encoder = Encoder::new(&mut conn, Target::new(Id::ZERO), method);
            let result = match kind {
                0 => Accept::fixed(encoder, status, (), b"body".to_vec()),
                1 => Accept::statics(encoder, status, (), fixed::Body::new(b"body")),
                2 => Accept::exact(encoder, status, (), Body(&calls)),
                _ => Accept::bounded(encoder, status, (), Body(&calls)),
            };
            result.ok().expect("prepare").finish().expect("commit");
            assert_eq!(
                calls.get(),
                0,
                "suppressed body is never measured or encoded"
            );
            let write = conn.events().poll_write().expect("write").expect("headers");
            assert_eq!(write.boundary(), Boundary::End);
            let wire = write.into_bytes();
            let (frame, used) = Payload::parse(&wire, usize::MAX).expect("response frame");
            assert!(matches!(frame, Payload::Headers(_)));
            assert_eq!(used, wire.len());
            assert!(conn.events().poll_write().expect("drained").is_none());
        }
    }
}

#[test]
fn reject_informational() {
    for kind in 0..4 {
        let mut conn = Connection::with_role(Role::Server).expect("server");
        let calls = Cell::new(0);
        let encoder = Encoder::new(&mut conn, Target::new(Id::ZERO), Method::Other);
        let result = match kind {
            0 => Accept::fixed(encoder, Code::EARLY_HINTS, (), b"body".to_vec()),
            1 => Accept::statics(encoder, Code::EARLY_HINTS, (), fixed::Body::new(b"body")),
            2 => Accept::exact(encoder, Code::EARLY_HINTS, (), Body(&calls)),
            _ => Accept::bounded(encoder, Code::EARLY_HINTS, (), Body(&calls)),
        };
        assert!(
            matches!(result, Err((Error::Informational(Code::EARLY_HINTS), target)) if target.stream_id() == Id::ZERO)
        );
        assert_eq!(calls.get(), 0);
        assert!(conn.events().poll_write().expect("empty").is_none());
    }
}

#[test]
fn connect_content() {
    for status in [Code::OK, Code::NO_CONTENT, Code::RESET_CONTENT] {
        let mut conn = Connection::with_role(Role::Server).expect("server");
        Receive::new(&mut conn)
            .owned(
                Id::ZERO,
                [
                    literal!(entry(b":method", b"CONNECT")),
                    literal!(entry(b":authority", b"example.test:443")),
                    literal!(entry(b"content-length", b"0")),
                ]
                .try_headers()
                .expect("request"),
                Boundary::More,
            )
            .expect("CONNECT");
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("request"),
            Some(Event::Headers { .. })
        ));
        Receive::new(&mut conn)
            .owned(
                Id::ZERO,
                b"client".try_frame(TYPE_DATA).expect("tunnel DATA"),
                Boundary::End,
            )
            .expect("CONNECT input is tunnel data before the response");
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("tunnel input"),
            Some(Event::Data { data, .. }) if data.len() == 6
        ));
        let encoder = Encoder::new(&mut conn, Target::new(Id::ZERO), Method::Connect);
        Accept::fixed(encoder, status, (), b"tunnel".to_vec())
            .ok()
            .expect("prepare")
            .finish()
            .expect("commit");
        let head = conn.events().poll_write().expect("write").expect("headers");
        assert_eq!(head.boundary(), Boundary::More);
        let data = conn.events().poll_write().expect("write").expect("data");
        assert_eq!(data.boundary(), Boundary::End);
        let wire = data.into_bytes();
        let (data, used) = Payload::parse(&wire, usize::MAX).expect("DATA frame");
        assert!(matches!(data, Payload::Data(bytes) if bytes == b"tunnel"));
        assert_eq!(used, wire.len());
        assert!(conn.events().poll_write().expect("drained").is_none());
    }
}

#[test]
fn received_content_uses_originating_method() {
    for (method, status, length, body, expected) in [
        (&b"HEAD"[..], &b"200"[..], &b"7"[..], &b""[..], None),
        (b"GET", b"304", b"7", b"", None),
        (
            b"GET",
            b"204",
            b"0",
            b"",
            Some(ContentError::ForbiddenLength { length: 0 }),
        ),
        (b"CONNECT", b"200", b"1", b"tunnel", None),
        (b"CONNECT", b"204", b"1", b"tunnel", None),
        (b"CONNECT", b"400", b"1", b"x", None),
        (
            b"CONNECT",
            b"400",
            b"1",
            b"xx",
            Some(ContentError::Exceeded {
                remaining: 1,
                actual: 2,
            }),
        ),
    ] {
        let mut conn = Connection::with_role(Role::Client).expect("client");
        let fields = [
            entry(b":method", method).expect("method"),
            literal!(entry(b":authority", b"example.test:443")),
            literal!(entry(b":scheme", b"https")),
            literal!(entry(b":path", b"/")),
        ];
        let fields = if method == b"CONNECT" {
            &fields[..2]
        } else {
            &fields[..]
        };
        Send::new(&mut conn)
            .headers(Id::ZERO, fields, Boundary::More)
            .expect("originating request");
        let head = [
            entry(b":status", status).expect("status"),
            entry(b"content-length", length).expect("length"),
        ]
        .try_headers()
        .expect("response headers");
        let boundary = if body.is_empty() {
            Boundary::End
        } else {
            Boundary::More
        };
        Receive::new(&mut conn)
            .owned(Id::ZERO, head, boundary)
            .expect("response ingress");
        let event = Events::new(&mut conn)
            .poll_token()
            .expect("response event")
            .expect("response");
        let content = match event {
            Event::Headers { message, .. } => {
                assert_eq!(message.boundary(), boundary);
                if body.is_empty() {
                    None
                } else {
                    Receive::new(&mut conn)
                        .owned(
                            Id::ZERO,
                            body.try_frame(TYPE_DATA).expect("response DATA"),
                            Boundary::End,
                        )
                        .expect("response DATA ingress");
                    Events::new(&mut conn).poll_token().expect("content event")
                }
            }
            event => Some(event),
        };
        match expected {
            Some(error) => assert!(matches!(content,
                Some(Event::Rejected { stream_id, error: actual })
                    if stream_id == Id::ZERO.into() && actual == error
            )),
            None => {
                if body.is_empty() {
                    assert!(content.is_none());
                } else {
                    assert!(matches!(
                        content,
                        Some(Event::Data { data, .. })
                            if data.segments().flatten().copied().eq(body.iter().copied())
                    ));
                }
            }
        }
        assert!(
            Events::new(&mut conn)
                .poll_token()
                .expect("no extra response events")
                .is_none()
        );
    }
}

#[test]
fn connect_rejects_push_promise() {
    let mut conn = Connection::with_role(Role::Client).expect("client");
    Send::new(&mut conn)
        .headers(
            Id::ZERO,
            [
                literal!(entry(b":method", b"CONNECT")),
                literal!(entry(b":authority", b"example.test:443")),
            ],
            Boundary::More,
        )
        .expect("CONNECT request");
    Receive::new(&mut conn)
        .owned(
            Id::ZERO,
            [literal!(entry(b":status", b"200"))]
                .try_headers()
                .expect("response headers"),
            Boundary::More,
        )
        .expect("CONNECT response");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("response"),
        Some(Event::Headers { .. })
    ));
    let promise = VarInt::ZERO
        .try_promise(&Authority.try_request().expect("promised request"))
        .expect("PUSH_PROMISE");
    assert_eq!(
        Receive::new(&mut conn).owned(Id::ZERO, promise, Boundary::More),
        Err(Error::FrameUnexpected(Violation::Receive(Frame {
            stream_id: 0,
            kind: TYPE_PUSH_PROMISE.get(),
            role: Role::Client,
            site: Site::Request(Phase::Headers),
        }))),
    );
}
