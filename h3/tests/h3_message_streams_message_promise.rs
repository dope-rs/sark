use sark_h3::{conn::io::Send, frame::ErrorCode};
use sark_http::field::literal;

use crate::common::{
    conn::{Events, build::Fields as _},
    field::entry,
    message::{
        Frame as _,
        promise::{Authority, Push as _},
    },
    stream::Stream as _,
};

pub mod common;

use dope_quic::{
    errors::Code,
    wire::{stream::Id, varint::VarInt},
};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection, Error,
        event::{Event, Frame, Phase, Role, Site, Violation},
        io::Receive,
    },
    frame::{
        Payload, STREAM_TYPE_CONTROL, TYPE_CANCEL_PUSH, TYPE_HEADERS, TYPE_PUSH_PROMISE,
        TYPE_SETTINGS,
    },
};

#[test]
fn promise_requires_request() {
    let promise = VarInt::new(17)
        .expect("push id")
        .try_promise(&Authority.try_request().expect("message fixture"))
        .expect("push fixture");
    let invalid = VarInt::new(9)
        .expect("push id")
        .try_stream(&promise)
        .expect("push wire");
    let mut conn = Connection::with_push::<Authority>(Role::Client)
        .expect("client push authority configuration must be valid");
    conn.allow_push(Role::Client, 17).expect("push grant");
    Send::new(&mut conn)
        .headers(Id::ZERO, Authority, Boundary::End)
        .expect("originating request");
    Receive::new(&mut conn)
        .owned(Id::new(0).expect("stream id"), promise, Boundary::More)
        .expect("request-stream push promise must be valid");
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("push promise must poll"), Some(Event::PushPromise {
            message,
            push_id: 17,
            ..
        }) if message.stream_id() == Id::ZERO.into())
    );

    let mut conn = Connection::with_push::<Authority>(Role::Client)
        .expect("client push authority configuration must be valid");
    conn.allow_push(Role::Client, 17).expect("push grant");
    Send::new(&mut conn)
        .headers(Id::ZERO, Authority, Boundary::End)
        .expect("originating request");
    Receive::new(&mut conn)
        .owned(
            Id::ZERO,
            VarInt::new(9)
                .expect("push id")
                .try_promise(&Authority.try_request().expect("message fixture"))
                .expect("push fixture"),
            Boundary::More,
        )
        .expect("promise for the inspected push");
    assert!(matches!(
        Events::new(&mut conn).poll_token().expect("push promise"),
        Some(Event::PushPromise { push_id: 9, .. })
    ));
    assert!(
        Receive::new(&mut conn).owned(Id::new(3).expect("stream id"), invalid, Boundary::More)
            == Err(Error::FrameUnexpected(Violation::Receive(Frame {
                stream_id: 3,
                kind: TYPE_PUSH_PROMISE.get(),
                role: Role::Client,
                site: Site::Push(Phase::Idle),
            })))
    );
}

#[test]
fn final_promise_owns_fin() {
    let mut wire = [literal!(entry(b":status", b"200"))]
        .try_block()
        .expect("message fixture")
        .try_frame(TYPE_HEADERS)
        .expect("message fixture");
    wire.extend(
        VarInt::new(17)
            .expect("push id")
            .try_promise(&Authority.try_request().expect("message fixture"))
            .expect("push fixture"),
    );
    let mut conn = Connection::with_push::<Authority>(Role::Client)
        .expect("client push authority configuration must be valid");
    conn.allow_push(Role::Client, 17).expect("push grant");
    Send::new(&mut conn)
        .headers(Id::ZERO, Authority, Boundary::End)
        .expect("originating request");

    Receive::new(&mut conn)
        .owned(Id::new(0).expect("stream id"), wire, Boundary::End)
        .expect("final push promise request stream must be valid");

    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("initial headers must poll"), Some(Event::Headers {
            message,
            ..
        }) if message.boundary() == Boundary::More)
    );
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("final push promise must poll"), Some(Event::PushPromise {
            message,
            push_id: 17,
            ..
        }) if message.stream_id() == Id::ZERO.into()
            && matches!(message.boundary(), Boundary::End))
    );
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("drained request stream must poll")
            .is_none()
    );
}

#[test]
fn reset_preserves_admitted_promise() {
    let response = [literal!(entry(b":status", b"200"))]
        .try_headers()
        .expect("response headers");
    let push = VarInt::ZERO;
    let promise = push
        .try_promise(&Authority.try_request().expect("promised request"))
        .expect("promise frame");
    for boundary in [Boundary::More, Boundary::End] {
        let mut conn = Connection::with_push::<Authority>(Role::Client).expect("connection");
        conn.allow_push(Role::Client, push.get())
            .expect("push grant");
        Send::new(&mut conn)
            .headers(Id::ZERO, Authority, Boundary::End)
            .expect("originating request");
        Receive::new(&mut conn)
            .owned(Id::ZERO, response.clone(), Boundary::More)
            .expect("response headers");
        Receive::new(&mut conn)
            .owned(Id::ZERO, promise.clone(), boundary)
            .expect("admitted promise");
        let code = Code::from_u16(42);
        Receive::new(&mut conn)
            .reset(Id::ZERO, Some(Id::ZERO.into()), code)
            .expect("parent reset");
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("preserved promise"),
            Some(Event::PushPromise { message, push_id: 0, head })
                if message.stream_id() == Id::ZERO.into()
                    && message.boundary() == Boundary::More
                    && head.fields().get(b":path") == Some(b"/asset".as_slice())
        ));
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("parent terminal"),
            Some(Event::Reset { stream_id, error_code, .. })
                if stream_id == Id::ZERO.into() && error_code == code
        ));
        Receive::new(&mut conn)
            .owned(
                Id::new(3).expect("push stream"),
                push.try_stream(&response).expect("push response"),
                Boundary::End,
            )
            .expect("independent push remains live");
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("push response"),
            Some(Event::PushHeaders { message, push_id: 0, .. })
                if message.boundary() == Boundary::End
        ));
    }
}

#[test]
fn missing_policy_cancels() {
    let mut conn =
        Connection::with_role(Role::Client).expect("default H3 client configuration must be valid");
    conn.control()
        .open(Id::new(2).expect("stream id"))
        .expect("client control stream id must be valid");
    {
        let setup = conn
            .events()
            .poll_write()
            .expect("control stream prefix must poll")
            .expect("control stream prefix must be queued");
        assert!(setup.stream_id() == Id::new(2).expect("stream id"));
    }

    Send::new(&mut conn).max_push(17).expect("push grant");
    let _ = conn.events().poll_write().expect("grant write");
    Receive::new(&mut conn)
        .owned(
            Id::new(0).expect("stream id"),
            VarInt::new(17)
                .expect("push id")
                .try_promise(&Authority.try_request().expect("message fixture"))
                .expect("push fixture"),
            Boundary::More,
        )
        .expect("well-formed unauthorized promise must be consumed");

    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("cancelled promise must poll")
            .is_none()
    );
    let write = conn
        .events()
        .poll_write()
        .expect("cancel-push write must poll")
        .expect("cancel-push write must be queued");
    assert!(write.stream_id() == Id::new(2).expect("stream id"));
    assert_eq!(
        &*write.prefix,
        &*VarInt::new(17)
            .expect("push id")
            .try_cancel()
            .expect("push fixture")
    );
}

#[test]
fn invalid_origin_cancels() {
    for host in [None, Some(&b""[..]), Some(&b"foreign.example"[..])] {
        let mut fields = vec![
            literal!(entry(b":method", b"GET")),
            literal!(entry(b":scheme", b"https")),
            literal!(entry(b":path", b"/asset")),
        ];
        if let Some(host) = host {
            fields.push(entry(b":authority", host).expect("field fixture"));
        }
        let mut conn = Connection::with_push::<Authority>(Role::Client)
            .expect("client push authority configuration must be valid");
        conn.control()
            .open(Id::new(2).expect("stream id"))
            .expect("client control stream id must be valid");
        {
            let setup = conn
                .events()
                .poll_write()
                .expect("control stream prefix must poll")
                .expect("control stream prefix must be queued");
            assert!(setup.stream_id() == Id::new(2).expect("stream id"));
        }

        Send::new(&mut conn).max_push(17).expect("push grant");
        let _ = conn.events().poll_write().expect("grant write");
        Receive::new(&mut conn)
            .owned(
                Id::new(0).expect("stream id"),
                VarInt::new(17)
                    .expect("push id")
                    .try_promise(&fields.try_block().expect("message fixture"))
                    .expect("push fixture"),
                Boundary::More,
            )
            .expect("well-formed foreign promise must be consumed");

        assert!(
            Events::new(&mut conn)
                .poll_token()
                .expect("cancelled promise must poll")
                .is_none()
        );
        let write = conn
            .events()
            .poll_write()
            .expect("cancel-push write must poll")
            .expect("cancel-push write must be queued");
        assert!(write.stream_id() == Id::new(2).expect("stream id"));
        assert_eq!(
            &*write.prefix,
            &*VarInt::new(17)
                .expect("push id")
                .try_cancel()
                .expect("push fixture")
        );
    }
}

#[test]
fn unsafe_promise_cancels() {
    for fields in [
        vec![
            literal!(entry(b":method", b"POST")),
            literal!(entry(b":scheme", b"https")),
            literal!(entry(b":path", b"/unsafe")),
            literal!(entry(b":authority", b"example.test")),
        ],
        vec![
            literal!(entry(b":method", b"GET")),
            literal!(entry(b":scheme", b"https")),
            literal!(entry(b":path", b"/content")),
            literal!(entry(b":authority", b"example.test")),
            literal!(entry(b"content-length", b"1")),
        ],
    ] {
        let promise = VarInt::new(17)
            .expect("push id")
            .try_promise(&fields.try_block().expect("message fixture"))
            .expect("push fixture");
        let mut conn = Connection::with_push::<Authority>(Role::Client)
            .expect("default H3 client configuration must be valid");
        conn.control()
            .open(Id::new(2).expect("stream id"))
            .expect("client control stream id must be valid");
        {
            let setup = conn
                .events()
                .poll_write()
                .expect("control stream prefix must poll")
                .expect("control stream prefix must be queued");
            assert!(setup.stream_id() == Id::new(2).expect("stream id"));
        }

        Send::new(&mut conn).max_push(17).expect("push grant");
        let _ = conn.events().poll_write().expect("grant write");
        Receive::new(&mut conn)
            .owned(Id::new(0).expect("stream id"), promise, Boundary::More)
            .expect("well-formed unsafe promise must be consumed");
        assert!(
            Events::new(&mut conn)
                .poll_token()
                .expect("cancelled promise must poll")
                .is_none()
        );
        let write = conn
            .events()
            .poll_write()
            .expect("cancel-push write must poll")
            .expect("cancel-push write must be queued");
        assert!(write.stream_id() == Id::new(2).expect("stream id"));
        assert_eq!(
            &*write.prefix,
            &*VarInt::new(17)
                .expect("push id")
                .try_cancel()
                .expect("push fixture")
        );
        assert!(write.payload.is_none());
    }
}

#[test]
fn settings_lead_control() {
    let mut wire = Vec::new();
    STREAM_TYPE_CONTROL.encode(&mut wire).expect("varint");
    wire.extend(
        VarInt::new(17)
            .expect("push id")
            .try_cancel()
            .expect("push fixture"),
    );
    let mut conn =
        Connection::with_role(Role::Server).expect("default H3 server configuration must be valid");

    assert!(
        Receive::new(&mut conn).owned(Id::new(2).expect("stream id"), wire, Boundary::More)
            == Err(Error::MissingSettings(Frame {
                stream_id: 2,
                kind: TYPE_CANCEL_PUSH.get(),
                role: Role::Server,
                site: Site::Control,
            }))
    );
}

#[test]
fn settings_are_unique() {
    let mut wire = Vec::new();
    STREAM_TYPE_CONTROL.encode(&mut wire).expect("varint");
    Payload::encode(TYPE_SETTINGS, &[], &mut wire).expect("first settings frame must encode");
    Payload::encode(TYPE_SETTINGS, &[], &mut wire).expect("second settings frame must encode");
    let mut conn =
        Connection::with_role(Role::Server).expect("default H3 server configuration must be valid");

    Receive::new(&mut conn)
        .owned(Id::new(2).expect("stream id"), wire, Boundary::More)
        .expect("first settings frame must be accepted");
    assert!(matches!(
        Events::new(&mut conn)
            .poll_token()
            .expect("first settings frame must poll"),
        Some(Event::Settings(_))
    ));
    assert!(
        matches!(Events::new(&mut conn).poll_token(), Err(error) if error.error_code() == ErrorCode::FrameUnexpected && error == Error::DuplicateSettings(Frame {
            stream_id: 2,
            kind: TYPE_SETTINGS.get(),
            role: Role::Server,
            site: Site::Control,
        }))
    );
}

#[test]
fn cancellation_ids() {
    let mut conn =
        Connection::with_role(Role::Client).expect("default H3 client configuration must be valid");
    conn.allow_push(Role::Client, 17).expect("push grant");
    Receive::new(&mut conn)
        .owned(
            Id::new(3).expect("stream id"),
            VarInt::new(17)
                .expect("push id")
                .try_control()
                .expect("push fixture"),
            Boundary::More,
        )
        .expect("client control stream frames must parse through settings");
    assert!(matches!(
        Events::new(&mut conn)
            .poll_token()
            .expect("client settings must poll"),
        Some(Event::Settings(_))
    ));
    assert!(matches!(
        Events::new(&mut conn).poll_token(),
        Ok(Some(Event::CancelPush { push_id: 17 }))
    ));

    let mut conn =
        Connection::with_role(Role::Server).expect("default H3 server configuration must be valid");
    conn.allow_push(Role::Server, 17).expect("push grant");
    conn.promise(17).expect("push promise");
    Receive::new(&mut conn)
        .owned(
            Id::new(2).expect("stream id"),
            VarInt::new(17)
                .expect("push id")
                .try_cancel()
                .expect("push fixture"),
            Boundary::More,
        )
        .expect("server cancel-push control stream must be valid");
    assert!(matches!(
        Events::new(&mut conn).poll_token(),
        Ok(Some(Event::CancelPush { push_id: 17 }))
    ));
}
