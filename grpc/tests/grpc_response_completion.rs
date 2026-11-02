pub mod common;

use common::{
    codec::Bytes,
    grpc::{
        call::{
            Open as _,
            calls::{
                BAD_MESSAGE_FRAMES, BAD_STATUS_FIELDS, FIRST_RESPONSE, RESET_FAILURES, Response,
                SMALL_RESPONSE, Settings,
            },
            ingress::INCOMPLETE_RESPONSES,
        },
        wire::Transfer,
    },
};
use sark_core::manifold::stream::{Boundary, Section};
use sark_grpc::{
    client::{
        call::{bidi, response, unary},
        config::{Input, Limits},
        io::{Session as _, ingress::Session as _},
        receive,
        session::Session,
    },
    details::Diagnostic,
    message,
    metadata::Metadata,
    protocol::section,
};
use sark_h2::{ServerRole, client, conn::machine::Connection, frame::numbers::ErrorCode};

#[test]
fn peer_reset_codes() {
    for (wire, reason, code) in RESET_FAILURES {
        let plan = client::Plan::<section::Plan>::new(Limits::default().calls()).expect("plan");
        Session::with(plan, |mut client| {
            let unary = unary::Session::start_raw(
                &mut client, b"/svc/Unary", None, &Metadata::new(), b""
            ).expect("unary");
            let bidi = bidi::Session::open(
                &mut client, b"/svc/Stream", None, &Metadata::new()
            ).expect("stream");
            let stream = bidi::Session::finish(&mut client, bidi).expect("request end");
            let sibling = unary::Session::start_raw(
                &mut client, b"/svc/Sibling", None, &Metadata::new(), b""
            ).expect("sibling");
            let mut server = Connection::<ServerRole>::try_new().expect("server");
            let ids = Transfer { client: &mut client, server: &mut server }
                .try_request().expect("requests");
            assert_eq!(ids.len(), 3);
            for id in &ids[..2] {
                let key = server.status().stream_key(*id).expect("live request");
                server.streams().try_cancel(key, wire).expect("peer reset");
            }
            Response::new(&mut server, ids[2]).try_respond(FIRST_RESPONSE).expect("sibling response");
            Transfer { client: &mut client, server: &mut server }
                .try_response().expect("stream-local resets");
            for status in [
                unary::Session::poll(&mut client, &unary).err().expect("unary reset"),
                response::Session::try_poll(&mut client, &stream).err().expect("stream reset"),
            ] {
                assert_eq!(status.code(), code);
                assert!(matches!(Diagnostic::from(&status), Diagnostic::Reset(actual) if *actual == reason));
            }
            let response = unary::Session::poll(&mut client, &sibling)
                .expect("sibling lookup").expect("sibling completion");
            assert_eq!(response.into_payload().expect("sibling success").contiguous(), Some(FIRST_RESPONSE));
        }).expect("session");
    }
}

#[test]
fn bad_status_stays_local() {
    for (section, boundary, code, fault, fields) in [
        (Section::Initial, Boundary::End),
        (Section::Initial, Boundary::More),
        (Section::Trailing, Boundary::End),
    ]
    .into_iter()
    .flat_map(|(section, boundary)| {
        BAD_STATUS_FIELDS
            .into_iter()
            .map(move |(code, fault, fields)| (section, boundary, code, fault, fields))
    }) {
        let plan = client::Plan::<section::Plan>::new(Limits::default().calls()).expect("plan");
        Session::with(plan, |mut client| {
            let calls = client.try_unary(2).expect("unary calls");
            let bidi = bidi::Session::open(&mut client, b"/svc/Stream", None, &Metadata::new())
                .expect("stream");
            let stream = bidi::Session::finish(&mut client, bidi).expect("request end");
            let mut server = Connection::<ServerRole>::try_new().expect("server");
            let ids = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("requests");
            assert_eq!(ids.len(), 3);
            for id in [ids[0], ids[2]] {
                Response::new(&mut server, id)
                    .try_status(section, boundary, fields)
                    .expect("malformed gRPC status over H2");
            }
            Response::new(&mut server, ids[1])
                .try_respond(FIRST_RESPONSE)
                .expect("sibling response");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("malformed status stays local");
            let failure = loop {
                match response::Session::try_poll(&mut client, &stream) {
                    Ok(Some(receive::Response::Headers { .. })) => {}
                    result => break result.err().expect("stream status"),
                }
            };
            for status in [
                unary::Session::try_poll(&mut client, &calls[0])
                    .err()
                    .expect("unary status"),
                failure,
            ] {
                assert_eq!(status.code(), code);
                assert!(matches!(
                    Diagnostic::from(&status),
                    Diagnostic::SectionStored(actual) if actual == fault
                ));
            }
            let sibling = unary::Session::try_poll(&mut client, &calls[1])
                .expect("sibling lookup")
                .expect("sibling completion");
            assert_eq!(
                sibling
                    .into_payload()
                    .expect("sibling success")
                    .contiguous(),
                Some(FIRST_RESPONSE)
            );
        })
        .expect("session");
    }
}

#[test]
fn reset_after_response_retires_request() {
    let limits = Settings::new(Some(2), Some(1), Some(1), None)
        .try_build()
        .expect("one event slot");
    let plan = client::Plan::<section::Plan>::new(limits.calls()).expect("plan");
    Session::with_limits(plan, limits, |mut client| {
        let early = bidi::Session::open(&mut client, b"/svc/Early", None, &Metadata::new())
            .expect("open request");
        let sibling = client.try_unary(1).expect("sibling");
        let mut server = Connection::<ServerRole>::try_new().expect("server");
        let ids = Transfer {
            client: &mut client,
            server: &mut server,
        }
        .try_request()
        .expect("requests");
        Response::new(&mut server, ids[0])
            .try_http(b"200", Some(b"application/grpc"), Some(b"0"), Boundary::End)
            .expect("response finishes before request");
        let key = server
            .status()
            .stream_key(ids[0])
            .expect("open request half");
        server
            .streams()
            .try_cancel(key, ErrorCode::Cancel)
            .expect("reset request half");
        Response::new(&mut server, ids[1])
            .try_respond(FIRST_RESPONSE)
            .expect("sibling response");
        Transfer {
            client: &mut client,
            server: &mut server,
        }
        .try_response()
        .expect("request reset preserves queued response");
        assert!(
            matches!(bidi::Session::try_poll(&mut client, &early).expect("early lookup"),
            Some(receive::Response::Trailers { status }) if status.code() == sark_grpc::Code::Ok)
        );
        assert_eq!(
            unary::Session::try_poll(&mut client, &sibling[0])
                .expect("sibling lookup")
                .expect("sibling completion")
                .into_payload()
                .expect("sibling success")
                .contiguous(),
            Some(FIRST_RESPONSE)
        );
        for call in client.try_open(2).expect("both request records returned") {
            bidi::Session::cancel(&mut client, call);
        }
    })
    .expect("session");
}

#[test]
fn bad_message_stays_local() {
    for (first, invalid, code, expected) in BAD_MESSAGE_FRAMES {
        let input = Input::try_new(1, 1, 1, 1, 1).expect("one retained message and delivery");
        let limits = Limits::default()
            .try_calls(3)
            .expect("three calls")
            .try_bytes(1)
            .expect("one-byte message")
            .try_input(input)
            .expect("bounded input");
        let plan = client::Plan::<section::Plan>::new(limits.calls()).expect("plan");
        Session::with_limits(plan, limits, |mut client| {
            let calls = client.try_unary(3).expect("concurrent calls");
            let mut server = Connection::<ServerRole>::try_new().expect("server");
            let ids = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("requests");
            Response::new(&mut server, ids[0])
                .try_respond(b"")
                .expect("occupy completion");
            let mut victim = Response::new(&mut server, ids[1]);
            victim.try_header().expect("victim header");
            victim.try_message(first).expect("retained first message");
            victim
                .try_data(&invalid, Boundary::More)
                .expect("invalid gRPC data");
            Response::new(&mut server, ids[2])
                .try_respond(SMALL_RESPONSE)
                .expect("sibling response");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("stream errors preserve the connection");
            assert!(client.is_blocked());
            assert!(
                unary::Session::try_poll(&mut client, &calls[0])
                    .expect("blocker lookup")
                    .expect("blocker completion")
                    .into_payload()
                    .expect("blocker success")
                    .is_empty()
            );
            let error = unary::Session::try_poll(&mut client, &calls[1])
                .err()
                .expect("victim error");
            assert_eq!(error.code(), code);
            match (Diagnostic::from(&error), expected) {
                (Diagnostic::Message(actual), Diagnostic::Message(expected)) => {
                    assert_eq!(actual, expected)
                }
                (
                    Diagnostic::Local { reason: actual, .. },
                    Diagnostic::Local {
                        reason: expected, ..
                    },
                ) => assert_eq!(actual, expected),
                _ => panic!("unexpected message error: {error:?}"),
            }
            assert_eq!(
                unary::Session::try_poll(&mut client, &calls[2])
                    .expect("sibling lookup")
                    .expect("sibling completion")
                    .into_payload()
                    .expect("sibling success")
                    .contiguous(),
                Some(SMALL_RESPONSE)
            );
            for call in client.try_unary(3).expect("all call slots returned") {
                unary::Session::cancel(&mut client, call);
            }
        })
        .expect("session");
    }
}

#[test]
fn terminal_event_pressure() {
    let limits = Settings::new(Some(1), Some(1), Some(1), None)
        .try_build()
        .expect("one event slot");
    let plan = client::Plan::<section::Plan>::new(limits.calls()).expect("plan");
    Session::with_limits(plan, limits, |mut client| {
        let call = bidi::Session::open(&mut client, b"/svc/Stream", None, &Metadata::new())
            .expect("stream");
        let call = bidi::Session::finish(&mut client, call).expect("request end");
        let mut server = Connection::<ServerRole>::try_new().expect("server");
        let ids = Transfer { client: &mut client, server: &mut server }
            .try_request().expect("request");
        let mut frame = Vec::new();
        Bytes::try_frame(message::Compression::Plain, b"terminal", &mut frame).expect("frame");
        Response::new(&mut server, ids[0]).try_header().expect("headers");
        Response::new(&mut server, ids[0]).try_data(&frame, Boundary::End).expect("terminal data");
        Transfer { client: &mut client, server: &mut server }.try_response().expect("response");
        assert!(matches!(response::Session::try_poll(&mut client, &call),
            Ok(Some(receive::Response::Headers { .. }))));
        let event = response::Session::try_poll(&mut client, &call).expect("message").expect("queued message");
        let message = match event {
            receive::Response::Message { message } => Some(message),
            _ => None,
        }.expect("message");
        assert_eq!(message.payload.contiguous(), Some(b"terminal".as_slice()));
        let event = response::Session::try_poll(&mut client, &call).expect("terminal").expect("queued terminal");
        let status = match event {
            receive::Response::Trailers { status } => Some(status),
            _ => None,
        }.expect("trailers");
        assert_eq!(status.code(), sark_grpc::Code::Unknown);
        assert!(matches!(Diagnostic::from(&status), Diagnostic::HttpStatus(code) if code.as_u16() == 200));
        let next = bidi::Session::open(&mut client, b"/svc/Next", None, &Metadata::new()).expect("retired call capacity");
        bidi::Session::cancel(&mut client, next);
    }).expect("session");
}

#[test]
fn rejects_incomplete() {
    for payload in INCOMPLETE_RESPONSES {
        let plan = client::Plan::<section::Plan>::new(Limits::default().calls()).expect("plan");
        Session::with(plan, |mut client| {
            let unary =
                unary::Session::start_raw(&mut client, b"/svc/Unary", None, &Metadata::new(), b"")
                    .expect("unary");
            let bidi = bidi::Session::open(&mut client, b"/svc/Stream", None, &Metadata::new())
                .expect("stream");
            let stream = bidi::Session::finish(&mut client, bidi).expect("request end");
            let wire = Response::try_wire(client.outbound(), payload).expect("response fixture");
            client.ingest(&wire).expect("response wire");
            let error = unary::Session::poll(&mut client, &unary)
                .err()
                .expect("incomplete unary");
            assert_eq!(error.code(), sark_grpc::Code::Internal);
            assert!(matches!(
                Diagnostic::from(&error),
                Diagnostic::Message(message::Error::Incomplete)
            ));
            let result = loop {
                match response::Session::try_poll(&mut client, &stream) {
                    Ok(Some(
                        receive::Response::Headers { .. } | receive::Response::Message { .. },
                    )) => {}
                    result => break result,
                }
            };
            let error = result.err().expect("incomplete stream");
            assert_eq!(error.code(), sark_grpc::Code::Internal);
            assert!(matches!(
                Diagnostic::from(&error),
                Diagnostic::Message(message::Error::Incomplete)
            ));
        })
        .expect("session");
    }
}
