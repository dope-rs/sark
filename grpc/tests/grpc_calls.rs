use sark_grpc::{Code, client::io::Session as _};

use crate::common::grpc::wire::Transfer;
pub mod common;
use sark_grpc::{
    client::{
        call::{Admission, bidi, request, response, unary},
        config::Limits,
        receive,
        session::Session,
    },
    metadata::Metadata,
    protocol::section,
};
use sark_h2::{ServerRole, client, conn::machine::Connection, stream};

use crate::common::{
    codec::Bytes,
    grpc::call::calls::{Response, Settings},
};

#[test]
fn recycles_request_owner() {
    let limits = Settings::new(None, None, None, Some(1))
        .try_build()
        .expect("gRPC fixture limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let mut server = Connection::<ServerRole>::try_new().expect("server connection");
            let metadata = Metadata::new();
            let mut codec = Bytes;
            let mut stream = request::Session::open(&mut client, b"/proof/Upload", None, &metadata)
                .expect("affine client stream");
            request::Session::try_send(&mut client, &mut stream, &mut codec, &b"first".to_vec())
                .into_result()
                .expect("first request message");
            assert_eq!(client.owner_cap(), 0);
            assert!(matches!(
                request::Session::try_send(
                    &mut client,
                    &mut stream,
                    &mut codec,
                    &b"second".to_vec(),
                ),
                Admission::Pending
            ));

            let streams = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("gRPC request transfer");
            assert_eq!(streams.len(), 1);
            let id = streams[0];
            assert_eq!(client.owner_cap(), 1);
            request::Session::try_send(&mut client, &mut stream, &mut codec, &b"second".to_vec())
                .into_result()
                .expect("second request message after physical owner return");
            let call = request::Session::finish(&mut client, stream)
                .expect("request-to-response transition");
            assert_eq!(
                Transfer {
                    client: &mut client,
                    server: &mut server
                }
                .try_request()
                .expect("gRPC request transfer"),
                [id]
            );

            Response::new(&mut server, id)
                .try_respond(b"two-messages")
                .expect("gRPC response fixture");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("gRPC response transfer");
            let result = unary::Session::try_poll(&mut client, &call)
                .expect("exact completion lookup")
                .expect("client-streaming response");
            assert_eq!(
                result
                    .into_payload()
                    .expect("response contains one message")
                    .contiguous(),
                Some(b"two-messages".as_slice())
            );
        },
    )
    .expect("bounded client");
}

#[test]
fn reverses_uploads() {
    Session::with(
        client::Plan::<section::Plan>::new(Limits::default().calls())
            .expect("valid H2 client plan"),
        |mut client| {
            let mut server = Connection::<ServerRole>::try_new().expect("server connection");
            let metadata = Metadata::new();
            let mut codec = Bytes;
            let mut first = request::Session::open(&mut client, b"/proof/Upload", None, &metadata)
                .expect("first upload");
            let mut second = request::Session::open(&mut client, b"/proof/Upload", None, &metadata)
                .expect("second upload");
            request::Session::try_send(&mut client, &mut first, &mut codec, &b"slow".to_vec())
                .into_result()
                .expect("first request message");
            request::Session::try_send(&mut client, &mut second, &mut codec, &b"fast".to_vec())
                .into_result()
                .expect("second request message");
            let first =
                request::Session::finish(&mut client, first).expect("first response transition");
            let second =
                request::Session::finish(&mut client, second).expect("second response transition");
            let streams = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("gRPC request transfer");
            assert_eq!(streams.len(), 2);
            let slow_id = streams[0];
            let fast_id = streams[1];
            Response::new(&mut server, fast_id)
                .try_respond(b"second")
                .expect("gRPC response fixture");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("gRPC response transfer");
            assert!(
                unary::Session::try_poll(&mut client, &first)
                    .expect("first response lookup")
                    .is_none()
            );
            let second = unary::Session::try_poll(&mut client, &second)
                .expect("second response lookup")
                .expect("second response completes first");
            assert_eq!(
                second
                    .into_payload()
                    .expect("second response contains one message")
                    .contiguous(),
                Some(b"second".as_slice())
            );
            Response::new(&mut server, slow_id)
                .try_respond(b"first")
                .expect("gRPC response fixture");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("gRPC response transfer");
            let first = unary::Session::try_poll(&mut client, &first)
                .expect("first response lookup")
                .expect("first response completes last");
            assert_eq!(
                first
                    .into_payload()
                    .expect("first response contains one message")
                    .contiguous(),
                Some(b"first".as_slice())
            );
        },
    )
    .expect("client");
}

#[test]
fn reclaims_cancelled() {
    let limits = Settings::new(Some(1), Some(1), None, None)
        .try_build()
        .expect("gRPC fixture limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let metadata = Metadata::new();
            let stale = request::Session::open(&mut client, b"/proof/Upload", None, &metadata)
                .expect("first capability");
            let status = request::Session::open(&mut client, b"/proof/Upload", None, &metadata)
                .err()
                .expect("the retained request consumes the sole call slot");
            assert!(status.code() == Code::ResourceExhausted, "{status:?}");

            request::Session::cancel(&mut client, stale);
            let next = request::Session::open(&mut client, b"/proof/Upload", None, &metadata)
                .expect("capacity after exact cancellation");
            request::Session::cancel(&mut client, next);
        },
    )
    .expect("single-call client");
}

#[test]
fn shares_event_slab() {
    let limits = Settings::new(None, None, Some(3), None)
        .try_build()
        .expect("gRPC fixture limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let mut server = Connection::<ServerRole>::try_new().expect("server connection");
            let metadata = Metadata::new();
            let mut codec = Bytes;
            let msg_a = b"first".to_vec();
            let msg_b = b"second".to_vec();
            let first = response::Session::try_start(
                &mut client,
                b"/proof/Watch",
                None,
                &metadata,
                &mut codec,
                &msg_a,
            )
            .into_result()
            .expect("first response capability");
            let second = response::Session::try_start(
                &mut client,
                b"/proof/Watch",
                None,
                &metadata,
                &mut codec,
                &msg_b,
            )
            .into_result()
            .expect("second response capability");
            let streams = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("gRPC request transfer");
            assert_eq!(streams.len(), 2);
            let id_a = streams[0];
            let id_b = streams[1];
            Response::new(&mut server, id_b)
                .try_respond(b"second-response")
                .expect("gRPC response fixture");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("gRPC response transfer");
            Response::new(&mut server, id_a)
                .try_respond(b"first-response")
                .expect("gRPC response fixture");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("gRPC response transfer");
            assert!(
                response::Session::try_poll(&mut client, &first)
                    .expect("first response lookup")
                    .is_none(),
                "the full shared slab cannot leak another stream's event"
            );
            assert_eq!(drain_response(&mut client, &second), b"second-response");
            assert_eq!(drain_response(&mut client, &first), b"first-response");
        },
    )
    .expect("one-response event slab");
}

#[test]
fn shares_bidi_identity() {
    let limits = Settings::new(None, None, None, Some(1))
        .try_build()
        .expect("gRPC fixture limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let mut server = Connection::<ServerRole>::try_new().expect("server connection");
            let metadata = Metadata::new();
            let mut codec = Bytes;
            let mut stream = bidi::Session::open(&mut client, b"/proof/Chat", None, &metadata)
                .expect("affine bidi stream");
            bidi::Session::try_send(&mut client, &mut stream, &mut codec, &b"one".to_vec())
                .into_result()
                .expect("first bidi message");
            let streams = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("gRPC request transfer");
            assert_eq!(streams.len(), 1);
            let id = streams[0];
            bidi::Session::try_send(&mut client, &mut stream, &mut codec, &b"two".to_vec())
                .into_result()
                .expect("recycled request owner");
            let response = bidi::Session::finish(&mut client, stream)
                .expect("request half-close preserves response half");
            assert_eq!(
                Transfer {
                    client: &mut client,
                    server: &mut server
                }
                .try_request()
                .expect("gRPC request transfer"),
                [id]
            );
            Response::new(&mut server, id)
                .try_respond(b"same-call-response")
                .expect("gRPC response fixture");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("gRPC response transfer");
            assert_eq!(
                drain_response(&mut client, &response),
                b"same-call-response"
            );
        },
    )
    .expect("bounded bidi client");
}

#[test]
fn splits_bidi_progress() {
    let limits = Settings::new(None, None, None, Some(1))
        .try_build()
        .expect("gRPC fixture limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let mut server = Connection::<ServerRole>::try_new().expect("server connection");
            let metadata = Metadata::new();
            let mut codec = Bytes;
            let stream = bidi::Session::open(&mut client, b"/proof/Chat", None, &metadata)
                .expect("affine bidi stream");
            let (mut send, response) = stream.split();

            bidi::Session::try_half(&mut client, &mut send, &mut codec, &b"split".to_vec())
                .into_result()
                .expect("split request authority sends independently");
            let streams = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("gRPC request transfer");
            assert_eq!(streams.len(), 1);
            let id = streams[0];
            Response::new(&mut server, id)
                .try_respond(b"split-response")
                .expect("gRPC response fixture");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("gRPC response transfer");

            bidi::Session::try_half(
                &mut client,
                &mut send,
                &mut codec,
                &b"after-response".to_vec(),
            )
            .into_result()
            .expect("remote response completion does not close request authority");
            bidi::Session::finish_half(&mut client, send)
                .expect("finishing the send half does not consume receive ownership");
            assert_eq!(
                Transfer {
                    client: &mut client,
                    server: &mut server
                }
                .try_request()
                .expect("gRPC request transfer"),
                [id]
            );
            assert_eq!(drain_response(&mut client, &response), b"split-response");
        },
    )
    .expect("bounded split-bidi client");
}

#[test]
fn reclaims_early_response() {
    let limits = Settings::new(Some(1), None, Some(3), None)
        .try_build()
        .expect("gRPC fixture limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let mut server = Connection::<ServerRole>::try_new().expect("server connection");
            let metadata = Metadata::new();
            let stream = bidi::Session::open(&mut client, b"/proof/Chat", None, &metadata)
                .expect("first bidi identity");
            let (mut stale, response) = stream.split();
            let streams = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("gRPC request transfer");
            assert_eq!(streams.len(), 1);
            let id = streams[0];
            Response::new(&mut server, id)
                .try_respond(b"early-response")
                .expect("gRPC response fixture");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("gRPC response transfer");
            response::Session::cancel(&mut client, response);

            let mut fresh = bidi::Session::open(&mut client, b"/proof/Chat", None, &metadata)
                .expect("detached response claim and request record were both reclaimed");
            assert!(matches!(
                bidi::Session::try_half(
                    &mut client,
                    &mut stale,
                    &mut Bytes,
                    &b"stale".to_vec()
                ).into_result(),
                Err(status) if status.code() == Code::Internal
            ));
            bidi::Session::cancel_half(&mut client, stale);
            bidi::Session::try_send(&mut client, &mut fresh, &mut Bytes, &b"fresh".to_vec())
                .into_result()
                .expect("stale cleanup cannot affect the replacement call");
            bidi::Session::cancel(&mut client, fresh);
        },
    )
    .expect("one-call split-bidi client");
}

fn drain_response<'scope>(
    client: &mut Session<'scope, client::Plan<section::Plan>>,
    stream: &response::Stream<'scope, stream::Key>,
) -> Vec<u8> {
    assert!(matches!(
        response::Session::try_poll(client, stream),
        Ok(Some(receive::Response::Headers { .. }))
    ));
    let mut payload = Vec::new();
    let event = response::Session::try_poll(client, stream)
        .expect("poll response message")
        .expect("response headers must be followed by a message");
    assert!(
        matches!(event, receive::Response::Message { .. }),
        "expected response message"
    );
    if let receive::Response::Message { message } = event {
        payload.extend_from_slice(message.payload.contiguous().expect("contiguous response"));
    }
    assert!(matches!(
        response::Session::try_poll(client, stream),
        Ok(Some(receive::Response::Trailers { status })) if status.code() == Code::Ok
    ));
    payload
}
