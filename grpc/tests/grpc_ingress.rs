use std::iter;
pub mod common;

use common::codec::Bytes;
use o3::buffer::storage;
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Code, Status,
    client::{
        call::{bidi, response::Session as _},
        config::Input,
        io::{Session as _, ingress::Session as _},
        receive::Response,
        session::Session,
    },
    message::Compression,
    metadata::Metadata,
    protocol::{Block, section},
};
use sark_h2::{
    ServerRole, client,
    conn::{DataOutcome, event::Event, machine::Connection},
};

use crate::common::grpc::call::calls::Settings;

#[test]
fn retains_owner_order() {
    let payloads: [&[u8]; 3] = [
        b"first-exact-owner",
        b"second-exact-owner",
        b"third-exact-owner",
    ];
    let max_len = payloads
        .iter()
        .map(|payload| payload.len())
        .max()
        .expect("payload fixture is non-empty");
    let limits = Settings::try_ingress(1, 1, 1, 1, max_len).expect("ingress limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let stream = bidi::Session::open(&mut client, b"/svc/Stream", None, &Metadata::new())
                .expect("open bidi stream");
            let response = bidi::Session::finish(&mut client, stream).expect("finish bidi send");
            let request = client.outbound().to_vec();
            client
                .drain(request.len())
                .expect("released egress resumes queued calls");

            let mut server = Connection::<ServerRole>::try_new().expect("valid H2 server plan");
            server
                .ingress()
                .ingest(&request)
                .expect("ingest request wire");
            let id = iter::from_fn(|| server.ingress().poll_event())
                .find_map(|event| match event {
                    Event::Headers { stream_id: id, .. } => Some(id.id()),
                    _ => None,
                })
                .expect("request stream id");
            let headers = Block::response(&Metadata::new()).expect("valid response headers");
            server
                .streams()
                .headers()
                .respond(id, &headers, Boundary::More)
                .expect("send response headers");
            let head =
                storage::Shared::try_copy(server.output().first()).expect("copy response headers");
            server.transfer().drain(head.len());
            client
                .ingest_owned(head)
                .expect("headers fill the one-slot delivery queue");
            let mut framed = Vec::new();
            for payload in payloads {
                Bytes::try_frame(Compression::Plain, payload, &mut framed)
                    .expect("encode message frame");
            }
            assert!(matches!(
                server
                    .streams()
                    .try_send(id, &framed, Boundary::More)
                    .expect("send response data"),
                DataOutcome::Sent(sent) if sent.get() == framed.len()
            ));
            let wire =
                storage::Shared::try_copy(server.output().first()).expect("copy response wire");
            server.transfer().drain(wire.len());
            let base = wire.as_slice().as_ptr();
            let mut start = 0;
            let expected = payloads.map(|payload| {
                let relative = wire.as_slice()[start..]
                    .windows(payload.len())
                    .position(|window| window == payload)
                    .expect("payload on response wire");
                let at = start + relative;
                start = at + payload.len();
                base.wrapping_add(at)
            });

            client
                .ingest_owned(wire)
                .expect("delivery pressure is resumable");
            let trailers =
                Block::trailers(&Status::ok(), &Metadata::new()).expect("valid trailers");
            server
                .streams()
                .headers()
                .trail(id, &trailers)
                .expect("send response trailers");
            let tail =
                storage::Shared::try_copy(server.output().first()).expect("copy response trailers");
            client
                .ingest_owned(tail)
                .expect("trailers wait behind retained messages");
            assert!(matches!(
                client.try_poll(&response).expect("response head event"),
                Some(Response::Headers { .. })
            ));
            for (payload, address) in payloads.into_iter().zip(expected) {
                let event = client
                    .try_poll(&response)
                    .expect("poll response message")
                    .expect("response message event");
                assert!(
                    matches!(event, Response::Message { .. }),
                    "response message order must be preserved"
                );
                if let Response::Message { message } = event {
                    assert_eq!(message.payload.contiguous(), Some(payload));
                    assert_eq!(
                        message
                            .payload
                            .contiguous()
                            .expect("contiguous retained payload")
                            .as_ptr(),
                        address
                    );
                }
            }
            let event = client
                .try_poll(&response)
                .expect("poll response trailers")
                .expect("response trailer event");
            assert!(
                matches!(event, Response::Trailers { .. }),
                "terminal owner must follow all messages"
            );
            if let Response::Trailers { status } = event {
                assert_eq!(status.code(), Code::Ok);
                assert_eq!(status.message(), None);
            }
            assert!(client.try_poll(&response).is_err());

            (bidi::Session::open(&mut client, b"/svc/Next", None, &Metadata::new()))
                .as_ref()
                .expect("terminal replay releases the exact in-flight slot");
        },
    )
    .expect("bounded client session");
}

#[test]
fn pull_holds_blocked_owner() {
    let defaults = Input::default();
    let limits = Settings::try_ingress(1, defaults.completed(), 1, 2, defaults.bytes())
        .expect("ingress limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let stream = bidi::Session::open(&mut client, b"/svc/Stream", None, &Metadata::new())
                .expect("open bidi stream");
            let response = bidi::Session::finish(&mut client, stream).expect("finish bidi send");
            let request = client.outbound().to_vec();
            client
                .drain(request.len())
                .expect("released egress resumes queued calls");

            let mut server = Connection::<ServerRole>::try_new().expect("valid H2 server plan");
            server
                .ingress()
                .ingest(&request)
                .expect("ingest request wire");
            let id = iter::from_fn(|| server.ingress().poll_event())
                .find_map(|event| match event {
                    Event::Headers { stream_id: id, .. } => Some(id.id()),
                    _ => None,
                })
                .expect("request stream id");

            let headers = Block::response(&Metadata::new()).expect("valid response headers");
            server
                .streams()
                .headers()
                .respond(id, &headers, Boundary::More)
                .expect("send response headers");
            let head_len = server.output().len();
            let head =
                storage::Shared::try_copy(server.output().first()).expect("copy response head");
            server.transfer().drain(head_len);
            client
                .ingest_owned(head)
                .expect("headers fill the one-slot delivery queue");

            let mut first = Vec::new();
            Bytes::try_frame(Compression::Plain, b"first", &mut first)
                .expect("encode first message");
            let sent = server
                .streams()
                .try_send(id, &first, Boundary::More)
                .expect("send first message");
            assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == first.len()));
            let len = server.output().len();
            let first =
                storage::Shared::try_copy(server.output().first()).expect("copy first message");
            server.transfer().drain(len);
            client
                .ingest_owned(first)
                .expect("first message waits behind headers");

            let mut second = Vec::new();
            Bytes::try_frame(Compression::Plain, b"second-exact-owner", &mut second)
                .expect("encode second message");
            let sent = server
                .streams()
                .try_send(id, &second, Boundary::More)
                .expect("send second message");
            assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == second.len()));
            let second =
                storage::Shared::try_copy(server.output().first()).expect("copy second message");
            let expected = second
                .as_slice()
                .windows(b"second-exact-owner".len())
                .find(|bytes| *bytes == b"second-exact-owner")
                .expect("second payload on response wire")
                .as_ptr();
            let mut owner = Some(second);
            let mut source = iter::from_fn(|| owner.take());
            client
                .ingest_many(&mut source)
                .expect("semantic pressure is resumable");

            let retained = owner
                .as_ref()
                .expect("source owner is not yielded before semantic admission");
            assert!(retained.as_ref().as_ptr() <= expected);
            assert!(expected < retained.as_ref().as_ptr().wrapping_add(retained.len()));

            assert!(matches!(
                client.try_poll(&response).expect("response head event"),
                Some(Response::Headers { .. })
            ));
            let mut source = iter::from_fn(|| owner.take());
            client
                .ingest_many(&mut source)
                .expect("released delivery credit admits the same owner");
            assert!(owner.is_none());

            let event = client
                .try_poll(&response)
                .expect("poll first message")
                .expect("first message event");
            assert!(
                matches!(event, Response::Message { .. }),
                "first message must follow response headers"
            );
            if let Response::Message { message } = event {
                assert_eq!(message.payload.contiguous(), Some(b"first".as_slice()));
            }
            let event = client
                .try_poll(&response)
                .expect("poll second message")
                .expect("second message event");
            assert!(
                matches!(event, Response::Message { .. }),
                "retained owner retry must yield the second message"
            );
            if let Response::Message { message } = event {
                assert_eq!(
                    message.payload.contiguous(),
                    Some(b"second-exact-owner".as_slice())
                );
                assert_eq!(
                    message
                        .payload
                        .contiguous()
                        .expect("contiguous retained payload")
                        .as_ptr(),
                    expected
                );
            }
        },
    )
    .expect("bounded client session");
}
