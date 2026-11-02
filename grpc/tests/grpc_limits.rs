use std::iter;

use sark_grpc::client::receive::Response;
use sark_h2::conn::DataOutcome;

pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    client::{
        call::bidi::Session as _,
        config::{Input, Limits},
        io::{Session as _, ingress::Session as _},
        session::Session,
    },
    message::Compression,
    metadata::Metadata,
    protocol::{Block, section},
};
use sark_h2::{
    ServerRole, client,
    conn::{event::Event, machine::Connection},
};

use crate::common::codec::Bytes;

#[test]
fn live_buffer() {
    let defaults = Input::default();
    let input = Input::try_new(
        defaults.completed(),
        defaults.events(),
        1 << 20,
        defaults.segments(),
        1000,
    )
    .expect("one bounded live input owner");
    let limits = Limits::default()
        .try_bytes(1 << 20)
        .expect("one-mebibyte message limit")
        .try_input(input)
        .expect("bounded client input layout");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let stream = client
                .open(b"/svc/Method", None, &Metadata::new())
                .expect("start stream");
            let req = client.outbound().to_vec();
            client
                .drain(req.len())
                .expect("released egress resumes queued calls");

            let mut server =
                Connection::<ServerRole>::try_new().expect("construct H2 server state");
            server
                .ingress()
                .ingest(&req)
                .expect("server ingests client request");
            let id = iter::from_fn(|| server.ingress().poll_event())
                .find_map(|event| match event {
                    Event::Headers { stream_id: id, .. } => Some(id.id()),
                    _ => None,
                })
                .expect("request stream id");

            let head = Block::response(&Metadata::new()).expect("response headers");
            server
                .streams()
                .headers()
                .respond(id, &head, Boundary::More)
                .expect("send response headers");

            let body = Bytes::try_frames(15, 100, Compression::Plain, 7).expect("framed fixture");
            let sent = server
                .streams()
                .try_send(id, &body, Boundary::More)
                .expect("send response data");
            assert!(
                matches!(sent,
                    DataOutcome::Sent(sent) if sent.get() == body.len()
                ),
                "flow window must accept the test body"
            );

            let resp = server.output().first().to_vec();
            client.ingest(&resp).expect("bounded owner admission");
            let mut headers = 0;
            let mut messages = 0;
            let mut trailers = 0;
            while let Some(event) = client
                .try_poll(&stream)
                .expect("poll bounded bidi response")
            {
                match event {
                    Response::Headers { .. } => headers += 1,
                    Response::Message { message } => {
                        assert_eq!(message.payload.len(), 100);
                        messages += 1;
                    }
                    Response::Trailers { .. } => trailers += 1,
                }
            }
            assert_eq!(headers, 1);
            assert_eq!(messages, 15);
            assert_eq!(trailers, 0, "the test response remains open");
        },
    )
    .expect("bounded client session");
}
