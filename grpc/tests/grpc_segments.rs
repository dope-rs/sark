pub mod common;

use std::iter;

use o3::buffer::{segments, storage};
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Status,
    client::{
        call::unary::Session as _,
        config::Limits,
        io::{Session as _, ingress::Session as _},
        session::Session,
    },
    message::{Compression, Frame},
    metadata::Metadata,
    protocol::{Block, section},
};
use sark_h2::{
    ServerRole, client,
    conn::{DataOutcome, event::Event, machine::Connection},
};

#[test]
fn fragments_keep_owners() {
    Session::with(
        client::Plan::<section::Plan>::new(Limits::default().calls())
            .expect("valid H2 client plan"),
        |mut client| {
            let call = client
                .start_raw(b"/svc/Method", None, &Metadata::new(), b"request")
                .expect("fixed unary request must start");
            let mut server =
                Connection::<ServerRole>::try_new().expect("default H2 server must allocate");
            server
                .ingress()
                .ingest(client.outbound())
                .expect("fixed client request must reach the server");
            let stream = iter::from_fn(|| server.ingress().poll_event())
                .find_map(|event| match event {
                    Event::Headers { stream_id: id, .. } => Some(id.id()),
                    _ => None,
                })
                .expect("request stream id");

            let headers =
                Block::response(&Metadata::new()).expect("empty response metadata must encode");
            server
                .streams()
                .headers()
                .respond(stream, &headers, Boundary::More)
                .expect("response headers must queue");
            let payload = b"segmented-message";
            let header =
                Frame::try_header(Compression::Plain, payload.len()).expect("message header");
            let sent = server
                .streams()
                .try_parts(stream, &header, payload, Boundary::More)
                .expect("split response payload must queue");
            assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == header.len() + payload.len()));
            let trailers = Block::trailers(&Status::ok(), &Metadata::new())
                .expect("successful empty trailers must encode");
            server
                .streams()
                .headers()
                .trail(stream, &trailers)
                .expect("response trailers must queue");

            let wire = server.output().first().to_vec();
            let start = wire
                .windows(payload.len())
                .position(|window| window == payload)
                .expect("gRPC payload on wire");
            let split = start + 6;
            let end = start + payload.len();
            let owner_a = storage::Shared::try_copy(&wire[start..split])
                .expect("first payload fragment must allocate");
            let owner_b = storage::Shared::try_copy(&wire[split..end])
                .expect("second payload fragment must allocate");
            let ptr_a = owner_a.as_slice().as_ptr();
            let ptr_b = owner_b.as_slice().as_ptr();

            client
                .ingest(&wire[..start])
                .expect("response and gRPC headers must ingest");
            client
                .ingest_owned(owner_a)
                .expect("first retained fragment must ingest");
            assert!(
                client
                    .poll(&call)
                    .expect("partial unary call must poll")
                    .is_none()
            );
            client
                .ingest_owned(owner_b)
                .expect("second retained fragment must ingest");
            client
                .ingest(&wire[end..])
                .expect("terminal trailers must ingest");
            let result = client
                .poll(&call)
                .expect("complete unary call must poll")
                .expect("completed unary response");
            let frame = result.message.as_ref().expect("one gRPC message");
            assert_eq!(frame.payload.count(), segments::Count::TWO);
            let mut segments = frame.payload.segments();
            let first = segments.next().expect("first payload segment").as_slice();
            let second = segments.next().expect("second payload segment").as_slice();
            assert_eq!(first, &payload[..6]);
            assert_eq!(second, &payload[6..]);
            assert_eq!(first.as_ptr(), ptr_a);
            assert_eq!(second.as_ptr(), ptr_b);
            assert!(segments.next().is_none());
        },
    )
    .expect("fixed segmented ownership session must complete");
}

#[test]
fn header_accepts_fragments() {
    Session::with(
        client::Plan::<section::Plan>::new(Limits::default().calls())
            .expect("valid H2 client plan"),
        |mut client| {
            let call = client
                .start_raw(b"/svc/Method", None, &Metadata::new(), b"request")
                .expect("fixed unary request must start");
            let mut server =
                Connection::<ServerRole>::try_new().expect("default H2 server must allocate");
            server
                .ingress()
                .ingest(client.outbound())
                .expect("fixed client request must reach the server");
            let stream = iter::from_fn(|| server.ingress().poll_event())
                .find_map(|event| match event {
                    Event::Headers { stream_id: id, .. } => Some(id.id()),
                    _ => None,
                })
                .expect("request stream id");

            let headers =
                Block::response(&Metadata::new()).expect("empty response metadata must encode");
            server
                .streams()
                .headers()
                .respond(stream, &headers, Boundary::More)
                .expect("response headers must queue");
            let payload = b"fragmented-header";
            let header =
                Frame::try_header(Compression::Plain, payload.len()).expect("message header");
            let sent = server
                .streams()
                .try_parts(stream, &header, payload, Boundary::More)
                .expect("fragmented response payload must queue");
            assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == header.len() + payload.len()));
            let trailers = Block::trailers(&Status::ok(), &Metadata::new())
                .expect("successful empty trailers must encode");
            server
                .streams()
                .headers()
                .trail(stream, &trailers)
                .expect("response trailers must queue");

            for byte in server.output().first().chunks(1) {
                client
                    .ingest(byte)
                    .expect("single-byte wire fragment must ingest");
            }
            let result = client
                .poll(&call)
                .expect("fragmented unary call must poll")
                .expect("fragmented unary response");
            let response = result
                .into_payload()
                .expect("one fragmented response payload");
            let response = response
                .segments()
                .flat_map(|segment| segment.as_ref().iter().copied())
                .collect::<Vec<_>>();
            assert_eq!(response, payload);
        },
    )
    .expect("fixed fragmented header session must complete");
}
