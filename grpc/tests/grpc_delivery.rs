pub mod common;

use std::iter;

use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Code,
    client::{
        call::unary::Session as _,
        config::Limits,
        io::{Session as _, ingress::Session as _},
        session::Session,
    },
    metadata::Metadata,
    protocol::{Block, section},
};
use sark_h2::{
    ServerRole, client,
    conn::{event::Event, machine::Connection},
};

#[test]
fn owns_terminal_trailers() {
    Session::with(
        client::Plan::<section::Plan>::new(Limits::default().calls())
            .expect("valid H2 client plan"),
        |mut client| {
            let call = client
                .start_raw(b"/svc/Method", None, &Metadata::new(), b"request")
                .expect("unary call capacity");
            let mut server = Connection::<ServerRole>::try_new().expect("server connection");
            server
                .ingress()
                .ingest(client.outbound())
                .expect("client request frame");
            let id = iter::from_fn(|| server.ingress().poll_event())
                .find_map(|event| match event {
                    Event::Headers { stream_id: id, .. } => Some(id.id()),
                    _ => None,
                })
                .expect("request stream id");

            let mut fields = Block::new();
            fields
                .push(b":status", b"200")
                .expect("literal status field");
            fields
                .push(b"content-type", b"application/grpc+proto")
                .expect("literal content type field");
            fields
                .push(b"x-terminal", b"owned")
                .expect("literal terminal field");
            fields
                .push(b"grpc-status", b"3")
                .expect("literal gRPC status field");
            fields
                .push(b"grpc-message", b"terminal%20failure")
                .expect("literal gRPC message field");
            server
                .streams()
                .headers()
                .respond(id, &fields, Boundary::End)
                .expect("terminal headers fit server stream");

            client
                .ingest(server.output().first())
                .expect("client accepts terminal frame");
            let result = client
                .poll(&call)
                .expect("unary terminal response remains valid")
                .expect("unary terminal response is complete");
            assert!(result.metadata.is_empty());
            assert!(result.message.is_none());
            assert_eq!(result.status.code(), Code::InvalidArgument);
            assert_eq!(result.status.message(), Some("terminal failure"));
            assert_eq!(
                result.status.metadata().values(b"x-terminal").next(),
                Some(b"owned".as_slice())
            );
        },
    )
    .expect("client session completes");
}
