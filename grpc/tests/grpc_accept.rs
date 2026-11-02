use dope::core::io::socket::Backlog;

pub mod common;

use std::{
    convert, io,
    io::Read,
    time::{Duration, Instant},
};

use dope::core::driver::route::table::conn;
use dope_test::harness::{peer, server::Pair};
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    message,
    metadata::Metadata,
    protocol::Block,
    server::config::{
        Input, Limits, Output,
        error::{Error, Field, Operation},
    },
};
use sark_h2::{
    ClientRole,
    conn::{event, machine::Connection},
    frame::numbers::ErrorCode,
    stream::Id,
};

use crate::common::grpc::{call::Accept, settings::Settings};

#[test]
fn invalid_limits() {
    let result = Limits::default().try_calls(0);
    std::assert_matches!(
        result.err().expect("zero call capacity"),
        Error::ZeroCapacity(Field::Calls)
    );

    let input = Input::default();
    let result = Input::try_new(input.messages(), 0, input.stream_bytes(), input.bytes());
    std::assert_matches!(
        result.err().expect("zero segment capacity"),
        Error::ZeroCapacity(Field::InputSegments)
    );

    let result = Output::try_new(Output::default().messages(), message::HEADER_LEN - 1);
    std::assert_matches!(
        result.err().expect("output cannot fit a message header"),
        Error::MinimumCapacity {
            field: Field::OutputWireBytes,
            actual,
            minimum,
        } if actual == message::HEADER_LEN - 1 && minimum == message::HEADER_LEN
    );

    let result = Output::try_new(0, message::HEADER_LEN);
    std::assert_matches!(
        result.err().expect("zero output message capacity"),
        Error::ZeroCapacity(Field::OutputMessages)
    );

    let input = Input::default();
    let result = Input::try_new(
        input.messages(),
        input.segments(),
        usize::MAX,
        input.bytes(),
    );
    std::assert_matches!(
        result.err().expect("stream wire capacity overflow"),
        Error::Overflow {
            field: Field::StreamWireBytes,
            operation: Operation::Add,
            left: usize::MAX,
            ..
        }
    );

    if let Some(bytes) = (u32::MAX as usize).checked_add(1) {
        let result = Limits::default().try_bytes(bytes);
        std::assert_matches!(
            result.err().expect("message exceeds the wire length range"),
            Error::MaximumCapacity {
                field: Field::MessageBytes,
                maximum,
                error,
            } if maximum == u32::MAX as usize && error.requested() == bytes
        );
    }
}

#[test]
fn accept_capacity() {
    let harness = Pair::bind().expect("capacity-test harness");
    let bind = harness.addr();
    let config = Settings {
        bind,
        connections: conn::Capacity::fixed::<4>(),
        backlog: Backlog::fixed::<16>(),
        limits: Limits::default()
            .try_calls(1)
            .expect("one concurrent gRPC call"),
    };
    harness
        .run(
            move |context, listener| {
                let mut accepted = Vec::new();
                let retired = config
                    .try_serve_on(
                        Accept(&mut accepted),
                        context,
                        listener,
                        common::recover,
                        |result| result.expect("gRPC runtime"),
                    )
                    .expect("gRPC readiness bank");
                assert!(
                    retired.is_empty(),
                    "unexpected server retirement: {retired:?}"
                );
                assert_eq!(accepted.as_slice(), [Id::FIRST_CLIENT]);
                Ok::<(), convert::Infallible>(())
            },
            |bind| {
                let mut socket = peer::Peer::at(bind)
                    .try_connect()
                    .expect("connect listening peer");
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("capacity-test read deadline");
                let mut client =
                    Connection::<ClientRole>::try_new().expect("construct capacity-test H2 client");
                let headers = Block::request(b"/svc/Method", None, &Metadata::new())
                    .expect("valid capacity-test request headers");
                assert_eq!(
                    client
                        .streams()
                        .headers()
                        .request(&headers, Boundary::End)
                        .expect("open first local H2 stream"),
                    Id::new(1).expect("stream ID")
                );
                assert_eq!(
                    client
                        .streams()
                        .headers()
                        .request(&headers, Boundary::End)
                        .expect("open second local H2 stream"),
                    Id::new(3).expect("stream ID")
                );
                io::Write::write_all(&mut socket, client.output().first())
                    .expect("write capacity-test requests");
                client.transfer().drain(usize::MAX);

                let mut bytes = [0u8; 4096];
                let deadline = Instant::now() + Duration::from_secs(2);
                let mut done = false;
                let mut refused = false;
                while !done || !refused {
                    let left = deadline
                        .checked_duration_since(Instant::now())
                        .expect("capacity response deadline");
                    socket
                        .set_read_timeout(Some(left))
                        .expect("response deadline");
                    let read = socket
                        .read(&mut bytes)
                        .expect("read capacity-test response");
                    assert_ne!(read, 0, "server closed before capacity response");
                    client
                        .ingress()
                        .ingest(&bytes[..read])
                        .expect("ingest capacity-test H2 response");
                    while let Some(event) = client.ingress().poll_event() {
                        match event {
                            event::Event::Headers {
                                stream_id: id,
                                boundary: Boundary::End,
                                ..
                            } if id == Id::new(1).expect("stream ID") => done = true,
                            event::Event::StreamReset {
                                stream_id: id,
                                error,
                                ..
                            } if id == Id::new(3).expect("stream ID") => {
                                assert_eq!(error, ErrorCode::RefusedStream);
                                refused = true;
                            }
                            _ => {}
                        }
                    }
                }
                assert_eq!(client.settings().peer().max_concurrent_streams, Some(1));
            },
        )
        .expect("capacity-at-accept E2E");
}
