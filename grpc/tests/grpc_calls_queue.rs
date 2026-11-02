use crate::common::grpc::wire::Transfer;
pub mod common;

use sark_grpc::{
    Code, Status,
    client::{call::response, receive, session::Session},
    metadata::Metadata,
    protocol::{Block, section},
};
use sark_h2::{ServerRole, client, conn::machine::Connection, frame::numbers};

use crate::common::{
    codec::Bytes,
    grpc::call::calls::{self, Response},
};

#[test]
fn drains_before_retiring() {
    for code in [Code::Ok, Code::Cancelled] {
        let limits = calls::Settings::new(Some(1), Some(1), Some(3), Some(1))
            .try_build()
            .expect("gRPC fixture limits");
        Session::<_>::with_limits(
            client::Plan::<section::Plan>::new(limits.calls()).expect("one H2 call"),
            limits,
            |mut client| {
                let mut server = Connection::<ServerRole>::try_new().expect("H2 server");
                let metadata = Metadata::new();
                let request = b"request".to_vec();
                let stream = response::Session::try_start(
                    &mut client,
                    b"/proof/Watch",
                    None,
                    &metadata,
                    &mut Bytes,
                    &request,
                )
                .into_result()
                .expect("one response claim");
                let ids = Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_request()
                .expect("gRPC request transfer");
                assert_eq!(ids.len(), 1);
                let id = ids[0];
                Response::new(&mut server, id)
                    .try_header()
                    .expect("gRPC response fixture");
                Response::new(&mut server, id)
                    .try_message(b"queued")
                    .expect("gRPC response fixture");
                Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_response()
                .expect("gRPC response transfer");
                if code == Code::Ok {
                    let trailers =
                        Block::trailers(&Status::ok(), &metadata).expect("successful trailers");
                    server
                        .streams()
                        .headers()
                        .trail(id, &trailers)
                        .expect("close the response");
                } else {
                    let key = server.status().stream_key(id).expect("live response");
                    server
                        .streams()
                        .try_cancel(key, numbers::ErrorCode::Cancel)
                        .expect("reset the response");
                }
                Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_response()
                .expect("gRPC response transfer");
                assert!(matches!(
                    response::Session::try_poll(&mut client, &stream),
                    Ok(Some(receive::Response::Headers { .. }))
                ));
                let event = response::Session::try_poll(&mut client, &stream)
                    .expect("closed stream preserves preceding message")
                    .expect("message remains queued");
                let receive::Response::Message { message } = event else {
                    panic!("message precedes termination");
                };
                assert_eq!(
                    message.payload.contiguous().expect("contiguous payload"),
                    b"queued"
                );
                let terminal = response::Session::try_poll(&mut client, &stream);
                if code == Code::Ok {
                    assert!(
                        matches!(terminal, Ok(Some(receive::Response::Trailers { status }))
                        if status.code() == Code::Ok)
                    );
                } else {
                    let status = terminal.err().expect("queued reset is delivered last");
                    assert_eq!(status.code(), code);
                }
                response::Session::try_poll(&mut client, &stream)
                    .err()
                    .expect("terminal delivery retires the claim");
                let next = response::Session::try_start(
                    &mut client,
                    b"/proof/Watch",
                    None,
                    &metadata,
                    &mut Bytes,
                    &request,
                )
                .into_result()
                .expect("terminal delivery returns the only claim");
                response::Session::cancel(&mut client, next);
            },
        )
        .expect("closed response queue");
    }
}

#[test]
fn reuses_open_claim() {
    let limits = calls::Settings::new(Some(1), Some(1), Some(1), Some(1))
        .try_build()
        .expect("gRPC fixture limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("one H2 call"),
        limits,
        |mut client| {
            let mut server = Connection::<ServerRole>::try_new().expect("H2 server");
            let metadata = Metadata::new();
            let stream = response::Session::try_start(
                &mut client,
                b"/proof/Watch",
                None,
                &metadata,
                &mut Bytes,
                &b"request".to_vec(),
            )
            .into_result()
            .expect("one response claim");
            let ids = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("gRPC request transfer");
            assert_eq!(ids.len(), 1);
            let id = ids[0];
            Response::new(&mut server, id)
                .try_header()
                .expect("gRPC response fixture");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("gRPC response transfer");
            assert!(matches!(
                response::Session::try_poll(&mut client, &stream),
                Ok(Some(receive::Response::Headers { .. }))
            ));
            for payload in [b"first".as_slice(), b"second".as_slice()] {
                assert!(
                    response::Session::try_poll(&mut client, &stream)
                        .expect("empty queue retains its open claim")
                        .is_none()
                );
                Response::new(&mut server, id)
                    .try_message(payload)
                    .expect("gRPC response fixture");
                Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_response()
                .expect("gRPC response transfer");
                let event = response::Session::try_poll(&mut client, &stream)
                    .expect("later delivery uses the same claim")
                    .expect("later message is queued");
                let receive::Response::Message { message } = event else {
                    panic!("response message");
                };
                assert_eq!(
                    message.payload.contiguous().expect("contiguous payload"),
                    payload
                );
            }
            let trailers = Block::trailers(&Status::ok(), &metadata).expect("trailers");
            server
                .streams()
                .headers()
                .trail(id, &trailers)
                .expect("close response");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("gRPC response transfer");
            assert!(matches!(response::Session::try_poll(&mut client, &stream),
                Ok(Some(receive::Response::Trailers { status })) if status.code() == Code::Ok));
        },
    )
    .expect("reusable open response queue");
}
