use o3::depot::arena::slab::key;
use sark_grpc::Code;

use crate::common::grpc::wire::Transfer;
pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    client::{
        call::{bidi, request, response, unary},
        io::ingress::Session as _,
        session::Session,
    },
    details::{Action, Claim, Diagnostic},
    message::{Compression, Frame},
    metadata::Metadata,
    protocol::{Block, section},
};
use sark_h2::{
    ServerRole, client,
    conn::{DataOutcome, machine::Connection},
};
use sark_http::payload::response::status;

use crate::common::{
    codec::Bytes,
    grpc::call::calls::{Response, Settings},
};

#[test]
fn split_bidi_retires_claim() {
    let limits = Settings::new(Some(1), None, Some(1), None)
        .try_build()
        .expect("gRPC fixture limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let metadata = Metadata::new();
            let stream = bidi::Session::open(&mut client, b"/proof/Chat", None, &metadata)
                .expect("split bidi identity");
            let (send, response) = stream.split();
            bidi::Session::cancel_half(&mut client, send);
            assert!(
                response::Session::try_poll(&mut client, &response).is_err(),
                "the response capability cannot remain pending after its request owner is retired"
            );
            let next = bidi::Session::open(&mut client, b"/proof/Chat", None, &metadata)
                .expect("retirement returns the exact response and call capacity");
            bidi::Session::cancel(&mut client, next);
        },
    )
    .expect("one-call split-bidi client");
}

#[test]
fn missing_status_retires() {
    let limits = Settings::new(Some(1), Some(1), None, Some(1))
        .try_build()
        .expect("gRPC fixture limits");
    Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let mut server = Connection::<ServerRole>::try_new().expect("valid H2 server plan");
            let metadata = Metadata::new();
            let request = b"request".to_vec();
            let call = unary::Session::try_start(
                &mut client,
                b"/proof/Unary",
                None,
                &metadata,
                &mut Bytes,
                &request,
            )
            .into_result()
            .expect("unary request");
            let streams = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("gRPC request transfer");
            assert_eq!(streams.len(), 1);
            let id = streams[0];

            let headers = Block::response(&Metadata::new()).expect("response headers");
            server
                .streams()
                .headers()
                .respond(id, &headers, Boundary::More)
                .expect("response head");
            let prefix = Frame::try_header(Compression::Plain, 3).expect("message header");
            let sent = server
                .streams()
                .try_parts(id, &prefix, b"bad", Boundary::End)
                .expect("terminal DATA without grpc-status");
            assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == prefix.len() + 3));
            let wire = server.output().first().to_vec();

            client
                .ingest(&wire)
                .expect("missing status remains per-call");
            let result = unary::Session::try_poll(&mut client, &call)
                .expect("completed unary lookup")
                .expect("missing-status completion");
            assert_eq!(result.status.code(), Code::Unknown);
            assert!(matches!(
                Diagnostic::from(&result.status),
                Diagnostic::HttpStatus(status::Code::OK)
            ));
            let failure = unary::Session::try_poll(&mut client, &call)
                .err()
                .expect("retired unary completion");
            let (index, epoch, actual) = match Diagnostic::from(&failure) {
                Diagnostic::Claim(Claim::Key {
                    action: Action::Completion,
                    index,
                    generation: epoch,
                    observation: key::Observation::Free { actual },
                }) => Some((index, epoch, actual)),
                _ => None,
            }
            .expect("free unary completion");

            let next = unary::Session::try_start(
                &mut client,
                b"/proof/Unary",
                None,
                &metadata,
                &mut Bytes,
                &request,
            )
            .into_result()
            .expect("terminal failure returns call and completion capacity");
            let failure = unary::Session::try_poll(&mut client, &call)
                .err()
                .expect("stale unary completion");
            assert!(matches!(
                Diagnostic::from(&failure),
                Diagnostic::Claim(Claim::Key {
                    action: Action::Completion,
                    index: observed,
                    generation: queried,
                    observation: key::Observation::Generation { actual: current },
                }) if observed == index && queried == epoch && current == actual
            ));
            unary::Session::cancel(&mut client, next);
        },
    )
    .expect("one-call unary client");
}

#[test]
fn early_reply_reclaims() {
    for closed in [false, true] {
        let limits = Settings::new(Some(1), Some(1), None, None)
            .try_build()
            .expect("gRPC fixture limits");
        Session::<_>::with_limits(
            client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
            limits,
            |mut client| {
                let mut server = Connection::<ServerRole>::try_new().expect("valid H2 server plan");
                let metadata = Metadata::new();
                let stream = request::Session::open(&mut client, b"/proof/Upload", None, &metadata)
                    .expect("first upload identity");
                let streams = Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_request()
                .expect("gRPC request transfer");
                assert_eq!(streams.len(), 1);
                let id = streams[0];

                Response::new(&mut server, id)
                    .try_respond(b"early-unary-response")
                    .expect("gRPC response fixture");
                Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_response()
                .expect("gRPC response transfer");
                if closed {
                    let call = request::Session::finish(&mut client, stream)
                        .expect("close request after its response completes");
                    unary::Session::cancel(&mut client, call);
                } else {
                    request::Session::cancel(&mut client, stream);
                }

                let next = request::Session::open(&mut client, b"/proof/Upload", None, &metadata)
                    .expect("early unary response and open request are reclaimed together");
                request::Session::cancel(&mut client, next);
            },
        )
        .expect("one-call client-streaming client");
    }
}
