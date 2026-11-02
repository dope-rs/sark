pub mod common;

use common::grpc::{
    call::{
        Open as _,
        calls::{FIRST_RESPONSE, Response, SECOND_RESPONSE, UNARY_BUDGETS},
    },
    wire::Transfer,
};
use o3::buffer::segments;
use sark_grpc::{
    Code,
    client::{
        call::unary,
        config::{Input, Limits},
        io::ingress::Session as _,
        session::Session,
    },
    details::Diagnostic,
    message,
    protocol::section,
};
use sark_h2::{ServerRole, client, conn::machine::Connection};

#[test]
fn isolates_held_budget() {
    for (messages, bytes, reason, expected) in UNARY_BUDGETS {
        let input = Input::try_new(2, 2, messages, 16, bytes).expect("bounded input");
        let limits = Limits::default()
            .try_calls(2)
            .expect("two calls")
            .try_input(input)
            .expect("client limits");
        Session::with_limits(
            client::Plan::<section::Plan>::new(limits.calls()).expect("client plan"),
            limits,
            |mut client| {
                let calls = client.try_unary(2).expect("concurrent unary calls");
                let mut server = Connection::<ServerRole>::try_new().expect("server");
                let streams = Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_request()
                .expect("request transfer");
                assert_eq!(streams.len(), 2);
                Response::try_overlap(&mut server, [streams[0], streams[1]])
                    .expect("both messages precede either call's trailers");
                Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_response()
                .expect("call capacity does not fail the connection");

                let error = unary::Session::try_poll(&mut client, &calls[1])
                    .err()
                    .expect("blocked call fails without waiting for first trailers");
                assert_eq!(error.code(), Code::ResourceExhausted);
                let (actual, parts) = match Diagnostic::from(&error) {
                    Diagnostic::Capacity { reason, error } => Some((reason, error.into_parts())),
                    _ => None,
                }
                .expect("exact capacity diagnostic");
                assert_eq!(actual, reason);
                assert_eq!(parts, expected);
                let response = unary::Session::try_poll(&mut client, &calls[0])
                    .expect("first call remains valid")
                    .expect("first trailers progress past rejected data");
                assert_eq!(
                    response
                        .into_payload()
                        .expect("successful first response")
                        .contiguous(),
                    Some(FIRST_RESPONSE)
                );
            },
        )
        .expect("client session");
    }
}

#[test]
fn isolates_held_segments() {
    let input = Input::try_new(2, 2, 2, 1, 1024).expect("one retained segment");
    let limits = Limits::default()
        .try_calls(2)
        .expect("two calls")
        .try_input(input)
        .expect("client limits");
    Session::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("client plan"),
        limits,
        |mut client| {
            let calls = client.try_unary(2).expect("concurrent unary calls");
            let mut server = Connection::<ServerRole>::try_new().expect("server");
            let streams = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("request transfer");
            assert_eq!(streams.len(), 2);
            Response::try_overlap(&mut server, [streams[0], streams[1]])
                .expect("both messages precede either call's trailers");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("segment capacity does not fail the connection");

            let error = unary::Session::try_poll(&mut client, &calls[1])
                .err()
                .expect("blocked call fails without waiting for first trailers");
            assert_eq!(error.code(), Code::ResourceExhausted);
            assert!(
                matches!(
                    Diagnostic::from(&error),
                    Diagnostic::Message(message::Error::Transfer(
                        segments::Error::SegmentCapacity(shortage)
                    )) if shortage.requested.get() == 1 && shortage.available == 0
                ),
                "unexpected segment error: {error:?}"
            );
            let response = unary::Session::try_poll(&mut client, &calls[0])
                .expect("first call remains valid")
                .expect("first trailers progress past rejected data");
            assert_eq!(
                response
                    .into_payload()
                    .expect("successful first response")
                    .contiguous(),
                Some(FIRST_RESPONSE)
            );
        },
    )
    .expect("client session");
}

#[test]
fn consumed_payload_blocks() {
    let input = Input::try_new(2, 2, 2, 1, 1024).expect("one retained segment");
    let limits = Limits::default()
        .try_calls(2)
        .expect("two calls")
        .try_input(input)
        .expect("client limits");
    Session::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("client plan"),
        limits,
        |mut client| {
            let calls = client.try_unary(2).expect("concurrent unary calls");
            let mut server = Connection::<ServerRole>::try_new().expect("server");
            let streams = Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_request()
            .expect("request transfer");
            assert_eq!(streams.len(), 2);
            Response::new(&mut server, streams[0])
                .try_respond(FIRST_RESPONSE)
                .expect("first response completes before second data");
            Response::new(&mut server, streams[1])
                .try_respond(SECOND_RESPONSE)
                .expect("second response waits for retained segment");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("completed response applies backpressure");

            let held = unary::Session::try_poll(&mut client, &calls[0])
                .expect("first call lookup")
                .expect("first response is complete")
                .into_payload()
                .expect("successful first response");
            assert_eq!(held.contiguous(), Some(FIRST_RESPONSE));
            assert!(
                unary::Session::try_poll(&mut client, &calls[1])
                    .expect("caller can still release the retained segment")
                    .is_none()
            );
            drop(held);
            client.resume().expect("returned segment resumes ingress");
            let response = unary::Session::try_poll(&mut client, &calls[1])
                .expect("second call lookup")
                .expect("second response resumes after payload release");
            assert_eq!(
                response
                    .into_payload()
                    .expect("successful second response")
                    .contiguous(),
                Some(SECOND_RESPONSE)
            );
        },
    )
    .expect("client session");
}
