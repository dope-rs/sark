pub mod common;

use common::grpc::{
    call::{
        Open as _,
        calls::{
            FIRST_RESPONSE, HTTP_BODY, HTTP_FAILURES, HTTP_MEDIA, HTTP_TRAILERS, Response,
            SECOND_RESPONSE, Settings,
        },
    },
    wire::Transfer,
};
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Code,
    client::{
        call::{bidi, response, unary},
        config::Limits,
        receive,
        session::Session,
    },
    details::{Diagnostic, Fault},
    metadata::Metadata,
    protocol::{Block, section},
};
use sark_h2::{ServerRole, client, conn::machine::Connection};

#[test]
fn ignores_informational() {
    Session::with(
        client::Plan::<section::Plan>::new(Limits::default().calls()).expect("client plan"),
        |mut client| {
            let call = bidi::Session::open(&mut client, b"/svc/Watch", None, &Metadata::new()).expect("call");
            let call = bidi::Session::finish(&mut client, call).expect("request end");
            let mut server = Connection::<ServerRole>::try_new().expect("server");
            let ids = Transfer { client: &mut client, server: &mut server }.try_request().expect("request");
            let mut info = Block::new();
            info.push(b":status", b"103").expect("informational status");
            info.push(b"x-meta-bin", b"!").expect("HTTP metadata");
            info.push(b"grpc-status", b"0").expect("interim metadata");
            server.streams().headers().respond(ids[0], &info, Boundary::More).expect("informational headers");
            Transfer { client: &mut client, server: &mut server }.try_response().expect("interim response");
            assert!(response::Session::try_poll(&mut client, &call).expect("ignore informational fields").is_none());
            Response::new(&mut server, ids[0]).try_respond(FIRST_RESPONSE).expect("final response");
            Transfer { client: &mut client, server: &mut server }.try_response().expect("final transfer");
            assert!(matches!(response::Session::try_poll(&mut client, &call), Ok(Some(receive::Response::Headers { .. }))));
            let event = response::Session::try_poll(&mut client, &call).expect("message").expect("queued message");
            let message = match event {
            receive::Response::Message { message } => Some(message),
            _ => None,
        }.expect("message");
            assert_eq!(message.payload.contiguous(), Some(FIRST_RESPONSE));
            assert!(matches!(response::Session::try_poll(&mut client, &call), Ok(Some(receive::Response::Trailers { status })) if status.code() == Code::Ok));
        },
    ).expect("client session");
}

#[test]
fn terminal_status_priority() {
    for (media, code, expected) in HTTP_FAILURES {
        Session::with(
            client::Plan::<section::Plan>::new(Limits::default().calls()).expect("client plan"),
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
                    .try_http(b"503", media, code, Boundary::End)
                    .expect("terminal error headers");
                Response::new(&mut server, streams[1])
                    .try_respond(FIRST_RESPONSE)
                    .expect("concurrent success");
                Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_response()
                .expect("HTTP error remains local to its call");

                let response = unary::Session::try_poll(&mut client, &calls[0])
                    .expect("error call lookup")
                    .expect("terminal status");
                assert_eq!(response.status.code(), expected);
                if code.is_none() {
                    assert!(matches!(
                        Diagnostic::from(&response.status),
                        Diagnostic::HttpStatus(code) if code.as_u16() == 503
                    ));
                }
                let response = unary::Session::try_poll(&mut client, &calls[1])
                    .expect("concurrent call lookup")
                    .expect("concurrent completion");
                assert_eq!(
                    response
                        .into_payload()
                        .expect("successful response")
                        .contiguous(),
                    Some(FIRST_RESPONSE)
                );
            },
        )
        .expect("client session");
    }
}

#[test]
fn rejects_ok_media() {
    for (media, boundary) in HTTP_MEDIA {
        let limits = Settings::new(Some(3), Some(1), None, None)
            .try_build()
            .expect("one completed response slot");
        Session::with_limits(
            client::Plan::<section::Plan>::new(limits.calls()).expect("client plan"),
            limits,
            |mut client| {
                let calls = client.try_unary(3).expect("concurrent unary calls");
                let mut server = Connection::<ServerRole>::try_new().expect("server");
                let streams = Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_request()
                .expect("request transfer");
                Response::new(&mut server, streams[0])
                    .try_respond(FIRST_RESPONSE)
                    .expect("occupy completion slot");
                let code = matches!(boundary, Boundary::End).then_some(b"0".as_slice());
                Response::new(&mut server, streams[1])
                    .try_http(b"200", media, code, boundary)
                    .expect("HTTP response with invalid gRPC media");
                if matches!(boundary, Boundary::More) {
                    Response::new(&mut server, streams[1])
                        .try_data(HTTP_BODY, Boundary::More)
                        .expect("non-gRPC response body");
                    Response::new(&mut server, streams[1])
                        .try_trailers(Some(Code::Ok))
                        .expect("status cannot override invalid media");
                }
                Response::new(&mut server, streams[2])
                    .try_respond(SECOND_RESPONSE)
                    .expect("healthy response after rejected media");
                Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_response()
                .expect("invalid media stays local while completion is full");

                drop(
                    unary::Session::try_poll(&mut client, &calls[0])
                        .expect("first call lookup")
                        .expect("release completion slot"),
                );
                let error = unary::Session::try_poll(&mut client, &calls[1])
                    .err()
                    .expect("invalid media completes its call with an error");
                assert_eq!(error.code(), Code::Unknown);
                assert!(matches!(
                    Diagnostic::from(&error),
                    Diagnostic::SectionStored(Fault::ContentType)
                ));
                let response = unary::Session::try_poll(&mut client, &calls[2])
                    .expect("healthy call lookup")
                    .expect("healthy concurrent completion");
                assert_eq!(
                    response
                        .into_payload()
                        .expect("healthy payload")
                        .contiguous(),
                    Some(SECOND_RESPONSE)
                );
            },
        )
        .expect("client session");
    }
}

#[test]
fn drains_http_body() {
    for (code, expected) in HTTP_TRAILERS {
        Session::with(
            client::Plan::<section::Plan>::new(Limits::default().calls()).expect("client plan"),
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
                    .try_http(b"503", Some(b"text/html"), None, Boundary::More)
                    .expect("initial error headers");
                Response::new(&mut server, streams[0])
                    .try_data(HTTP_BODY, Boundary::More)
                    .expect("intermediary response body");
                Response::new(&mut server, streams[1])
                    .try_respond(FIRST_RESPONSE)
                    .expect("concurrent success before error trailers");
                Response::new(&mut server, streams[0])
                    .try_trailers(code)
                    .expect("terminal headers");
                Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_response()
                .expect("HTTP error body drains without failing the connection");

                let response = unary::Session::try_poll(&mut client, &calls[0])
                    .expect("error call lookup")
                    .expect("terminal status");
                assert_eq!(response.status.code(), expected);
                assert!(response.message.is_none(), "HTTP body was drained");
                let response = unary::Session::try_poll(&mut client, &calls[1])
                    .expect("concurrent call lookup")
                    .expect("concurrent completion");
                assert_eq!(
                    response
                        .into_payload()
                        .expect("successful response")
                        .contiguous(),
                    Some(FIRST_RESPONSE)
                );
            },
        )
        .expect("client session");
    }
}

#[test]
fn retains_grpc_override() {
    Session::with(
        client::Plan::<section::Plan>::new(Limits::default().calls()).expect("client plan"),
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
                .try_http(b"503", Some(b"application/grpc"), None, Boundary::More)
                .expect("initial HTTP error headers");
            Response::new(&mut server, streams[0])
                .try_message(SECOND_RESPONSE)
                .expect("valid gRPC response body");
            Response::new(&mut server, streams[1])
                .try_respond(FIRST_RESPONSE)
                .expect("concurrent success");
            Response::new(&mut server, streams[0])
                .try_trailers(Some(Code::Ok))
                .expect("successful gRPC status overrides initial HTTP status");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("gRPC responses remain independent");

            let response = unary::Session::try_poll(&mut client, &calls[0])
                .expect("first call lookup")
                .expect("gRPC completion");
            assert_eq!(
                response
                    .into_payload()
                    .expect("successful final status")
                    .contiguous(),
                Some(SECOND_RESPONSE)
            );
            let response = unary::Session::try_poll(&mut client, &calls[1])
                .expect("concurrent call lookup")
                .expect("concurrent completion");
            assert_eq!(
                response
                    .into_payload()
                    .expect("successful response")
                    .contiguous(),
                Some(FIRST_RESPONSE)
            );
        },
    )
    .expect("client session");
}

#[test]
fn ends_without_trailers() {
    Session::with(
        client::Plan::<section::Plan>::new(Limits::default().calls()).expect("client plan"),
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
                .try_http(b"503", None, None, Boundary::More)
                .expect("initial error headers");
            Response::new(&mut server, streams[0])
                .try_data(HTTP_BODY, Boundary::End)
                .expect("terminal intermediary response body");
            Response::new(&mut server, streams[1])
                .try_respond(FIRST_RESPONSE)
                .expect("concurrent success");
            Transfer {
                client: &mut client,
                server: &mut server,
            }
            .try_response()
            .expect("missing gRPC trailers remain local to their call");

            let response = unary::Session::try_poll(&mut client, &calls[0])
                .expect("error call lookup")
                .expect("HTTP fallback completion");
            assert_eq!(response.status.code(), Code::Unavailable);
            let response = unary::Session::try_poll(&mut client, &calls[1])
                .expect("concurrent call lookup")
                .expect("concurrent completion");
            assert_eq!(
                response
                    .into_payload()
                    .expect("successful response")
                    .contiguous(),
                Some(FIRST_RESPONSE)
            );
        },
    )
    .expect("client session");
}
