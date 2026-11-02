pub mod common;

use common::{
    codec::Bytes,
    grpc::{
        call::{
            Open as _,
            calls::{FIRST_RESPONSE, Response},
        },
        wire::Transfer,
    },
    headers,
};
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Code, Reason,
    client::{call::unary, config::Limits, session::Session},
    details::{Diagnostic, Fault},
    message::{Compression, Frame},
    metadata::Metadata,
    protocol::{Block, request, response, section},
};
use sark_h2::{ServerRole, client, conn::machine::Connection};
use sark_http::field::metadata::map::Owned;

#[test]
fn admits_plain_headers() {
    for encoding in [None, Some(b"identity".as_slice())] {
        let extra =
            encoding.map(|value| Owned::try_new(b"grpc-encoding", value).expect("encoding field"));
        let fields = headers::try_request(extra).expect("request fields");
        let head = request::Head::from_fields(&fields).expect("plain request");
        assert_eq!(head.path(), b"/service/method");
        assert_eq!(head.metadata().values(b"grpc-encoding").next(), None);

        let extra =
            encoding.map(|value| Owned::try_new(b"grpc-encoding", value).expect("encoding field"));
        let fields = headers::try_response(extra).expect("response fields");
        let head = response::Head::from_fields(&fields).expect("plain response");
        assert_eq!(head.metadata().values(b"grpc-encoding").next(), None);
    }
}

#[test]
fn rejects_unsupported_request_encoding() {
    let fields =
        headers::try_request([Owned::try_new(b"grpc-encoding", b"gzip").expect("encoding field")])
            .expect("request fields");
    let status = request::Head::from_fields(&fields)
        .err()
        .expect("unsupported request encoding");
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(matches!(
        Diagnostic::from(&status),
        Diagnostic::SectionStored(Fault::RequestEncoding)
    ));
}

#[test]
fn rejects_duplicate_encoding() {
    let fields = headers::try_request([
        Owned::try_new(b"grpc-encoding", b"identity").expect("first encoding"),
        Owned::try_new(b"grpc-encoding", b"identity").expect("second encoding"),
    ])
    .expect("request fields");
    let status = request::Head::from_fields(&fields)
        .err()
        .expect("one message encoding");
    assert!(matches!(
        Diagnostic::from(&status),
        Diagnostic::SectionStored(Fault::Duplicate)
    ));
}

#[test]
fn rejects_response_encoding_locally() {
    for boundary in [Boundary::More, Boundary::End] {
        Session::with(
            client::Plan::<section::Plan>::new(Limits::default().calls()).expect("client plan"),
            |mut client| {
                let calls = client.try_unary(2).expect("concurrent calls");
                let mut server = Connection::<ServerRole>::try_new().expect("server");
                let streams = Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_request()
                .expect("request transfer");
                let mut fields = Block::response(&Metadata::new()).expect("response fields");
                fields.push(b"grpc-encoding", b"gzip").expect("encoding");
                if matches!(boundary, Boundary::End) {
                    fields.push(b"grpc-status", b"0").expect("terminal status");
                }
                server
                    .streams()
                    .headers()
                    .respond(streams[0], &fields, boundary)
                    .expect("unsupported response encoding");
                Response::new(&mut server, streams[1])
                    .try_respond(FIRST_RESPONSE)
                    .expect("concurrent success");
                Transfer {
                    client: &mut client,
                    server: &mut server,
                }
                .try_response()
                .expect("encoding rejection is local to its call");

                let status = unary::Session::try_poll(&mut client, &calls[0])
                    .err()
                    .expect("unsupported response encoding");
                assert_eq!(status.code(), Code::Internal);
                assert!(matches!(
                    Diagnostic::from(&status),
                    Diagnostic::SectionStored(Fault::ResponseEncoding)
                ));
                let response = unary::Session::try_poll(&mut client, &calls[1])
                    .expect("concurrent call")
                    .expect("concurrent completion");
                assert_eq!(
                    response.into_payload().expect("plain payload").contiguous(),
                    Some(FIRST_RESPONSE)
                );
            },
        )
        .expect("client session");
    }
}

#[test]
fn admits_only_plain_response_frames() {
    for encoding in [None, Some(b"identity".as_slice())] {
        for compression in [Compression::Plain, Compression::Compressed] {
            Session::with(
                client::Plan::<section::Plan>::new(Limits::default().calls()).expect("client plan"),
                |mut client| {
                    let calls = client.try_unary(1).expect("call");
                    let mut server = Connection::<ServerRole>::try_new().expect("server");
                    let streams = Transfer {
                        client: &mut client,
                        server: &mut server,
                    }
                    .try_request()
                    .expect("request transfer");
                    let mut fields = Block::response(&Metadata::new()).expect("response fields");
                    if let Some(encoding) = encoding {
                        fields.push(b"grpc-encoding", encoding).expect("encoding");
                    }
                    server
                        .streams()
                        .headers()
                        .respond(streams[0], &fields, Boundary::More)
                        .expect("plain response head");
                    let mut frame = Vec::new();
                    Bytes::try_frame(compression, FIRST_RESPONSE, &mut frame).expect("frame");
                    let mut response = Response::new(&mut server, streams[0]);
                    response
                        .try_data(&frame, Boundary::More)
                        .expect("response message");
                    response.try_trailers(Some(Code::Ok)).expect("trailers");
                    Transfer {
                        client: &mut client,
                        server: &mut server,
                    }
                    .try_response()
                    .expect("frame rejection stays local");
                    match compression {
                        Compression::Plain => {
                            let response = unary::Session::try_poll(&mut client, &calls[0])
                                .expect("call")
                                .expect("completion");
                            let message: Frame<_> = response.message.expect("plain message");
                            assert_eq!(message.payload.contiguous(), Some(FIRST_RESPONSE));
                        }
                        Compression::Compressed => {
                            let failure = unary::Session::try_poll(&mut client, &calls[0])
                                .err()
                                .expect("compressed flag");
                            assert!(matches!(
                                Diagnostic::from(&failure),
                                Diagnostic::Local {
                                    code: Code::Internal,
                                    reason: Reason::Compression,
                                }
                            ));
                        }
                    }
                },
            )
            .expect("client session");
        }
    }
}
