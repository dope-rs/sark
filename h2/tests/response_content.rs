pub mod common;

use std::{cell::Cell, num::NonZero, pin::Pin};

use common::{
    INITIAL_SETTINGS,
    capacity::Fragments,
    headers::TEXT,
    hpack::{Encoded, Fields},
    protocol::{Session, session::Frame},
    response::{Body, CONNECT, HEAD},
};
use o3::{base::cell::region, buffer::resident};
use sark_core::manifold::{schedule, stream::Boundary};
use sark_h2::{
    conn::{CLIENT_PREFACE, Error, config::Config, event::Event},
    frame::{Block, layout::Header, numbers::Flags, payload::Payload},
    hpack::decode::Decoder,
    server::response::Encoder,
    stream::Id,
};
use sark_http::{
    payload::response::{dispatch::Accept, status::Code},
    request::body::Ref,
};

#[test]
fn suppress_content() {
    for (head, status) in [
        (true, Code::OK),
        (false, Code::NO_CONTENT),
        (false, Code::NOT_MODIFIED),
        (false, Code::RESET_CONTENT),
    ] {
        let mut domain = resident::Domain::new();
        region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
            let budget = domain
                .get_mut()
                .try_budget(Config::default().inbound_capacity, token.proof())
                .expect("ingress capacity");
            let (mut server, stream) = if head {
                Session::try_fields(budget, HEAD, Boundary::End)
            } else {
                Session::try_request(budget)
            }
            .expect("request connection");
            let encoder = Encoder::new(&mut server, stream, Ref::from_slice(&[]));
            let prepared = Accept::fixed(encoder, status, [TEXT], b"response".to_vec())
                .expect("response head");
            assert!(prepared.finish().expect("commit response").is_none());

            let wire = server.output().first();
            let header = Header::parse(wire).expect("response frame header");
            assert!(header.flags.contains(Flags::END_STREAM));
            let (frame, consumed) = Payload::parse(wire).expect("response frame");
            let headers = match frame {
                Payload::Headers(headers) => Some(headers),
                _ => None,
            }
            .expect("response must contain HEADERS");
            assert_eq!(consumed, server.output().len(), "no DATA after HEADERS");
            let (_, fields) = Encoded::new(headers.block())
                .try_collect(Decoder::try_new(4096).expect("HPACK decoder"))
                .expect("response metadata");
            assert!(fields.iter().any(|(name, value)| {
                name == b":status" && value.as_slice() == status.as_bytes()
            }));
            assert!(fields.iter().any(|(name, value)| {
                name == TEXT.name() && value.as_slice() == TEXT.value().as_bytes()
            }));
            assert!(!server.status().has_key(stream));
        });
    }
}

#[test]
fn skip_head_encoding() {
    for bounded in [false, true] {
        let mut domain = resident::Domain::new();
        region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
            let budget = domain
                .get_mut()
                .try_budget(Config::default().inbound_capacity, token.proof())
                .expect("ingress capacity");
            let (mut server, stream) = if bounded {
                Session::try_fields(budget, HEAD, Boundary::End)
            } else {
                let block = Fields::new(&HEAD.map(|field| field.as_record()))
                    .try_encode()
                    .expect("HEAD block");
                let frames =
                    Fragments::new(Id::new(1).expect("request stream"), Boundary::End, &block)
                        .try_split(NonZero::<usize>::MIN)
                        .expect("HEADERS and CONTINUATION frames");
                Session::try_input(
                    budget,
                    [CLIENT_PREFACE, INITIAL_SETTINGS.as_slice()]
                        .into_iter()
                        .chain(frames.iter().map(Vec::as_slice)),
                )
            }
            .expect("HEAD request");
            let encoded = Cell::new(0);
            let dropped = Cell::new(0);
            let body = Body {
                encoded: &encoded,
                dropped: &dropped,
            };
            let encoder = Encoder::new(&mut server, stream, Ref::from_slice(&[]));
            let prepared = if bounded {
                Accept::bounded(encoder, Code::OK, (), body)
            } else {
                Accept::exact(encoder, Code::OK, (), body)
            }
            .expect("HEAD response");
            assert_eq!(encoded.get(), 0);
            assert_eq!(dropped.get(), 1);
            assert!(prepared.finish().expect("commit HEAD response").is_none());
        });
    }
}

#[test]
fn requires_request_head() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(Config::default().inbound_capacity, token.proof())
            .expect("ingress capacity");
        let (mut server, _) = Session::try_request(budget).expect("request connection");
        let block = Fields::new(&HEAD.map(|field| field.as_record()))
            .try_encode()
            .expect("HEAD block");
        server
            .ingress()
            .ingest(
                &Frame::new(3, &block)
                    .try_headers(Boundary::End, Block::More)
                    .expect("pending request"),
            )
            .expect("request awaits CONTINUATION");
        let stream = server
            .status()
            .stream_key(Id::new(3).expect("stream id"))
            .expect("pending stream key");
        let encoded = Cell::new(0);
        let dropped = Cell::new(0);
        let encoder = Encoder::new(&mut server, stream, Ref::from_slice(&[]));
        assert!(matches!(
            Accept::exact(
                encoder,
                Code::OK,
                (),
                Body { encoded: &encoded, dropped: &dropped },
            ),
            Err((Error::InactiveStream(key), returned)) if key == stream && returned == stream
        ));
        assert_eq!((encoded.get(), dropped.get()), (0, 1));
        assert!(server.output().is_empty());
        server
            .ingress()
            .ingest(
                &Frame::new(3, &[])
                    .try_continue(Block::End)
                    .expect("completed request"),
            )
            .expect("request remains admissible");
        let encoder = Encoder::new(&mut server, stream, Ref::from_slice(&[]));
        assert!(
            Accept::fixed(encoder, Code::OK, (), b"suppressed".as_slice())
                .expect("admitted HEAD response")
                .finish()
                .expect("commit response")
                .is_none()
        );
    });
}

#[test]
fn reject_informational() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(Config::default().inbound_capacity, token.proof())
            .expect("ingress capacity");
        let (mut server, stream) = Session::try_request(budget).expect("request connection");
        let encoder = Encoder::new(&mut server, stream, Ref::from_slice(&[]));
        assert!(matches!(
            Accept::fixed(encoder, Code::EARLY_HINTS, (), b"body".to_vec()),
            Err((Error::Informational(Code::EARLY_HINTS), key)) if key == stream
        ));
        assert!(server.output().is_empty());
        assert!(server.status().has_key(stream));

        let encoder = Encoder::new(&mut server, stream, Ref::from_slice(&[]));
        let prepared = Accept::fixed(encoder, Code::NO_CONTENT, (), [])
            .expect("rejection leaves no prepared head");
        assert!(prepared.finish().expect("commit final response").is_none());
    });
}

#[test]
fn connect_duplex() {
    for status in [Code::OK, Code::NO_CONTENT, Code::RESET_CONTENT] {
        let mut domain = resident::Domain::new();
        region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
            let budget = domain
                .get_mut()
                .try_budget(Config::default().inbound_capacity, token.proof())
                .expect("ingress capacity");
            let (mut server, stream) =
                Session::try_fields(budget, CONNECT, Boundary::More).expect("CONNECT request");
            let encoder = Encoder::new(&mut server, stream, Ref::from_slice(&[]));
            let prepared = Accept::fixed(encoder, status, (), b"tunnel".to_vec())
                .expect("successful CONNECT response");
            let mut pending = prepared
                .finish()
                .expect("commit CONNECT response")
                .expect("successful CONNECT retains tunnel bytes");
            let header = Header::parse(server.output().first()).expect("response frame header");
            assert!(!header.flags.contains(Flags::END_STREAM));
            server.transfer().drain(usize::MAX);

            assert!(matches!(
                pending.turn(&mut server, stream),
                Ok(schedule::Turn::Complete)
            ));
            let (frame, consumed) = Payload::parse(server.output().first()).expect("tunnel DATA");
            assert!(matches!(
                frame,
                Payload::Data(data)
                    if data.payload() == b"tunnel" && data.boundary() == Boundary::End
            ));
            assert_eq!(consumed, server.output().len());
            assert!(
                server.status().has_key(stream),
                "peer can still send tunnel DATA"
            );

            let wire = Frame::new(stream.id().into_u32(), b"inbound")
                .try_data(Boundary::End)
                .expect("peer tunnel DATA");
            server
                .ingress()
                .ingest(&wire)
                .expect("peer half remains open");
            assert!(matches!(
                server.ingress().poll_event(),
                Some(Event::Data {
                    data,
                    boundary: Boundary::End,
                    ..
                }) if data.contiguous() == Some(b"inbound".as_slice())
            ));
            assert!(!server.status().has_key(stream));
        });
    }
}
