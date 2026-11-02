pub mod common;

use std::pin::Pin;

use common::protocol::Session;
use o3::{base::cell::region, buffer::resident};
use sark_core::manifold::{schedule, stream::Boundary};
use sark_h2::{
    conn::{DataBlock, Error, config::Config},
    frame::numbers::ErrorCode,
    server::response,
};
use sark_http::{
    field::{self, Entry, Pseudo, Value},
    payload::response::{dispatch::Accept, headers::set::Set, status::Code},
    request::body::Ref,
};

#[test]
fn discards_partial_head() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(Config::default().inbound_capacity, token.proof())
            .expect("ingress capacity");
        let (mut server, stream) = Session::try_request(budget).expect("request connection");
        let encoder = response::Encoder::new(&mut server, stream, Ref::from_slice(&[]));
        let prepared = Accept::fixed(encoder, Code::OK, Set::<'static, 0>::new(), Vec::new());
        let prepared = prepared.expect("first response must prepare");
        drop(prepared);

        assert_eq!(server.output().len(), 0);
        assert!(server.status().has_key(stream));

        let result = server.streams().headers().respond_key(
            stream,
            [Entry::pseudo(
                Pseudo::Status,
                field::literal!(Value::try_bytes(b"204")),
            )],
            Boundary::End,
        );
        assert!(
            result.is_ok(),
            "discarded HPACK transaction restores the encoder"
        );
        assert_ne!(server.output().len(), 0);
    });
}

#[test]
fn commits_head_body() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(Config::default().inbound_capacity, token.proof())
            .expect("ingress capacity");
        let (mut server, stream) = Session::try_request(budget).expect("request connection");
        let encoder = response::Encoder::new(&mut server, stream, Ref::from_slice(&[]));
        let prepared = Accept::fixed(
            encoder,
            Code::OK,
            Set::<'static, 0>::new(),
            b"response".to_vec(),
        );
        let prepared = prepared.expect("response must prepare");
        let pending = prepared.finish();
        let mut pending = pending
            .expect("atomic response commit")
            .expect("response body remains pending");

        assert_ne!(server.output().len(), 0, "HEADERS committed once");
        loop {
            if let Err(error) = server.control().ping([0; 8]) {
                assert!(error.is_pressure());
                break;
            }
        }
        let mut next = pending.turn(&mut server, stream).expect("drive body");
        if matches!(next, schedule::Turn::Progress) {
            next = pending.turn(&mut server, stream).expect("remaining body");
        }
        assert!(matches!(next, schedule::Turn::Blocked(DataBlock::Egress)));
        let before = (
            pending.retention(),
            server.output().len(),
            server.status().send_window(),
            server.status().send_for(stream.id()),
        );
        assert!(matches!(
            pending.turn(&mut server, stream),
            Ok(schedule::Turn::Blocked(DataBlock::Egress))
        ));
        assert!(
            before
                == (
                    pending.retention(),
                    server.output().len(),
                    server.status().send_window(),
                    server.status().send_for(stream.id()),
                )
        );
        server.transfer().drain(usize::MAX);
        assert!(matches!(
            pending.turn(&mut server, stream),
            Ok(schedule::Turn::Complete)
        ));
        assert!(!server.status().has_key(stream));
    });
}

#[test]
fn restores_owner() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(Config::default().inbound_capacity, token.proof())
            .expect("ingress capacity");
        let (mut server, stream) = Session::try_request(budget).expect("request connection");
        let encoder = response::Encoder::new(&mut server, stream, Ref::from_slice(&[]));
        let prepared = Accept::fixed(
            encoder,
            Code::OK,
            Set::<'static, 0>::new(),
            b"retained-owner".to_vec(),
        );
        let prepared = prepared.expect("response must prepare");
        let pending = prepared.finish();
        let mut pending = pending
            .expect("atomic response commit")
            .expect("response body remains pending");
        let retained = pending.retention();
        server.streams().cancel(stream, ErrorCode::Cancel);

        assert!(matches!(
            pending.turn(&mut server, stream),
            Err(Error::InactiveStream(found)) if found == stream
        ));
        assert!(pending.retention() == retained);
    });
}
