use std::num::NonZero;

use dope_quic::errors::Code;
use sark_h3::conn::Error;
use sark_http::field::literal;

use crate::common::{
    conn::payload::Write as _,
    field::entry,
    ledger::Section as _,
    message::{
        Frame as _,
        capacity::Capacity as _,
        qpack::{Kind, Wire as _},
    },
};

pub mod common;

use dope_quic::wire::stream::Id;
use o3::buffer::{resident, storage};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection,
        event::{Event, Role},
        io::{Receive, Step},
        options::{Config, Decode, Egress, Ingress, Qpack},
    },
    frame::TYPE_HEADERS,
    link::{Handoff, Status},
    qpack::{decoder, encoder::Encoder},
};

#[test]
fn decoder_prefix_retains_wire_budget() {
    let id = Id::new(64).expect("request id");
    let code = Code::from_u16(42);
    let mut receiver = Connection::with_config(
        Role::Server,
        Config {
            egress: Egress {
                wire_bytes: 2,
                ..Egress::default()
            },
            ..Config::qpack()
        },
    )
    .expect("connection");
    assert!(matches!(
        Receive::new(&mut receiver).reset(id, None, code),
        Err((
            Error::Feedback(sark_h3::qpack::stream::Capacity {
                len: 0,
                additional: 2,
                limit: 1,
            }),
            _
        ))
    ));
    assert_eq!(receiver.wire_len(), 0);
    assert!(matches!(
        receiver.events().poll_token().expect("unchanged events"),
        Step::Idle
    ));
    receiver
        .control()
        .open_decoder(Id::new(3).expect("decoder id"))
        .expect("decoder stream");
    assert_eq!(
        receiver
            .events()
            .poll_write()
            .expect("decoder write")
            .expect("decoder prefix")
            .into_bytes(),
        [3]
    );
    assert!(Receive::new(&mut receiver).reset(id, None, code).is_ok());
    assert_eq!(
        receiver
            .events()
            .poll_write()
            .expect("decoder write")
            .expect("stream cancellation")
            .into_bytes(),
        [0x7f, 0x01]
    );
}

#[test]
fn feedback_exceeding_budget_is_rejected() {
    let id = Id::new(16384).expect("request id");
    let (block, wire) = Kind::Request.try_blocked(id).expect("dynamic request");
    let mut receiver = Connection::with_config(
        Role::Server,
        Config {
            egress: Egress {
                wire_bytes: 2,
                ..Egress::default()
            },
            ..Config::qpack()
        },
    )
    .expect("connection");
    receiver
        .control()
        .open_decoder(Id::new(3).expect("decoder id"))
        .expect("decoder stream");
    Receive::new(&mut receiver)
        .owned(
            Id::new(2).expect("encoder id"),
            wire.try_wire().expect("encoder instructions"),
            Boundary::More,
        )
        .expect("dynamic entry");
    receiver
        .events()
        .poll_write()
        .expect("decoder feedback")
        .expect("decoder feedback");
    assert_eq!(
        Receive::new(&mut receiver).owned(
            id,
            block.try_frame(TYPE_HEADERS).expect("HEADERS frame"),
            Boundary::More,
        ),
        Err(Error::BudgetWire {
            retained: 0,
            additional: 3,
            limit: 2,
        })
    );
}

#[test]
fn blocked_limit_is_exact() {
    let dynamic = literal!(entry(b"x-limit", b"retained"));
    let mut sender = Encoder::try_bounded(256, 8).expect("fixed H3 QPACK scenario must succeed");
    sender.set_blocked(2);
    sender
        .encode_for(8, [dynamic])
        .expect("fixed H3 QPACK scenario must succeed");
    assert!(!sender.take_wire().is_empty());
    let first = sender
        .encode_for(0, [dynamic])
        .expect("fixed H3 QPACK scenario must succeed");
    let second = sender
        .encode_for(4, [dynamic])
        .expect("fixed H3 QPACK scenario must succeed");
    assert!(first.is_dynamic());
    assert!(second.is_dynamic());

    let mut receiver = Connection::with_config(
        Role::Server,
        Config {
            qpack: Qpack {
                decoder: Decode {
                    table_bytes: 256,
                    blocked_streams: 1,
                },
                ..Qpack::default()
            },
            ..Config::default()
        },
    )
    .expect("fixed H3 QPACK scenario must succeed");
    Receive::new(&mut receiver)
        .owned(
            Id::new(0).expect("stream id"),
            first
                .into_wire()
                .try_frame(TYPE_HEADERS)
                .expect("HEADERS frame"),
            Boundary::More,
        )
        .expect("fixed H3 QPACK scenario must succeed");
    let error = Receive::new(&mut receiver)
        .owned(
            Id::new(4).expect("stream id"),
            second
                .into_wire()
                .try_frame(TYPE_HEADERS)
                .expect("HEADERS frame"),
            Boundary::More,
        )
        .expect_err("second blocked stream exceeds the configured limit");
    assert!(matches!(error,
        Error::Qpack(decoder::Error::BlockedLimit {
            stream_id,
            required,
            limit: 1,
        }) if stream_id == Id::new(4).expect("stream id").get() && required.required() == NonZero::<u64>::MIN
    ));
}

#[test]
fn reset_reuses_budget() {
    let mut receiver = Connection::with_config(
        Role::Server,
        Config {
            ingress: Ingress {
                retained_bytes: 1,
                ..Ingress::default()
            },
            ..Config::default()
        },
    )
    .expect("fixed H3 QPACK scenario must succeed");
    let partial = || {
        let mut bytes = Vec::new();
        TYPE_HEADERS.encode(&mut bytes).expect("varint");
        assert_eq!(bytes.len(), 1);
        storage::Shared::try_from(&mut bytes).map(resident::Shared::from)
    };

    let first = (partial()).expect("first partial header must retain its owner");
    assert!(matches!(
        Receive::new(&mut receiver).hold(Id::new(0).expect("stream id"), first, Boundary::More),
        Handoff::Accepted(Ok(Status::Open))
    ));
    let second = (partial()).expect("second partial header must retain its owner");
    assert!(matches!(
        Receive::new(&mut receiver).hold(Id::new(4).expect("stream id"), second, Boundary::More),
        Handoff::Rejected(Error::ReceiveCapacity {
            retained: 1,
            additional: 1,
            limit: 1,
        })
    ));

    let result = Receive::new(&mut receiver).reset(
        Id::new(0).expect("stream id"),
        None,
        Code::from_u16(0x51),
    );
    assert!(
        result.is_ok(),
        "reset must release the retained header owner"
    );
    assert!(matches!(
        receiver.events().poll_token().expect("reset event"),
        Step::Item(Event::Reset { stream_id, released: 1, .. }) if stream_id == Id::ZERO.into()
    ));
    let retry = (partial()).expect("retried partial header must retain its owner");
    assert!(matches!(
        Receive::new(&mut receiver).hold(Id::new(4).expect("stream id"), retry, Boundary::More),
        Handoff::Accepted(Ok(Status::Open))
    ));
}
