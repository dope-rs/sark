use common::{
    hpack::{FIRST_DYNAMIC, Fields, GET, INDEXED_FIELDS, INDEXED_NAMES},
    protocol::{
        Session,
        conn::{Control, Controls},
        session::{Frame, Wire},
    },
};
use sark_h2::{
    conn::Continuation,
    frame::numbers::{Length, Type},
};
use sark_http::field;

use crate::common::headers::entry;

pub mod common;

use sark_core::manifold::{
    client::event::{self, Classify as _},
    stream::Boundary,
};
use sark_h2::{
    ServerRole,
    conn::{Error, event::Event, machine::Connection},
    frame,
    frame::{
        Ack,
        layout::Header,
        numbers::{ErrorCode, Flags},
    },
    stream::Id,
};

#[test]
fn rapid_reset_is_distinct() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    let mut id = 1u32;
    let mut hit = false;
    for _ in 0..200 {
        let mut bytes = Frame::new(id, &block)
            .try_headers(Boundary::More, frame::Block::End)
            .expect("protocol fixture");
        bytes.extend_from_slice(
            &Control::new(id)
                .try_reset(ErrorCode::Cancel)
                .expect("protocol fixture"),
        );
        match conn.ingress().ingest(&bytes) {
            Ok(()) => {
                while conn.ingress().poll_event().is_some() {}
                conn.transfer().drain(usize::MAX);
            }
            Err(e) => {
                assert!(
                    e == Error::RapidReset {
                        actual: 101,
                        limit: 100,
                    }
                );
                hit = true;
                break;
            }
        }
        id += 2;
    }
    assert!(hit, "rapid HEADERS+RST flood must trigger RapidReset");
}

#[test]
fn ping_flood_is_bounded() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.transfer().drain(usize::MAX);
    let ping = Controls::new(Ack::Required)
        .try_ping([1, 2, 3, 4, 5, 6, 7, 8])
        .expect("ping fixture");
    let mut hit = false;
    for _ in 0..400_000 {
        match conn.ingress().ingest(&ping) {
            Ok(()) => while conn.ingress().poll_event().is_some() {},
            Err(e) => {
                assert!(
                    matches!(e, Error::OutputAppend(cause)
                    if cause.into_parts().0.get() == 17 && cause.into_parts().1 == 16),
                    "{e:?}"
                );
                hit = true;
                break;
            }
        }
    }
    assert!(
        hit,
        "PING flood with a non-draining peer must report output pressure"
    );
    assert!(
        conn.output().len() <= 1 << 20,
        "outbound must stay bounded, got {}",
        conn.output().len()
    );
}

#[test]
fn completions_stay_bounded() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    let mut id = 1u32;
    for _ in 0..1000 {
        conn.ingress()
            .ingest(
                &Frame::new(id, &block)
                    .try_headers(Boundary::End, frame::Block::End)
                    .expect("protocol fixture"),
            )
            .expect("complete request HEADERS fit ingress");
        while conn.ingress().poll_event().is_some() {}
        conn.streams()
            .headers()
            .respond(
                Id::new(id).expect("fixture domain"),
                [field::literal!(entry(b":status", b"200"))],
                Boundary::End,
            )
            .expect("response HEADERS fit egress");
        conn.transfer().drain(usize::MAX);
        id += 2;
    }
    assert_eq!(conn.status().active_len(), 0);
}

#[test]
fn late_frames_survive_many_resets() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn).try_prime().expect("primed server");
    let block = Fields::new(&GET).try_encode().expect("request headers");
    for id in (1..=513).step_by(2) {
        conn.ingress()
            .ingest(
                &Frame::new(id, &block)
                    .try_headers(Boundary::More, frame::Block::End)
                    .expect("request frame"),
            )
            .expect("admitted request");
        let stream = match conn.ingress().poll_event() {
            Some(Event::Headers { stream_id, .. }) => stream_id,
            _ => panic!("expected request HEADERS"),
        };
        conn.streams()
            .try_cancel(stream, ErrorCode::Cancel)
            .expect("active stream cancellation");
        conn.transfer().drain(usize::MAX);
    }
    assert_eq!(conn.status().active_len(), 0);

    let window = conn.status().recv_window().value();
    conn.ingress()
        .ingest(
            &Frame::new(1, b"x")
                .try_padded(Boundary::More, 7)
                .expect("late padded DATA"),
        )
        .expect("closed DATA keeps connection flow control");
    assert_eq!(conn.status().recv_window().value(), window - 9);
    assert_eq!(
        Wire::new(conn.output().first())
            .try_reset()
            .expect("reset frame"),
        Some((Id::FIRST_CLIENT, ErrorCode::StreamClosed))
    );
    conn.transfer().drain(usize::MAX);

    conn.ingress()
        .ingest(
            &Frame::new(1, &INDEXED_NAMES[..2])
                .try_headers(Boundary::End, frame::Block::More)
                .expect("late HEADERS"),
        )
        .expect("closed HEADERS keep HPACK state");
    conn.ingress()
        .ingest(
            &Frame::new(1, &INDEXED_NAMES[2..])
                .try_continue(frame::Block::End)
                .expect("late CONTINUATION"),
        )
        .expect("closed CONTINUATION updates HPACK table");
    assert!(conn.ingress().poll_event().is_none());
    assert_eq!(
        Wire::new(conn.output().first())
            .try_reset()
            .expect("reset frame"),
        Some((Id::FIRST_CLIENT, ErrorCode::StreamClosed))
    );

    let mut block = Fields::new(&GET[..3])
        .try_encode()
        .expect("request pseudo headers");
    block.extend_from_slice(FIRST_DYNAMIC);
    conn.ingress()
        .ingest(
            &Frame::new(515, &block)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("indexed request frame"),
        )
        .expect("later request uses the closed stream's HPACK update");
    let head = match conn.ingress().poll_event() {
        Some(Event::Headers { head, .. }) => head.into_parts().0,
        _ => panic!("expected request HEADERS"),
    };
    assert!(head.iter().any(|field| {
        field.name == INDEXED_FIELDS[2].name && field.value == INDEXED_FIELDS[2].value
    }));
}

#[test]
fn rejects_declared_size() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let mut hdr = Vec::new();
    hdr.extend_from_slice(
        &Header {
            length: Length::new(16_777_000).expect("fixture domain"),
            kind: Type::Data,
            flags: Flags(0),
            stream_id: Id::new(1).expect("fixture domain"),
        }
        .wire_bytes(),
    );
    assert!(matches!(
        conn.ingress().ingest(&hdr),
        Err(Error::FrameLimit {
            kind: 0,
            actual: 16_777_000,
            limit: 16_384,
        })
    ));
}

#[test]
fn rejects_unknown_size() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let big: u32 = 16_777_000;
    let mut hdr = vec![(big >> 16) as u8, (big >> 8) as u8, big as u8, 0xFF, 0x00];
    hdr.extend_from_slice(&0u32.to_be_bytes());
    assert!(matches!(
        conn.ingress().ingest(&hdr),
        Err(Error::FrameLimit {
            kind: 0xff,
            actual: 16_777_000,
            limit: 16_384,
        })
    ));
}

#[test]
fn rejects_empty_followup() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_headers(Boundary::More, frame::Block::More)
                .expect("protocol fixture"),
        )
        .expect("open header block fits ingress");
    assert!(matches!(
        conn.ingress()
            .ingest(&Frame::new(1, &[]).try_continue(frame::Block::More).expect("protocol fixture")),
        Err(Error::Continuation(Continuation::Empty(id))) if id == Id::new(1).expect("fixture domain")
    ));
}

#[test]
fn caps_continuations() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &[])
                .try_headers(Boundary::More, frame::Block::More)
                .expect("protocol fixture"),
        )
        .expect("open empty header block fits ingress");
    let frame = Frame::new(1, &[0x00])
        .try_continue(frame::Block::More)
        .expect("protocol fixture");
    for _ in 0..64 {
        conn.ingress()
            .ingest(&frame)
            .expect("continuation within limit");
    }
    let error = conn
        .ingress()
        .ingest(&frame)
        .expect_err("continuation limit is terminal");
    assert_eq!(
        error,
        Error::Continuation(Continuation::Limit {
            actual: 65,
            limit: 64,
        })
    );
    assert!(!error.is_pressure());
    assert_eq!(error.kind(), event::Error::Capacity);
    assert_eq!(ErrorCode::from(&error), ErrorCode::EnhanceYourCalm);
    assert!(!conn.ingress().is_blocked());
}

#[test]
fn default_caps_streams() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    assert_eq!(conn.settings().local().max_concurrent_streams, Some(256));
    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    let mut id = 1u32;
    for _ in 0..256 {
        conn.ingress()
            .ingest(
                &Frame::new(id, &block)
                    .try_headers(Boundary::More, frame::Block::End)
                    .expect("protocol fixture"),
            )
            .expect("admitted stream HEADERS fit ingress");
        while conn.ingress().poll_event().is_some() {}
        id += 2;
    }
    assert_eq!(conn.status().active_len(), 256);
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(
            &Frame::new(id, &block)
                .try_headers(Boundary::More, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("refused stream HEADERS fit ingress");
    let rst = Wire::new(conn.output().first())
        .try_reset()
        .expect("protocol fixture")
        .expect("RST emitted for refused stream");
    assert!((rst.1) == (ErrorCode::RefusedStream));
    assert_eq!(conn.status().active_len(), 256);
}
