pub mod common;

use std::num::NonZero;

use common::{
    capacity::Fragments,
    hpack::{Fields, MALFORMED_DROPPED},
    protocol::{Session, rules::Stable},
};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ServerRole,
    conn::{
        config::{Config, Validated},
        machine::Connection,
    },
    hpack::encode::{Encoder, Mode},
    stream::Id,
};

#[test]
fn validates_dropped_value() {
    for (mode, capacity, split) in [
        (Mode::Plain, 0, false),
        (Mode::Shortest, 0, true),
        (Mode::Plain, 4096, false),
    ] {
        let mut conn = Connection::<ServerRole, Stable>::from_plan(
            Validated::new(Config::default()).expect("configuration"),
        )
        .expect("connection");
        Session::new(&mut conn).try_prime().expect("preface");
        let mut encoder = Encoder::try_new(capacity).expect("encoder");
        encoder.set_mode(mode);
        for number in [1, 3] {
            let id = Id::new(number).expect("stream");
            let block = Fields::new(&MALFORMED_DROPPED)
                .try_with(&mut encoder)
                .expect("HPACK table");
            let chunk = if split { 1 } else { block.len() };
            for wire in Fragments::new(id, Boundary::End, &block)
                .try_split(NonZero::new(chunk).expect("fragment"))
                .expect("frames")
            {
                conn.ingress()
                    .ingest(&wire)
                    .expect("discard malformed request");
            }
            assert!(!conn.status().has_stream(id));
            assert!(conn.ingress().poll_event().is_none());
        }
    }
}

#[test]
fn validates_indexed_value() {
    let mut conn = Connection::<ServerRole>::try_new().expect("connection");
    Session::new(&mut conn).try_prime().expect("preface");
    let mut encoder = Encoder::try_new(4096).expect("encoder");
    for number in [1, 3] {
        let id = Id::new(number).expect("stream");
        let block = Fields::new(&MALFORMED_DROPPED)
            .try_with(&mut encoder)
            .expect("HPACK table");
        for wire in Fragments::new(id, Boundary::End, &block)
            .try_split(NonZero::new(block.len()).expect("block"))
            .expect("frame")
        {
            conn.ingress()
                .ingest(&wire)
                .expect("reject malformed retained value");
        }
        assert!(!conn.status().has_stream(id));
        assert!(conn.ingress().poll_event().is_none());
    }
}
