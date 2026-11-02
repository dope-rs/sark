use std::num::NonZero;

use common::{
    capacity::Fragments,
    hpack::{
        Fields, GET,
        stream::{Prime, WIDE_INDEXED},
    },
    protocol::{
        Session,
        rules::{Retained, Wire},
        session::Frame,
    },
};
use sark_h2::{conn::event::Event, frame::payload::Continuation, stream::Id};

use crate::common::headers::entry;
pub mod common;
use o3::buffer::storage::Shared;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        config::{Config, Validated},
        machine::Connection,
    },
    frame,
    frame::numbers::ErrorCode,
    hpack::encode::Encoder,
};
use sark_http::field::{
    self,
    metadata::{
        block,
        map::record::Entry,
        section::{rules::Discard, storage::Tag},
    },
};

use crate::common::protocol::rules::{Authority, Stable};

#[test]
fn survives_table_churn() {
    let mut conn = Connection::<ServerRole, Stable>::from_plan(
        Validated::new(Config::default()).expect("default H2 configuration must be valid"),
    )
    .expect("stable server plan accepts default configuration");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let mut encoder = Encoder::try_new(4096).expect("HPACK encoder capacity");

    let first = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"https"),
        Entry::new(b":authority", b"example.test"),
        Entry::new(b":path", b"/first"),
        Entry::new(b"x-name", b"first-owner"),
    ])
    .try_with(&mut encoder)
    .expect("HPACK table");
    conn.ingress()
        .ingest(
            &Frame::new(1, &first)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("protocol wire fixture"),
        )
        .expect("first HEADERS frame fits ingress");
    let event = conn.ingress().poll_event().expect("first retained head");
    let Event::Headers { head, .. } = event else {
        assert!(matches!(event, Event::Headers { .. }));
        return;
    };

    let (headers, selected, _) = head.into_parts();
    let (path_at, name_at) = selected.into_inner();

    for index in 1..96u32 {
        let path = format!("/churn/{index:03}/{}", "p".repeat(32));
        let name = format!("owner-{index:03}-{}", "n".repeat(32));
        let block = Fields::new(&[
            Entry::new(b":method", b"GET"),
            Entry::new(b":scheme", b"https"),
            Entry::new(b":authority", b"example.test"),
            Entry::new(b":path", path.as_bytes()),
            Entry::new(b"x-name", name.as_bytes()),
        ])
        .try_with(&mut encoder)
        .expect("HPACK table");
        conn.ingress()
            .ingest(
                &Frame::new(1 + index * 2, &block)
                    .try_headers(Boundary::End, frame::Block::End)
                    .expect("protocol wire fixture"),
            )
            .expect("churn HEADERS frame fits ingress");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Headers { .. })
        ));
    }

    let bytes = headers.as_bytes();
    assert_eq!(
        path_at.map(|range| &bytes[range]),
        Some(b"/first".as_slice())
    );
    assert_eq!(
        name_at.map(|range| &bytes[range]),
        Some(b"first-owner".as_slice())
    );
    assert_eq!(
        bytes.len(),
        Tag::PREFIX_LEN + b"/first".len() + Tag::PREFIX_LEN + b"first-owner".len(),
        "discarded authority must live only in decoder scratch",
    );
    let retained = Retained::new(&headers)
        .try_collect()
        .expect("protocol fixture");
    assert!(
        (retained)
            == (vec![
                (Tag::PATH, b"/first".as_slice()),
                (Tag::USER, b"first-owner".as_slice(),),
            ])
    );
}

#[test]
fn keeps_connect_authority() {
    let mut conn = Connection::<ServerRole, Stable>::from_plan(
        Validated::new(Config::default()).expect("default H2 configuration must be valid"),
    )
    .expect("stable server plan accepts default configuration");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let block = Fields::new(&[
        Entry::new(b":method", b"CONNECT"),
        Entry::new(b":authority", b"origin.example:443"),
    ])
    .try_encode()
    .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_headers(Boundary::More, frame::Block::End)
                .expect("protocol wire fixture"),
        )
        .expect("CONNECT HEADERS frame fits ingress");
    let event = conn.ingress().poll_event();
    let Some(Event::Headers { head, .. }) = event else {
        assert!(matches!(event, Some(Event::Headers { .. })));
        return;
    };
    let (headers, _, control) = head.into_parts();
    assert!((control.map(block::Plan::form)) == (Some(block::Form::Connect)));
    assert_eq!(
        headers.as_bytes().len(),
        Tag::PREFIX_LEN + b"origin.example:443".len()
    );
    let retained = Retained::new(&headers)
        .try_collect()
        .expect("protocol fixture");
    assert!((retained) == (vec![(Tag::AUTHORITY, b"origin.example:443".as_slice())]));
}

#[test]
fn survives_direct_splits() {
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"https"),
        Entry::new(b":authority", b"example.test"),
        Entry::new(b":path", b"/fragmented/direct/value"),
        Entry::new(b"x-name", b"stable-fragment-owner"),
    ])
    .encode_never();

    for split in 1..block.len() {
        let mut conn = Connection::<ServerRole, Stable>::from_plan(
            Validated::new(Config::default()).expect("default H2 configuration must be valid"),
        )
        .expect("stable server plan accepts default configuration");
        Session::new(&mut conn)
            .try_prime()
            .expect("protocol fixture");
        conn.ingress()
            .ingest(
                &Frame::new(1, &block[..split])
                    .try_headers(Boundary::End, frame::Block::More)
                    .expect("protocol wire fixture"),
            )
            .expect("partial HEADERS frame fits ingress");
        assert!(conn.ingress().poll_event().is_none(), "split {split}");
        let mut tail = Vec::new();
        Continuation::new(
            Id::new(1).expect("fixture domain"),
            frame::Block::End,
            &block[split..],
        )
        .expect("remaining HPACK bytes form a valid CONTINUATION")
        .encode(&mut tail)
        .expect("frame allocation");
        conn.ingress()
            .ingest(&tail)
            .expect("CONTINUATION frame fits ingress");
        let event = conn
            .ingress()
            .poll_event()
            .expect("completed continuation emits headers");
        let Event::Headers { head, .. } = event else {
            assert!(matches!(event, Event::Headers { .. }));
            return;
        };
        let (headers, _, _) = head.into_parts();
        let retained = Retained::new(&headers)
            .try_collect()
            .expect("protocol fixture");
        assert!(
            (retained)
                == (vec![
                    (Tag::PATH, b"/fragmented/direct/value".as_slice()),
                    (Tag::USER, b"stable-fragment-owner".as_slice(),),
                ]),
            "split {split}"
        );
    }
}

/// Retains the first head across complete compression-table turnovers.
#[test]
fn survives_indexed_splits() {
    let path = b"/fragmented/indexed/final-owner";
    let name = b"stable-indexed-owner";

    for split in 1.. {
        let mut conn = Connection::<ServerRole, Stable>::from_plan(
            Validated::new(Config::default()).expect("default H2 configuration must be valid"),
        )
        .expect("stable server plan accepts default configuration");
        Session::new(&mut conn)
            .try_prime()
            .expect("protocol fixture");
        let mut encoder = Encoder::try_new(4096).expect("HPACK encoder capacity");
        let block = Fields::new(&[
            Entry::new(b":method", b"GET"),
            Entry::new(b":scheme", b"https"),
            Entry::new(b":authority", b"example.test"),
            Entry::new(b":path", path),
            Entry::new(b"x-name", name),
        ])
        .try_with(&mut encoder)
        .expect("HPACK table");
        if split >= block.len() {
            break;
        }

        conn.ingress()
            .ingest(
                &Frame::new(1, &block[..split])
                    .try_headers(Boundary::End, frame::Block::More)
                    .expect("protocol wire fixture"),
            )
            .expect("partial indexed HEADERS frame fits ingress");
        assert!(conn.ingress().poll_event().is_none(), "split {split}");
        let mut tail = Vec::new();
        Continuation::new(
            Id::new(1).expect("fixture domain"),
            frame::Block::End,
            &block[split..],
        )
        .expect("remaining indexed bytes form a valid CONTINUATION")
        .encode(&mut tail)
        .expect("frame allocation");
        conn.ingress()
            .ingest(&tail)
            .expect("indexed CONTINUATION frame fits ingress");
        let event = conn
            .ingress()
            .poll_event()
            .expect("completed indexed continuation emits headers");
        let Event::Headers { head, .. } = event else {
            assert!(matches!(event, Event::Headers { .. }));
            return;
        };
        let (headers, _, _) = head.into_parts();

        for index in 1..72u32 {
            let path = format!("/turnover/{index:03}/{}", "p".repeat(48));
            let owner = format!("owner-{index:03}-{}", "n".repeat(48));
            let churn = Fields::new(&[
                Entry::new(b":method", b"GET"),
                Entry::new(b":scheme", b"https"),
                Entry::new(b":authority", b"example.test"),
                Entry::new(b":path", path.as_bytes()),
                Entry::new(b"x-name", owner.as_bytes()),
            ])
            .try_with(&mut encoder)
            .expect("HPACK table");
            conn.ingress()
                .ingest(
                    &Frame::new(1 + index * 2, &churn)
                        .try_headers(Boundary::End, frame::Block::End)
                        .expect("protocol wire fixture"),
                )
                .expect("table-churn HEADERS frame fits ingress");
            assert!(matches!(
                conn.ingress().poll_event(),
                Some(Event::Headers { .. })
            ));
        }

        let retained = Retained::new(&headers)
            .try_collect()
            .expect("protocol fixture");
        assert!(
            (retained) == (vec![(Tag::PATH, path.as_slice()), (Tag::USER, name.as_slice(),),]),
            "split {split}"
        );
    }
}

#[test]
fn retains_split_integer() {
    let mut conn = Connection::<ServerRole, Stable>::from_plan(
        Validated::new(Config::default()).expect("configuration"),
    )
    .expect("connection");
    Session::new(&mut conn).try_prime().expect("preface");
    let mut encoder = Encoder::try_new(4096).expect("encoder");
    let (warm, indexed) = encoder.try_wide().expect("HPACK wide index");
    assert_eq!(&indexed[indexed.len() - 2..], &[0xff, 0x00]);
    conn.ingress()
        .ingest(
            &Frame::new(1, &warm)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("warm headers"),
        )
        .expect("warm table");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::Headers { .. })
    ));

    for (stream, chunk) in [(3, indexed.len()), (5, indexed.len() - 1)] {
        for wire in Fragments::new(Id::new(stream).expect("stream"), Boundary::End, &indexed)
            .try_split(NonZero::new(chunk).expect("chunk"))
            .expect("frames")
        {
            conn.ingress().ingest(&wire).expect("complete integer");
        }
        let head = match conn.ingress().poll_event().expect("retained head") {
            Event::Headers { head, .. } => Some(head),
            _ => None,
        }
        .expect("retained headers");
        let (headers, _, _) = head.into_parts();
        assert_eq!(
            Retained::new(&headers).try_collect().expect("fields"),
            [(Tag::PATH, GET[2].value), (Tag::USER, WIDE_INDEXED.value)],
        );
    }
}

#[test]
fn validates_push_method() {
    let mut conn = Connection::<ClientRole, Discard>::from_plan(
        Validated::new(Config::default()).expect("default H2 configuration must be valid"),
    )
    .expect("discarding client plan accepts default configuration");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("client request stream opens");
    conn.transfer().drain(usize::MAX);
    let block = Fields::new(&[
        Entry::new(b":method", b"POST"),
        Entry::new(b":scheme", b"https"),
        Entry::new(b":path", b"/unsafe"),
        Entry::new(b":authority", b"example.test"),
    ])
    .try_encode()
    .expect("protocol fixture");

    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_push(2, frame::Block::End)
                .expect("protocol wire fixture"),
        )
        .expect("invalid-method PUSH_PROMISE fits ingress");

    assert!(conn.ingress().poll_event().is_none());
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol wire fixture"))
            == (Some((
                Id::new(2).expect("fixture domain"),
                ErrorCode::ProtocolError
            )))
    );
}

#[test]
fn validates_push_authority() {
    let mut conn = Connection::<ClientRole, Discard, Shared, Authority>::from_plan(
        Validated::new(Config::default()).expect("default H2 configuration must be valid"),
    )
    .expect("authority-checking client plan accepts default configuration");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("client request stream opens");
    conn.transfer().drain(usize::MAX);
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"https"),
        Entry::new(b":path", b"/foreign"),
        Entry::new(b":authority", b"foreign.example"),
    ])
    .try_encode()
    .expect("protocol fixture");

    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_push(2, frame::Block::End)
                .expect("protocol wire fixture"),
        )
        .expect("foreign-authority PUSH_PROMISE fits ingress");

    assert!(conn.ingress().poll_event().is_none());
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol wire fixture"))
            == (Some((
                Id::new(2).expect("fixture domain"),
                ErrorCode::ProtocolError
            )))
    );
    assert!(
        !conn
            .status()
            .has_stream(Id::new(2).expect("fixture domain"))
    );
}
