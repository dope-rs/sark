use common::{
    hpack::{Fields, GET},
    protocol::{
        self, Session,
        session::{Frame, Wire},
    },
};

pub mod common;

use sark_h2::{
    ClientRole,
    conn::{Association, Error, Protocol},
    frame,
    frame::numbers::ErrorCode,
    stream::Id,
};
use sark_http::field::metadata::map::record::Entry;

use crate::common::protocol::session::Authority;

#[test]
fn push_requires_policy() {
    let mut conn = protocol::try_connection::<ClientRole>().expect("protocol fixture");
    Session::new(&mut conn)
        .try_start()
        .expect("protocol scenario precondition");

    conn.ingress()
        .ingest(
            &Frame::new(
                1,
                &Fields::new(&GET).try_encode().expect("protocol fixture"),
            )
            .try_push(2, frame::Block::End)
            .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");

    assert!(conn.ingress().poll_event().is_none());
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"))
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

#[test]
fn rejects_invalid_promises() {
    let promised = Id::new(2).expect("fixture domain");
    for (case, fields) in [
        (
            "rejects_unsafe_push",
            Fields::new(&[
                Entry::new(b":method", b"POST"),
                Entry::new(b":scheme", b"http"),
                Entry::new(b":path", b"/unsafe"),
                Entry::new(b":authority", b"x"),
            ]),
        ),
        (
            "requires_push_authority",
            Fields::new(&[
                Entry::new(b":method", b"GET"),
                Entry::new(b":scheme", b"http"),
                Entry::new(b":path", b"/missing-authority"),
            ]),
        ),
        (
            "enforces_push_origin",
            Fields::new(&[
                Entry::new(b":method", b"GET"),
                Entry::new(b":scheme", b"http"),
                Entry::new(b":path", b"/foreign"),
                Entry::new(b":authority", b"foreign.example"),
            ]),
        ),
    ] {
        let mut conn = protocol::try_push::<Authority>().expect(case);
        Session::new(&mut conn).try_start().expect(case);
        let block = fields.try_encode().expect(case);
        conn.ingress()
            .ingest(
                &Frame::new(1, &block)
                    .try_push(2, frame::Block::End)
                    .expect(case),
            )
            .expect(case);

        assert!(conn.ingress().poll_event().is_none(), "{case}");
        assert_eq!(
            Wire::new(conn.output().first()).try_reset().expect(case),
            Some((promised, ErrorCode::ProtocolError)),
            "{case}"
        );
        assert!(!conn.status().has_stream(promised), "{case}");
    }
}

#[test]
fn rejects_push_on_idle() {
    let mut conn = protocol::try_push::<Authority>().expect("protocol fixture");

    assert!(matches!(
        conn.ingress()
            .ingest(&Frame::new(1, &Fields::new(&GET).try_encode().expect("protocol fixture")).try_push(2, frame::Block::End).expect("protocol fixture")),
        Err(Error::Protocol(Protocol::Association(Association::Idle(id)))) if id == Id::new(1).expect("fixture domain")
    ));
    assert!(
        !conn
            .status()
            .has_stream(Id::new(2).expect("fixture domain"))
    );
    assert!(conn.ingress().poll_event().is_none());
}
