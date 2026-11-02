use common::protocol::conn::{Control, Controls};
use sark_h2::{conn::event::Event, stream::Id};

use crate::common::{INITIAL_SETTINGS, protocol::preface::Preface};
pub mod common;

use sark_h2::{
    ClientRole, ServerRole,
    conn::machine::Connection,
    frame,
    frame::{
        Ack,
        layout::Header,
        numbers::{ErrorCode, Flags},
    },
};

#[test]
fn replies_to_ping() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_server(&mut conn)
        .expect("connection preface");

    let opaque = [1u8, 2, 3, 4, 5, 6, 7, 8];
    let ping = Controls::new(Ack::Required)
        .try_ping(opaque)
        .expect("ping fixture");
    conn.ingress()
        .ingest(&ping)
        .expect("valid protocol fixture must be accepted");
    let out = conn.output().first();
    let h = Header::parse(out).expect("valid protocol fixture must be accepted");
    assert!((h.kind) == (frame::numbers::Type::Ping));
    assert!(h.flags.contains(Flags::ACK));
    let pong = &out[9..9 + 8];
    assert_eq!(pong, &opaque);
    assert!(matches!(
    conn.ingress()
                .poll_event(),
    Some(Event::Ping {
                ack: Ack::Required,
                opaque: actual,
            }) if actual == opaque
    ));
}

#[test]
fn accepts_ping_ack() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_server(&mut conn)
        .expect("connection preface");

    let opaque = [9u8; 8];
    let ack = Controls::new(Ack::Complete)
        .try_ping(opaque)
        .expect("ping fixture");
    conn.ingress()
        .ingest(&ack)
        .expect("valid protocol fixture must be accepted");
    assert!(conn.output().first().is_empty());
    assert!(matches!(
    conn.ingress()
                .poll_event(),
    Some(Event::Ping {
                ack: Ack::Complete,
                opaque: actual,
            }) if actual == opaque
    ));
}

#[test]
fn emits_goaway() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);
    conn.control().goaway(ErrorCode::ProtocolError);
    let out = conn.output().first();
    let h = Header::parse(out).expect("valid protocol fixture must be accepted");
    assert!((h.kind) == (frame::numbers::Type::GoAway));
    assert!(conn.status().is_goaway());
    assert!(!conn.status().is_draining());
}

#[test]
fn receives_goaway() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_server(&mut conn)
        .expect("connection preface");

    let goaway = Control::new(7)
        .try_goaway(ErrorCode::InternalError, b"down")
        .expect("protocol fixture");
    conn.ingress()
        .ingest(&goaway)
        .expect("valid protocol fixture must be accepted");
    assert!((conn.status().recv_goaway()) == (Some(ErrorCode::InternalError)));
    let ev = conn
        .ingress()
        .poll_event()
        .expect("valid protocol fixture must be accepted");
    let (last, error, debug) = match ev {
        Event::GoAway {
            last_stream_id: last,
            error,
            debug,
        } => Some((last, error, debug)),
        _ => None,
    }
    .expect("fixture must emit GoAway");
    assert!((last) == (Id::new(7).expect("fixture domain")));
    assert!((error) == (ErrorCode::InternalError));
    assert!(
        debug
            .segments()
            .flat_map(|part| part.as_ref().iter())
            .eq(b"down".iter())
    );
}

#[test]
fn drain_advances() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    let total = conn.output().first().len();
    assert!(total > 0);
    conn.transfer().drain(5);
    assert_eq!(conn.output().first().len(), total - 5);
    conn.transfer().drain(usize::MAX);
    assert!(conn.output().first().is_empty());
}

#[test]
fn ping_appends_frame() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    let before = conn.output().first().len();
    conn.control()
        .ping([7u8; 8])
        .expect("valid protocol fixture must be accepted");
    let after = conn.output().first();
    assert!(after.len() > before);
    let wire = &after[before..];
    let h = Header::parse(wire).expect("valid protocol fixture must be accepted");
    assert!((h.kind) == (frame::numbers::Type::Ping));
    assert_eq!(h.flags.0, 0);
}

#[test]
fn client_replies_to_ping() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_client(&mut conn)
        .expect("connection preface");

    let opaque = [42u8; 8];
    let ping = Controls::new(Ack::Required)
        .try_ping(opaque)
        .expect("ping fixture");
    conn.ingress()
        .ingest(&ping)
        .expect("valid protocol fixture must be accepted");
    let out = conn.output().first();
    let h = Header::parse(out).expect("valid protocol fixture must be accepted");
    assert!((h.kind) == (frame::numbers::Type::Ping));
    assert!(h.flags.contains(Flags::ACK));
}
