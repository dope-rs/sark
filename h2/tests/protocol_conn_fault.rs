use common::protocol::conn::{Control, Controls};
use sark_h2::{
    conn::event::Event,
    frame::numbers::{Length, Type},
    stream::Id,
};

use crate::common::{INITIAL_SETTINGS, protocol::preface::Preface};
pub mod common;

use sark_h2::{
    ServerRole,
    conn::{Error, machine::Connection},
    frame::{Ack, layout::Header, numbers::Flags},
};

#[test]
fn expands_conn_window() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_server(&mut conn)
        .expect("connection preface");

    let initial = conn.status().send_window().value();
    let wu = Control::new(0).try_window(1000).expect("protocol fixture");
    conn.ingress()
        .ingest(&wu)
        .expect("valid protocol fixture must be accepted");
    assert_eq!(conn.status().send_window().value(), initial + 1000);
}

#[test]
fn rejects_zero_window() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_server(&mut conn)
        .expect("connection preface");

    let mut payload = Vec::new();
    payload.extend_from_slice(&0u32.to_be_bytes());
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &Header {
            length: Length::new(4).expect("fixture domain"),
            kind: Type::WindowUpdate,
            flags: Flags(0),
            stream_id: Id::new(0).expect("fixture domain"),
        }
        .wire_bytes(),
    );
    bytes.extend_from_slice(&payload);
    assert!(matches!(
        conn.ingress().ingest(&bytes),
        Err(Error::ParseError(_))
    ));
}

#[test]
fn rejects_bad_hpack() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_server(&mut conn)
        .expect("connection preface");

    let payload = vec![0u8; 4];
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &Header {
            length: Length::new(payload.len() as u32).expect("fixture domain"),
            kind: Type::Headers,
            flags: Flags(Flags::END_HEADERS | Flags::END_STREAM),
            stream_id: Id::new(1).expect("fixture domain"),
        }
        .wire_bytes(),
    );
    bytes.extend_from_slice(&payload);
    assert!(matches!(
        conn.ingress().ingest(&bytes),
        Err(Error::Hpack(_))
    ));
}

#[test]
fn ignores_unknown_frame() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_server(&mut conn)
        .expect("connection preface");

    let payload = vec![0u8; 4];
    let mut bytes = vec![0, 0, payload.len() as u8, 0x99, 0];
    bytes.extend_from_slice(&0u32.to_be_bytes());
    bytes.extend_from_slice(&payload);
    conn.ingress()
        .ingest(&bytes)
        .expect("valid protocol fixture must be accepted");
    assert!(conn.ingress().poll_event().is_none());
    let ack = Controls::new(Ack::Complete)
        .try_settings(&[])
        .expect("protocol fixture");
    let split = bytes.len() - 1;
    conn.ingress()
        .ingest(&bytes[..split])
        .expect("fragmented unknown frame");
    assert!(conn.ingress().poll_event().is_none());
    conn.ingress().resume().expect("incomplete frame resume");
    assert!(conn.ingress().poll_event().is_none());
    let mut tail = bytes[split..].to_vec();
    tail.extend_from_slice(&ack);
    conn.ingress()
        .ingest(&tail)
        .expect("completed unknown frame and following ACK");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::SettingsAck)
    ));
    assert!(conn.ingress().poll_event().is_none());
}
