use common::protocol::conn::Controls;
use sark_h2::{conn::event::Event, stream::Id};
pub mod common;

use sark_h2::{
    ClientRole, ServerRole,
    conn::{CLIENT_PREFACE, Error, config, machine::Connection},
    frame,
    frame::{Ack, layout::Header, numbers::SettingId},
};

#[test]
fn server_starts_settings() {
    let conn = Connection::<ServerRole>::try_new().expect("server fixture");
    let out = conn.output().first();
    let h = Header::parse(out).expect("valid protocol fixture must be accepted");
    assert!((h.kind) == (frame::numbers::Type::Settings));
    assert!(h.flags.0 == 0);
}

#[test]
fn client_starts_frames() {
    let conn = Connection::<ClientRole>::try_new().expect("client fixture");
    let out = conn.output().first();
    assert!(out.len() >= CLIENT_PREFACE.len());
    assert_eq!(&out[..CLIENT_PREFACE.len()], CLIENT_PREFACE);
    let after = &out[CLIENT_PREFACE.len()..];
    let h = Header::parse(after).expect("valid protocol fixture must be accepted");
    assert!((h.kind) == (frame::numbers::Type::Settings));
    assert!(h.flags.0 == 0);
}

#[test]
fn client_starts_event() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
    let ev = conn
        .ingress()
        .poll_event()
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(ev, Event::PrefaceComplete));
}

#[test]
fn server_accepts_preface() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.ingress()
        .ingest(CLIENT_PREFACE)
        .expect("valid protocol fixture must be accepted");
    let ev = conn
        .ingress()
        .poll_event()
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(ev, Event::PrefaceComplete));
}

#[test]
fn server_rejects_preface() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    let mut bad = CLIENT_PREFACE.to_vec();
    *bad.last_mut()
        .expect("valid protocol fixture must be accepted") = b'X';
    assert!(matches!(
        conn.ingress().ingest(&bad),
        Err(Error::BadPreface {
            offset: 23,
            expected: b'\n',
            actual: b'X',
        })
    ));
}

#[test]
fn server_waits_for_preface() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    let partial = &CLIENT_PREFACE[..23];
    conn.ingress()
        .ingest(partial)
        .expect("valid protocol fixture must be accepted");
    assert!(conn.ingress().poll_event().is_none());
}

#[test]
fn server_joins_preface() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.ingress()
        .ingest(&CLIENT_PREFACE[..10])
        .expect("valid protocol fixture must be accepted");
    assert!(conn.ingress().poll_event().is_none());
    conn.ingress()
        .ingest(&CLIENT_PREFACE[10..])
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));
}

#[test]
fn server_blocks_early_ping() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(CLIENT_PREFACE)
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));

    assert!(matches!(
        conn.ingress().ingest(&Controls::new(Ack::Required).try_ping([1; 8]).expect("ping fixture")),
        Err(Error::InitialSettings {
            kind: 6,
            ack: Ack::Required,
            stream,
        }) if stream == Id::new(0).expect("fixture domain")
    ));
}

#[test]
fn client_blocks_early_ping() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));
    conn.transfer().drain(usize::MAX);

    assert!(matches!(
        conn.ingress().ingest(&Controls::new(Ack::Required).try_ping([1; 8]).expect("ping fixture")),
        Err(Error::InitialSettings {
            kind: 6,
            ack: Ack::Required,
            stream,
        }) if stream == Id::new(0).expect("fixture domain")
    ));
}

#[test]
fn server_accepts_push_one() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);
    let mut preface = CLIENT_PREFACE.to_vec();
    preface.extend_from_slice(
        &Controls::new(Ack::Required)
            .try_settings(&[(SettingId::EnablePush as u16, 1)])
            .expect("protocol fixture"),
    );

    conn.ingress()
        .ingest(&preface)
        .expect("valid protocol fixture must be accepted");

    assert!((conn.settings().peer().enable_push) == (config::Push::Enabled));
}

#[test]
fn client_push_starts_off() {
    let conn = Connection::<ClientRole>::try_new().expect("client fixture");
    assert!((conn.settings().peer().enable_push) == (config::Push::Disabled));
}

#[test]
fn rejects_initial_ack() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(CLIENT_PREFACE)
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));

    assert!(matches!(
        conn.ingress().ingest(&Controls::new(Ack::Complete).try_settings(&[]).expect("protocol fixture")),
        Err(Error::InitialSettings {
            kind: 4,
            ack: Ack::Complete,
            stream,
        }) if stream == Id::new(0).expect("fixture domain")
    ));
}

#[test]
fn rejects_initial_unknown() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(CLIENT_PREFACE)
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));

    let unknown = [0, 0, 0, 0x99, 0, 0, 0, 0, 0];
    assert!(matches!(
        conn.ingress().ingest(&unknown),
        Err(Error::ParseError(frame::numbers::ParseError::BadType(0x99)))
    ));
}

#[test]
fn joins_initial_settings() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);
    let settings = Controls::new(Ack::Required)
        .try_settings(&[])
        .expect("protocol fixture");
    let mut first = CLIENT_PREFACE.to_vec();
    first.extend_from_slice(&settings[..4]);
    conn.ingress()
        .ingest(&first)
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));
    assert!(conn.ingress().poll_event().is_none());

    conn.ingress()
        .ingest(&settings[4..])
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::SettingsApplied)
    ));
}

#[test]
fn parses_pipeline() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);

    let mut feed = Vec::new();
    feed.extend_from_slice(CLIENT_PREFACE);
    feed.extend_from_slice(
        &Controls::new(Ack::Required)
            .try_settings(&[])
            .expect("protocol fixture"),
    );
    feed.extend_from_slice(
        &Controls::new(Ack::Required)
            .try_ping([3u8; 8])
            .expect("ping fixture"),
    );
    conn.ingress()
        .ingest(&feed)
        .expect("valid protocol fixture must be accepted");

    let mut events = Vec::new();
    while let Some(e) = conn.ingress().poll_event() {
        events.push(e);
    }
    assert_eq!(events.len(), 3);
    assert!(matches!(events[0], Event::PrefaceComplete));
    assert!(matches!(events[1], Event::SettingsApplied));
    assert!(matches!(
    events[2],
    Event::Ping {
                ack: Ack::Required,
                opaque: actual
            } if actual == [3u8; 8]
    ));
}

#[test]
fn preserves_initial_frame() {
    let mut conn = Connection::<ServerRole>::with_config(config::Config {
        event_capacity: 1,
        ..config::Config::default()
    })
    .expect("one-event connection");
    let mut wire = CLIENT_PREFACE.to_vec();
    wire.extend_from_slice(
        &Controls::new(Ack::Required)
            .try_ping([0; 8])
            .expect("ping fixture"),
    );
    assert!(matches!(
        conn.ingress().ingest(&wire),
        Err(Error::EventFull { .. })
    ));
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));
    assert!(matches!(
        conn.ingress().resume(),
        Err(Error::InitialSettings { kind: 6, .. })
    ));
}
