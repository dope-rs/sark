use core::mem;

use common::protocol::conn::Controls;
use sark_h2::{
    conn::event::Event,
    frame::numbers::{Length, Type},
    stream::Id,
};

use crate::common::{INITIAL_SETTINGS, headers::entry, protocol::preface::Preface};
pub mod common;

use o3::buffer::storage::Shared;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        CLIENT_PREFACE, Error, config,
        config::{Config, Settings, Validated, capacity},
        machine::Connection,
    },
    frame,
    frame::{
        Ack,
        layout::Header,
        numbers::{Flags, SettingId},
    },
};
use sark_http::field::{self, metadata::section::rules};

use crate::common::protocol::conn::Authority;

#[test]
fn applies_peer_settings() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(CLIENT_PREFACE)
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));

    let initial = conn.status().send_window().value();
    let peer = Controls::new(Ack::Required)
        .try_settings(&[(SettingId::InitialWindowSize as u16, 100_000)])
        .expect("protocol fixture");
    conn.ingress()
        .ingest(&peer)
        .expect("valid protocol fixture must be accepted");
    assert_eq!(conn.status().send_window().value(), initial);
    assert_eq!(conn.settings().peer().initial_window_size, 100_000);
    let out = conn.output().first();
    let h = Header::parse(out).expect("valid protocol fixture must be accepted");
    assert!((h.kind) == (frame::numbers::Type::Settings));
    assert!(h.flags.contains(Flags::ACK));
    assert_eq!(h.length.into_u32(), 0);
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::SettingsApplied)
    ));
}

#[test]
fn emits_settings_ack() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_server(&mut conn)
        .expect("connection preface");

    let ack = Controls::new(Ack::Complete)
        .try_settings(&[])
        .expect("protocol fixture");
    conn.ingress()
        .ingest(&ack)
        .expect("valid protocol fixture must be accepted");
    assert!(conn.output().first().is_empty());
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::SettingsAck)
    ));
}

#[test]
fn rejects_small_frame_size() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(CLIENT_PREFACE)
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));

    let peer = Controls::new(Ack::Required)
        .try_settings(&[(SettingId::MaxFrameSize as u16, 15_000)])
        .expect("protocol fixture");
    assert!(matches!(
        conn.ingress().ingest(&peer),
        Err(Error::Setting {
            id: SettingId::MaxFrameSize,
            value: 15_000,
        })
    ));
}

#[test]
fn rejects_bad_push() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(CLIENT_PREFACE)
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));

    let peer = Controls::new(Ack::Required)
        .try_settings(&[(SettingId::EnablePush as u16, 2)])
        .expect("protocol fixture");
    assert!(matches!(
        conn.ingress().ingest(&peer),
        Err(Error::Setting {
            id: SettingId::EnablePush,
            value: 2,
        })
    ));
}

#[test]
fn rejects_settings_size() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(CLIENT_PREFACE)
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));

    let payload = vec![0u8; 7];
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &Header {
            length: Length::new(payload.len() as u32).expect("fixture domain"),
            kind: Type::Settings,
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
fn rejects_ack_payload() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_server(&mut conn)
        .expect("connection preface");

    let payload = vec![0u8; 6];
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &Header {
            length: Length::new(payload.len() as u32).expect("fixture domain"),
            kind: Type::Settings,
            flags: Flags(Flags::ACK),
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
fn client_applies_settings() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));
    conn.transfer().drain(usize::MAX);

    let peer = Controls::new(Ack::Required)
        .try_settings(&[(SettingId::HeaderTableSize as u16, 8192)])
        .expect("protocol fixture");
    conn.ingress()
        .ingest(&peer)
        .expect("valid protocol fixture must be accepted");
    assert_eq!(conn.settings().peer().header_table_size, 8192);
    assert_eq!(conn.settings().table_cap(), 4096);
    assert_eq!(conn.settings().table_limit(), 4096);
    let out = conn.output().first();
    let h = Header::parse(out).expect("valid protocol fixture must be accepted");
    assert!((h.kind) == (frame::numbers::Type::Settings));
    assert!(h.flags.contains(Flags::ACK));
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::SettingsApplied)
    ));
}

#[test]
fn default_hpack_foot() {
    let valid = Validated::<ClientRole>::new(Config::default())
        .expect("default H2 configuration must be valid");
    let foot = valid.hpack_foot();
    assert_eq!(foot.arena_bytes(), 12_192);
    assert_eq!(foot.index_bytes(), 1_088);
    assert_eq!(foot.ledger_bytes(), 4_096);
    assert_eq!(foot.tail_bytes(), 17_376);
    assert_eq!(foot.resident(), 17_552);
    assert_eq!(foot.allocations(), 2);
}

#[test]
fn zst_authority_stays_free() {
    assert_eq!(
        mem::size_of::<Connection<ClientRole>>(),
        mem::size_of::<Connection<ClientRole, rules::RetainRaw, Shared, Authority>>(),
    );
}

#[test]
fn peer_table_sets_limit() {
    let local = Settings {
        header_table_size: 64,
        ..Settings::DEFAULT
    };
    let valid = Validated::<ClientRole>::new(Config {
        local_settings: local,
        hpack_encoder: capacity::Encoder {
            table: 8192,
            ..Default::default()
        },
        ..Config::default()
    })
    .expect("valid protocol fixture must be accepted");
    let foot = valid.hpack_foot();
    let initial = Validated::<ClientRole>::new(Config {
        hpack_encoder: capacity::Encoder {
            table: 8192,
            ..Default::default()
        },
        ..Config::default()
    })
    .expect("initial decoder capacity");
    assert!(foot == initial.hpack_foot());
    assert_eq!(foot.allocations(), 2);

    let mut conn = Connection::<ClientRole>::from_plan(valid)
        .expect("valid protocol fixture must be accepted");
    assert_eq!(conn.settings().local().header_table_size, 64);
    assert_eq!(conn.settings().table_cap(), 8192);
    assert_eq!(conn.settings().table_limit(), 4096);
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));
    conn.transfer().drain(usize::MAX);

    let maximum = Controls::new(Ack::Required)
        .try_settings(&[(SettingId::HeaderTableSize as u16, u32::MAX)])
        .expect("protocol fixture");
    conn.ingress()
        .ingest(&maximum)
        .expect("valid protocol fixture must be accepted");
    assert_eq!(conn.settings().peer().header_table_size, u32::MAX);
    assert_eq!(conn.settings().table_cap(), 8192);
    assert_eq!(conn.settings().table_limit(), 8192);
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::SettingsApplied)
    ));
    conn.transfer().drain(usize::MAX);

    let shrink = Controls::new(Ack::Required)
        .try_settings(&[(SettingId::HeaderTableSize as u16, 128)])
        .expect("protocol fixture");
    conn.ingress()
        .ingest(&shrink)
        .expect("valid protocol fixture must be accepted");
    assert_eq!(conn.settings().table_cap(), 8192);
    assert_eq!(conn.settings().table_limit(), 128);
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::SettingsApplied)
    ));
    conn.transfer().drain(usize::MAX);

    conn.ingress()
        .ingest(&maximum)
        .expect("valid protocol fixture must be accepted");
    assert_eq!(conn.settings().table_cap(), 8192);
    assert_eq!(conn.settings().table_limit(), 8192);
}

#[test]
fn duplicate_keeps_shrink() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_client(&mut conn)
        .expect("connection preface");
    let peer = Controls::new(Ack::Required)
        .try_settings(&[
            (SettingId::HeaderTableSize as u16, 1024),
            (SettingId::HeaderTableSize as u16, 4096),
        ])
        .expect("protocol fixture");
    conn.ingress().ingest(&peer).expect("valid table limits");
    conn.transfer().drain(usize::MAX);
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("request after settings");
    assert_eq!(
        &conn.output().first()[frame::HEADER_LEN..],
        &[0x3f, 0xe1, 0x07, 0x3f, 0xe1, 0x1f, 0x82],
    );
}

#[test]
fn invalid_keeps_encoder() {
    let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
    Preface::new(INITIAL_SETTINGS)
        .try_client(&mut conn)
        .expect("connection preface");
    let peer = Controls::new(Ack::Required)
        .try_settings(&[
            (SettingId::HeaderTableSize as u16, 1024),
            (SettingId::HeaderTableSize as u16, 4096),
            (SettingId::MaxFrameSize as u16, 15_000),
        ])
        .expect("protocol fixture");
    assert!(matches!(
        peer.chunks(2)
            .try_for_each(|chunk| conn.ingress().ingest(chunk)),
        Err(Error::Setting {
            id: SettingId::MaxFrameSize,
            value: 15_000,
        })
    ));
    assert_eq!(conn.settings().peer().header_table_size, 4096);
    assert_eq!(conn.settings().table_limit(), 4096);
    assert!(conn.output().first().is_empty());
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("unchanged encoder after rejected settings");
    assert_eq!(&conn.output().first()[frame::HEADER_LEN..], &[0x82]);
}

#[test]
fn defaults_match_spec() {
    let s = Settings::DEFAULT;
    assert_eq!(s.header_table_size, 4096);
    assert!((s.enable_push) == (config::Push::Enabled));
    assert_eq!(s.max_concurrent_streams, None);
    assert_eq!(s.initial_window_size, 65_535);
    assert_eq!(s.max_frame_size.into_u32(), 16_384);
    assert_eq!(s.max_header_list_size, None);
}

#[test]
fn applies_known_settings() {
    let mut s = Settings::DEFAULT;
    s.apply(SettingId::HeaderTableSize, 1024)
        .expect("valid protocol fixture must be accepted");
    assert_eq!(s.header_table_size, 1024);
    s.apply(SettingId::EnablePush, 0)
        .expect("valid protocol fixture must be accepted");
    assert!((s.enable_push) == (config::Push::Disabled));
    s.apply(SettingId::MaxConcurrentStreams, 128)
        .expect("valid protocol fixture must be accepted");
    assert_eq!(s.max_concurrent_streams, Some(128));
}

#[test]
fn rejects_large_window() {
    let mut s = Settings::DEFAULT;
    assert!(matches!(
        s.apply(SettingId::InitialWindowSize, 0x8000_0000),
        Err(Error::Setting {
            id: SettingId::InitialWindowSize,
            value: 0x8000_0000,
        })
    ));
}

#[test]
fn ignores_unknown_setting() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    conn.transfer().drain(usize::MAX);
    conn.ingress()
        .ingest(CLIENT_PREFACE)
        .expect("valid protocol fixture must be accepted");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::PrefaceComplete)
    ));

    let peer = Controls::new(Ack::Required)
        .try_settings(&[
            (0xff_ff, 0xdeadbeef),
            (SettingId::HeaderTableSize as u16, 9999),
        ])
        .expect("protocol fixture");
    for chunk in peer.chunks(2) {
        conn.ingress()
            .ingest(chunk)
            .expect("fragmented settings must be accepted");
    }
    assert_eq!(conn.settings().peer().header_table_size, 9999);
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::SettingsApplied)
    ));
}
