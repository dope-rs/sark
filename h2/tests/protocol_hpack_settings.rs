pub mod common;

use std::{num::NonZero, pin::Pin};

use common::{
    capacity, hpack,
    protocol::{Session, conn::Controls, preface::Preface},
};
use o3::{
    base::cell::region,
    buffer::{hold::scope, resident, storage},
};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        self,
        config::{Config, Settings, Validated},
        event::Event,
        machine::Connection,
    },
    frame::Ack,
    hpack::{decode, encode::Encoder},
    stream::Id,
};
use sark_http::field::metadata::section::rules;

#[test]
fn initial_encoder_clamp() {
    for table in [0, 128] {
        let mut client = Connection::<ClientRole>::with_config(Config {
            hpack_encoder: conn::config::capacity::Encoder {
                table,
                ..Default::default()
            },
            ..Config::default()
        })
        .expect("client");
        let mut server = Connection::<ServerRole>::with_config(Config {
            local_settings: Settings {
                header_table_size: table,
                ..Settings::DEFAULT
            },
            ..Config::default()
        })
        .expect("server");
        server
            .ingress()
            .ingest(client.output().first())
            .expect("client preface");
        client.transfer().drain(usize::MAX);
        while server.ingress().poll_event().is_some() {}
        client
            .ingress()
            .ingest(server.output().first())
            .expect("server settings");
        server.transfer().drain(usize::MAX);
        server
            .ingress()
            .ingest(client.output().first())
            .expect("settings acknowledgement");
        client.transfer().drain(usize::MAX);
        assert!(matches!(
            server.ingress().poll_event(),
            Some(Event::SettingsAck)
        ));
        capacity::try_start(&mut client).expect("request");
        server
            .ingress()
            .ingest(client.output().first())
            .expect("initial table update");
        assert!(matches!(
            server.ingress().poll_event(),
            Some(Event::Headers { .. })
        ));
    }
}

#[test]
fn initial_until_ack() {
    let mut server = Connection::<ServerRole>::with_config(Config {
        local_settings: Settings {
            header_table_size: 64,
            ..Settings::DEFAULT
        },
        ..Config::default()
    })
    .expect("server");
    Preface::new(common::INITIAL_SETTINGS)
        .try_server(&mut server)
        .expect("preface");
    let mut encoder = Encoder::try_new(4096).expect("encoder");
    let first = capacity::Fields::new(Id::new(1).expect("id"), &hpack::DYNAMIC_REFERENCES)
        .try_encode(&mut encoder)
        .expect("headers");
    server
        .ingress()
        .ingest(&first)
        .expect("initial table references");
    assert!(matches!(
        server.ingress().poll_event(),
        Some(Event::Headers { .. })
    ));

    let ack = Controls::new(Ack::Complete).try_settings(&[]).expect("ack");
    server.ingress().ingest(&ack).expect("acknowledgement");
    assert!(matches!(
        server.ingress().poll_event(),
        Some(Event::SettingsAck)
    ));
    encoder.set_max_size(64);
    let block = hpack::Fields::new(&hpack::GET)
        .try_with(&mut encoder)
        .expect("HPACK table");
    let frames = capacity::Fragments::new(Id::new(3).expect("id"), Boundary::End, &block)
        .try_split(NonZero::new(1).expect("chunk"))
        .expect("fragments");
    for frame in frames {
        server
            .ingress()
            .ingest(&frame)
            .expect("fragmented table update");
    }
    assert!(matches!(
        server.ingress().poll_event(),
        Some(Event::Headers { .. })
    ));
}

#[test]
fn ack_requires_update() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(4096, token.proof())
            .expect("receive budget");
        let plan = Validated::new(Config {
            local_settings: Settings {
                header_table_size: 64,
                ..Settings::DEFAULT
            },
            ..Config::default()
        })
        .expect("server config");
        let mut server =
            Connection::<ServerRole, rules::RetainRaw, resident::Shared<'_>>::from_plan(plan)
                .expect("server");
        server.ingress().try_bind(budget).expect("bind receive");
        Session::new(&mut server).try_prime().expect("preface");
        let ack = Controls::new(Ack::Complete).try_settings(&[]).expect("ack");
        let owner = storage::Shared::try_copy(&ack).expect("ACK owner");
        let mut consumed = 0;
        server
            .ingress()
            .borrow_scope(&scope::Input::new(&owner, &budget), &mut consumed)
            .expect("acknowledgement");
        assert_eq!(consumed, ack.len());
        let mut encoder = Encoder::try_new(4096).expect("encoder");
        let headers = capacity::Fields::new(Id::new(1).expect("id"), &hpack::GET)
            .try_encode(&mut encoder)
            .expect("headers");
        assert!(matches!(
            server.ingress().ingest(&headers),
            Err(conn::Error::Hpack(decode::Error::MissingResize {
                limit: 64
            }))
        ));
    });
}
