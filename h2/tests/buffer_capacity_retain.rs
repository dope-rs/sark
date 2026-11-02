use std::{iter, num::NonZero, pin::Pin};

use sark_h2::conn::{config::capacity, event::Event};

pub mod common;

use o3::{
    base::cell::region,
    buffer::{
        self,
        hold::{self, scope},
        pool::Pool,
        resident, storage,
    },
};
use sark_core::manifold::stream::{Boundary, Section};
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        CLIENT_PREFACE, DataOutcome, Error,
        config::{Config, Validated},
        machine::Connection,
    },
    frame::numbers::ErrorCode,
    hpack::encode::Encoder,
};
use sark_http::field::metadata::{map::record::Entry, section::rules};

use crate::common::{
    capacity::{Fragments, Upload, try_start},
    protocol::session::Frame,
    retain::Frames,
};

#[test]
fn resident_append() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(CLIENT_PREFACE.len(), token.proof())
            .expect("preface budget");
        let plan = Validated::new(Config::default()).expect("server config");
        let mut server =
            Connection::<ServerRole, rules::RetainRaw, resident::Shared<'_>>::from_plan(plan)
                .expect("server");
        server.ingress().ingest(&[]).expect("empty unbound resume");
        let error = server
            .ingress()
            .ingest(&CLIENT_PREFACE[..4])
            .expect_err("unbound copy");
        assert_eq!(error, Error::Authority(resident::BindError::Unbound));
        assert!(!error.is_pressure());
        server
            .ingress()
            .try_bind(budget)
            .expect("bind copy authority");
        server
            .ingress()
            .ingest(&CLIENT_PREFACE[..4])
            .expect("partial preface");
        assert_eq!(budget.resident(), 4);
        server
            .ingress()
            .ingest(&CLIENT_PREFACE[4..])
            .expect("complete preface");
        assert_eq!(budget.resident(), 0);
        let error = server
            .ingress()
            .ingest(&[0; 25])
            .expect_err("append budget");
        assert!(!error.is_pressure());
        let cause = match error {
            Error::Resident(resident::AllocError::Budget(cause)) => Some(cause),
            _ => None,
        }
        .expect("exact append budget failure");
        let (required, left) = cause.into_parts();
        assert_eq!((required.get(), left), (25, CLIENT_PREFACE.len()));
        assert_eq!(budget.resident(), 0);
    });
}

#[test]
fn projection_retries() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(16_384 + 512, token.proof())
            .expect("copy headroom");
        let mut client = Connection::<ClientRole>::try_new().expect("client");
        let stream = try_start(&mut client).expect("request");
        let plan = Validated::new(Config::default()).expect("server config");
        let mut server =
            Connection::<ServerRole, rules::RetainRaw, resident::Shared<'_>>::from_plan(plan)
                .expect("server");
        server.ingress().try_bind(budget).expect("bind authority");
        server
            .ingress()
            .ingest(client.output().first())
            .expect("request head");
        while server.ingress().poll_event().is_some() {}
        let blocker = resident::Exact::try_copy(&budget, &[0; 512]).expect("transient owner");
        let input = resident::Exact::try_copy(&budget, &stream.record(None))
            .expect("input owner")
            .into_shared();
        let error = server
            .ingress()
            .ingest_owned(input)
            .expect_err("copy budget is occupied");
        assert!(!error.is_pressure());
        let cause = match error {
            Error::Resident(resident::AllocError::Budget(cause)) => Some(cause),
            _ => None,
        }
        .expect("exact projected copy budget failure");
        let (required, left) = cause.into_parts();
        assert_eq!((required.get(), left), (512, 0));
        assert!(server.ingress().poll_event().is_none());
        assert_eq!(budget.resident(), 16_384 + 512);
        drop(blocker);

        server
            .ingress()
            .resume()
            .expect("explicit retry with copy headroom");
        let data = match server.ingress().poll_event() {
            Some(Event::Data {
                data,
                boundary: Boundary::More,
                ..
            }) => Some(data),
            _ => None,
        }
        .expect("one committed DATA frame");
        assert_eq!(data.contiguous(), Some([b'x'; 512].as_slice()));
        assert!(server.ingress().poll_event().is_none());
        assert_eq!(budget.resident(), 512);
        drop(data);
        assert_eq!(budget.resident(), 0);
    });
}

#[test]
fn scoped_copy_retries() {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        let budget = domain
            .get_mut()
            .try_budget(8, token.proof())
            .expect("final DATA budget");
        let mut client = Connection::<ClientRole>::try_new().expect("client");
        let stream = try_start(&mut client).expect("request");
        let plan = Validated::new(Config {
            event_capacity: 1,
            payload_capacity: capacity::Payload::new(1, 1).expect("one payload permit"),
            ..Config::default()
        })
        .expect("server config");
        let mut server =
            Connection::<ServerRole, rules::RetainRaw, resident::Shared<'_>>::from_plan(plan)
                .expect("server");
        let mut result = server.ingress().ingest_owned(resident::Shared::from(
            storage::Shared::try_copy(client.output().first()).expect("request owner"),
        ));
        while matches!(result, Err(Error::EventFull { used: 1, limit: 1 })) {
            server.ingress().poll_event().expect("setup event");
            result = server.ingress().resume();
        }
        result.expect("request head");
        while server.ingress().poll_event().is_some() {}
        server.transfer().drain(usize::MAX);

        let record = stream.record(None);
        let payload = &record[9..17];
        let wire = Frame::new(stream.into_u32(), payload)
            .try_data(Boundary::End)
            .expect("final DATA");
        let pool = Pool::try_new(1, 16_384).expect("receive owner");
        let owner = pool
            .try_copy(&wire)
            .expect("DATA fits owner")
            .expect("receive owner is vacant")
            .into_shared();
        let blocker = resident::Exact::try_copy(&budget, payload).expect("occupied budget");
        let before = (
            server.status().recv_window().value(),
            server
                .status()
                .recv_for(stream)
                .expect("stream window")
                .value(),
            server.status().stream_state(stream),
        );
        let mut consumed = 0;
        let error = server
            .ingress()
            .borrow_scope(&scope::Input::new(&owner, &budget), &mut consumed)
            .expect_err("final copy exceeds occupied budget");
        assert!(!error.is_pressure());
        let cause = match error {
            Error::Hold(hold::Error::Resident(resident::AllocError::Budget(cause))) => Some(cause),
            _ => None,
        }
        .expect("exact scoped copy budget failure");
        let (required, left) = cause.into_parts();
        assert_eq!((required.get(), left), (8, 0));
        assert_eq!(consumed, 0);
        assert!(server.ingress().poll_event().is_none());
        assert_eq!(
            (
                server.status().recv_window().value(),
                server
                    .status()
                    .recv_for(stream)
                    .expect("stream window")
                    .value(),
                server.status().stream_state(stream),
            ),
            before
        );
        assert_eq!(budget.resident(), 8);
        drop(blocker);
        assert_eq!(budget.resident(), 0);

        server
            .ingress()
            .borrow_scope(&scope::Input::new(&owner, &budget), &mut consumed)
            .expect("retry with the same source and restored reservations");
        assert_eq!(consumed, wire.len());
        drop(owner);
        assert_eq!(pool.available(), 1);
        let data = match server.ingress().poll_event() {
            Some(Event::Data {
                data,
                boundary: Boundary::End,
                ..
            }) => Some(data),
            _ => None,
        }
        .expect("one committed final DATA event");
        assert_eq!(data.contiguous(), Some(payload));
        assert_eq!(
            data.segments()
                .map(buffer::Residency::resident)
                .sum::<usize>(),
            8
        );
        assert!(server.ingress().poll_event().is_none());
        assert_eq!(server.status().recv_window().value(), before.0 - 8);
        assert_eq!(
            server
                .status()
                .recv_for(stream)
                .expect("stream window")
                .value(),
            before.1 - 8
        );
        assert_eq!(budget.resident(), 8);
        drop(data);
        assert_eq!(budget.resident(), 0);
    });
}

#[test]
fn owners_flush_fragments() {
    check_pressure(Config {
        payload_capacity: capacity::Payload::new(1, 1)
            .expect("configure one retained payload owner"),
        ..Config::default()
    });
}

#[test]
fn bytes_flush_fragments() {
    check_pressure(Config {
        inbound_capacity: 96,
        ..Config::default()
    });
}

#[test]
fn data_retries_once() {
    let mut client = Connection::<ClientRole>::try_new().expect("construct the data client");
    let id = try_start(&mut client).expect("capacity fixture");
    assert!(matches!(
        client.streams().try_send(id, b"one", Boundary::More),
        Ok(DataOutcome::Sent(count)) if count.get() == 3
    ));
    assert!(matches!(
        client.streams().try_send(id, b"two", Boundary::End),
        Ok(DataOutcome::Sent(count)) if count.get() == 3
    ));

    let mut server = Connection::<ServerRole>::with_config(Config {
        event_capacity: 8,
        payload_capacity: capacity::Payload::new(1, capacity::Payload::default().headers())
            .expect("configure one retained data payload"),
        ..Config::default()
    })
    .expect("construct the pressured data server");
    let error = server.ingress().ingest(client.output().first());
    assert!(
        error.as_ref().is_err_and(|error| {
            matches!(error, Error::PayloadFull { used: 1, limit: 1 })
                && ErrorCode::from(error) == ErrorCode::EnhanceYourCalm
        }),
        "{error:?}"
    );
    let mut first = None;
    while let Some(event) = server.ingress().poll_event() {
        if let Event::Data { data, .. } = event {
            let mut retained = Vec::with_capacity(data.len());
            data.for_each(|segment| retained.extend_from_slice(segment));
            first = Some(retained);
        }
    }
    assert_eq!(first.as_deref(), Some(b"one".as_slice()));

    server
        .ingress()
        .resume()
        .expect("retry the uncommitted DATA frame");
    let event = server
        .ingress()
        .poll_event()
        .expect("emit the retried DATA event");
    assert!(matches!(
        event,
        Event::Data {
            boundary: Boundary::End,
            ..
        }
    ));
    if let Event::Data { data, .. } = event {
        assert!(
            data.segments()
                .flat_map(|part| part.as_ref().iter())
                .eq(b"two".iter())
        );
    }
    assert!(server.ingress().poll_event().is_none());
}

#[test]
fn header_pool_reuses_slots() {
    let mut client = Connection::<ClientRole>::try_new().expect("construct the header client");
    for _ in 0..3 {
        try_start(&mut client).expect("capacity fixture");
    }
    let mut server = Connection::<ServerRole>::with_config(Config {
        event_capacity: 8,
        payload_capacity: capacity::Payload::new(capacity::Payload::default().data(), 2)
            .expect("configure two retained header payloads"),
        ..Config::default()
    })
    .expect("construct the bounded header server");

    assert!(matches!(
        server.ingress().ingest(client.output().first()),
        Err(error @ Error::HeaderDecodeFull { used: 2, limit: 2 }) if error.is_pressure()
    ));
    let first = iter::from_fn(|| server.ingress().poll_event())
        .filter(|event| matches!(event, Event::Headers { .. }))
        .count();
    assert_eq!(first, 2);

    server
        .ingress()
        .resume()
        .expect("resume after releasing header payloads");
    let second = iter::from_fn(|| server.ingress().poll_event())
        .filter(|event| matches!(event, Event::Headers { .. }))
        .count();
    assert_eq!(second, 1);
}

fn check_pressure(config: Config) {
    let (_, mut server, id) = Upload::new(config).try_connect().expect("capacity fixture");
    let mut encoder = Encoder::try_new(4096).expect("HPACK encoder capacity");
    let mut block = Vec::new();
    encoder
        .try_encode(
            [Entry::new(
                b"x-pressure",
                b"abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz",
            )],
            &mut block,
        )
        .expect("HPACK table");
    let frames = Fragments::new(id, Boundary::End, &block)
        .try_split(NonZero::new(1).expect("fragment size"))
        .expect("header fragments");
    assert!(frames.len() > 6);
    assert!(frames.len() <= 1 + 64);
    for frame in frames {
        server
            .ingress()
            .ingest_owned(
                storage::Shared::try_copy(&frame).expect("retain one fragmented trailer frame"),
            )
            .expect("ingest one retained trailer fragment");
    }
    let event = server
        .ingress()
        .poll_event()
        .expect("completed trailers event");
    assert!(matches!(
        event,
        Event::Headers {
            stream_id: actual, boundary: Boundary::End, section: Section::Trailing, ..
        } if actual == id
    ));
    if let Event::Headers { head, .. } = event {
        let (headers, _, _) = head.into_parts();
        assert!(headers.iter().any(|header| {
            header.name == b"x-pressure"
                && header.value == b"abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz"
        }));
    }
}
