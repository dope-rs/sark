use std::{cell::Cell, iter};

use common::{
    headers::{Frames, Response},
    protocol::Session,
};
use o3::buffer::pool::Pool;
use sark_h2::conn::event::Event;

pub mod common;

use dope_test::alloc::Tracker;
use o3::buffer::storage::Shared;
use sark_h2::{
    ServerRole,
    conn::{
        CLIENT_PREFACE, Error,
        config::{Config, Validated},
        machine::Connection,
    },
    frame::{Ack, payload::Ping},
};
use sark_http::field::metadata::section::rules;

use crate::common::INITIAL_SETTINGS;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn run_has_no_alloc() {
    let config = (Validated::new(Config::default())).expect("default H2 configuration");
    let mut conn =
        (Connection::<ServerRole, rules::Discard>::from_plan(config)).expect("server allocation");
    Session::new(&mut conn)
        .try_prime()
        .expect("client preface and settings");

    let mut warm = Frames::try_new(1)
        .expect("fixture stream")
        .try_owners()
        .expect("header fixture construction")
        .into_iter();
    assert!(warm.len() > 16 && warm.len() <= 65);
    (conn.ingress().ingest_from(&mut warm))
        .as_ref()
        .expect("warm owner run");
    assert!(warm.next().is_none());
    Response::new(&mut conn)
        .try_finish()
        .ok()
        .expect("request must accept a terminal response");

    let mut frames = Frames::try_new(3)
        .expect("fixture stream")
        .try_owners()
        .expect("header fixture construction")
        .into_iter();
    assert!(frames.len() > 16 && frames.len() <= 65);
    let (count, _) = Tracker::during(|| {
        (conn.ingress().ingest_from(&mut frames))
            .as_ref()
            .expect("measured owner run");
        Response::new(&mut conn)
            .try_finish()
            .ok()
            .expect("request must accept a terminal response");
    });
    assert!(frames.next().is_none());
    assert_eq!(count, 0);
}

#[test]
fn run_combines_owners() {
    let config = (Validated::new(Config::default())).expect("default H2 configuration");
    let mut conn =
        (Connection::<ServerRole, rules::Discard>::from_plan(config)).expect("server allocation");
    Session::new(&mut conn)
        .try_prime()
        .expect("client preface and settings");

    let wire = Frames::try_new(1)
        .expect("fixture stream")
        .try_wire()
        .expect("header fixture construction");
    assert!(wire.len() > 16 && wire.len() <= 65);
    let wire = wire.into_iter().flatten().collect::<Vec<_>>();
    let chunks = (wire
        .chunks(13)
        .map(Shared::try_copy)
        .collect::<Result<Vec<_>, _>>())
    .expect("chunk allocation");
    let mut source = chunks.into_iter();
    (conn.ingress().ingest_from(&mut source))
        .as_ref()
        .expect("combined owner run");
    assert!(source.next().is_none());
    Response::new(&mut conn)
        .try_finish()
        .ok()
        .expect("request must accept a terminal response");
}

#[test]
fn feed_releases_owners() {
    let config = (Validated::new(Config::default())).expect("default H2 configuration");
    let mut conn =
        (Connection::<ServerRole, rules::Discard>::from_plan(config)).expect("server allocation");
    Session::new(&mut conn)
        .try_prime()
        .expect("client preface and settings");

    let frames = Frames::try_new(1)
        .expect("fixture stream")
        .try_wire()
        .expect("header fixture construction");
    assert!(frames.len() > 16 && frames.len() <= 65);
    let capacity = (frames.iter().map(Vec::len).max()).expect("owner frames");
    let pool = (Pool::try_new(1, capacity)).expect("receive pool allocation");
    for frame in frames {
        let mut lease = (pool.acquire())
            .expect("the preceding receive owner was released after synchronous HPACK feed");
        lease
            .try_append(&frame)
            .expect("frame fits receive lease")
            .commit();
        (conn.ingress().ingest_owned(lease.freeze().into_shared()))
            .as_ref()
            .expect("retained owner frame");
    }
    assert!(
        pool.acquire().is_some(),
        "the final receive owner is independent from the published request owner",
    );
    Response::new(&mut conn)
        .try_finish()
        .ok()
        .expect("request must accept a terminal response");
}

#[test]
fn run_releases_before_pull() {
    let config = (Validated::new(Config::default())).expect("default H2 configuration");
    let mut conn =
        (Connection::<ServerRole, rules::Discard>::from_plan(config)).expect("server allocation");
    Session::new(&mut conn)
        .try_prime()
        .expect("client preface and settings");

    let frames = Frames::try_new(1)
        .expect("fixture stream")
        .try_wire()
        .expect("header fixture construction");
    assert!(frames.len() > 16 && frames.len() <= 65);
    let capacity = (frames.iter().map(Vec::len).max()).expect("owner frames");
    let pool = (Pool::try_new(1, capacity)).expect("receive pool allocation");
    {
        let mut frames = frames.into_iter();
        let mut source = iter::from_fn(|| {
            let frame = frames.next()?;
            let mut lease =
                (pool.acquire()).expect("ReceiveRun pulled before releasing the preceding owner");
            lease
                .try_append(&frame)
                .expect("frame fits receive lease")
                .commit();
            Some(lease.freeze().into_shared())
        });
        (conn.ingress().ingest_from(&mut source))
            .as_ref()
            .expect("owner run");
    }
    assert!(
        pool.acquire().is_some(),
        "ReceiveRun retained no transport owner after header commit",
    );
    Response::new(&mut conn)
        .try_finish()
        .ok()
        .expect("request must accept a terminal response");
}

#[test]
fn run_stops_when_blocked() {
    let config = (Validated::new(Config {
        event_capacity: 1,
        ..Config::default()
    }))
    .expect("single-event server config");
    let mut conn =
        (Connection::<ServerRole, rules::Discard>::from_plan(config)).expect("server allocation");
    conn.transfer().drain(usize::MAX);
    (conn.ingress().ingest(CLIENT_PREFACE))
        .as_ref()
        .expect("client preface");
    while conn.ingress().poll_event().is_some() {}
    (conn.ingress().ingest(INITIAL_SETTINGS))
        .as_ref()
        .expect("initial settings");
    while conn.ingress().poll_event().is_some() {}
    conn.transfer().drain(usize::MAX);

    let mut blocker = Vec::new();
    (Ping {
        ack: Ack::Required,
        opaque: [7; 8],
    }
    .encode(&mut blocker))
    .as_ref()
    .expect("test operation must succeed");
    (conn.ingress().ingest(&blocker))
        .as_ref()
        .expect("blocking ping");

    let mut frames = Frames::try_new(1)
        .expect("fixture stream")
        .try_wire()
        .expect("header fixture construction");
    assert!(frames.len() > 16 && frames.len() <= 65);
    let owners = frames.len();
    let mut tail = Vec::new();
    (Ping {
        ack: Ack::Required,
        opaque: [9; 8],
    }
    .encode(&mut tail))
    .as_ref()
    .expect("test operation must succeed");
    frames.push(tail);
    let capacity = (frames.iter().map(Vec::len).max()).expect("batched frames");
    let pool = (Pool::try_new(1, capacity)).expect("receive pool allocation");
    let pulls = Cell::new(0usize);
    let mut frames = frames.into_iter();
    let mut source = iter::from_fn(|| {
        let frame = frames.next()?;
        pulls.set(pulls.get() + 1);
        let mut lease =
            (pool.acquire()).expect("ReceiveRun pulled across a blocked event boundary");
        lease
            .try_append(&frame)
            .expect("frame fits receive lease")
            .commit();
        Some(lease.freeze().into_shared())
    });

    assert!(conn.ingress().ingest_from(&mut source) == Err(Error::EventFull { used: 1, limit: 1 }));
    assert_eq!(pulls.get(), owners);
    assert!(matches!(conn.ingress().poll_event(), Some(Event::Ping {
            ack: Ack::Required,
            opaque
        }) if opaque == [7; 8]));
    (conn.ingress().resume())
        .as_ref()
        .expect("blocked event resumes");
    Response::new(&mut conn)
        .try_finish()
        .ok()
        .expect("request must accept a terminal response");

    (conn.ingress().ingest_from(&mut source))
        .as_ref()
        .expect("tail run");
    assert_eq!(pulls.get(), owners + 1);
    assert!(matches!(conn.ingress().poll_event(), Some(Event::Ping {
            ack: Ack::Required,
            opaque
        }) if opaque == [9; 8]));
    assert!(source.next().is_none());
}

#[test]
fn blocked_keeps_owner() {
    let config = (Validated::new(Config {
        event_capacity: 1,
        ..Config::default()
    }))
    .expect("single-event server config");
    let mut conn =
        (Connection::<ServerRole, rules::Discard>::from_plan(config)).expect("server allocation");
    conn.transfer().drain(usize::MAX);
    (conn.ingress().ingest(CLIENT_PREFACE))
        .as_ref()
        .expect("client preface");
    while conn.ingress().poll_event().is_some() {}
    (conn.ingress().ingest(INITIAL_SETTINGS))
        .as_ref()
        .expect("initial settings");
    while conn.ingress().poll_event().is_some() {}
    conn.transfer().drain(usize::MAX);

    let mut ping = Vec::new();
    (Ping {
        ack: Ack::Required,
        opaque: [7; 8],
    }
    .encode(&mut ping))
    .as_ref()
    .expect("test operation must succeed");
    (conn.ingress().ingest(&ping))
        .as_ref()
        .expect("blocking ping");

    let frames = Frames::try_new(1)
        .expect("fixture stream")
        .try_wire()
        .expect("header fixture construction");
    assert!(frames.len() > 16 && frames.len() <= 65);
    let capacity = (frames.iter().map(Vec::len).max()).expect("owner frames");
    let pool = (Pool::try_new(1, capacity)).expect("receive pool allocation");
    for frame in &frames[..frames.len() - 1] {
        let mut lease = (pool.acquire())
            .expect("a non-final receive owner was released despite event backpressure");
        lease
            .try_append(frame)
            .expect("frame fits receive lease")
            .commit();
        (conn.ingress().ingest_owned(lease.freeze().into_shared()))
            .as_ref()
            .expect("non-final owner frame");
    }

    let mut last = (pool.acquire()).expect("final receive lease");
    let frame = (frames.last()).expect("final owner frame");
    last.try_append(frame)
        .expect("final frame fits receive lease")
        .commit();
    assert!(
        conn.ingress().ingest_owned(last.freeze().into_shared())
            == Err(Error::EventFull { used: 1, limit: 1 })
    );
    assert!(
        pool.acquire().is_none(),
        "the final owner must remain replayable while commit is blocked"
    );
    assert!(matches!(conn.ingress().poll_event(), Some(Event::Ping {
            ack: Ack::Required,
            opaque
        }) if opaque == [7; 8]));
    (conn.ingress().resume())
        .as_ref()
        .expect("blocked event resumes");
    assert!(
        pool.acquire().is_some(),
        "the final receive owner was released immediately after commit",
    );
    Response::new(&mut conn)
        .try_finish()
        .ok()
        .expect("request must accept a terminal response");
}
