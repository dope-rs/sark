pub mod common;

use common::{
    capacity::{Upload, try_start},
    headers::Frames,
};
use dope_test::alloc::Tracker;
use sark_h2::conn::{Error, config::Config, event::Event};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn borrows_without_alloc() {
    let (mut client, mut server, _) = Upload::new(Config::default())
        .try_connect()
        .expect("connected pair");
    let id = try_start(&mut client).expect("request");
    let wire = client.output().first();
    let mut rest = wire;
    let input: &mut &[u8] = &mut rest;
    let (allocs, _) = Tracker::during(|| {
        server.ingress().borrow(input).expect("borrowed headers");
    });
    assert_eq!(allocs, 0);
    assert!(input.is_empty());
    assert!(
        matches!(server.ingress().poll_event(), Some(Event::Headers { stream_id, .. }) if stream_id.id() == id)
    );
}

#[test]
fn preserves_pressure_input() {
    let (mut client, mut server, _) = Upload::new(Config {
        event_capacity: 2,
        ..Config::default()
    })
    .try_connect()
    .expect("connected pair");
    for _ in 0..3 {
        try_start(&mut client).expect("request");
    }
    let wire = client.output().first();
    let mut rest = wire;
    let error = server
        .ingress()
        .borrow(&mut rest)
        .expect_err("event pressure");
    assert!(matches!(error, Error::EventFull { used: 2, limit: 2 }));
    assert!(!rest.is_empty());
    assert!(rest.len() < wire.len());
    for _ in 0..2 {
        assert!(matches!(
            server.ingress().poll_event(),
            Some(Event::Headers { .. })
        ));
    }
    server.ingress().borrow(&mut rest).expect("resume headers");
    assert!(rest.is_empty());
    assert!(matches!(
        server.ingress().poll_event(),
        Some(Event::Headers { .. })
    ));
    assert!(server.ingress().poll_event().is_none());
}

#[test]
fn retains_continuations() {
    let (_, mut server, _) = Upload::new(Config::default())
        .try_connect()
        .expect("connected pair");
    let (first, second) = Frames::try_new(3)
        .expect("stream")
        .try_split()
        .expect("fragments");
    for owner in [first, second] {
        let mut rest = owner.as_slice();
        server.ingress().borrow(&mut rest).expect("defer fragment");
        assert_eq!(rest, owner.as_slice());
        server
            .ingress()
            .ingest_owned(owner)
            .expect("retained fragment");
    }
    assert!(matches!(
        server.ingress().poll_event(),
        Some(Event::Headers { .. })
    ));
}
