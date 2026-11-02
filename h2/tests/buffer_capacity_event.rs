use std::num::NonZero;

use sark_h2::{conn::event::Event, frame::numbers::ErrorCode, hpack::decode::limit::Admission};
use sark_http::field::metadata::block::Validation;
pub mod common;

use sark_core::manifold::stream::{Boundary, Section};
use sark_h2::{
    ClientRole, ServerRole,
    conn::{Error, config::Config, machine::Connection},
    frame::{Ack, Block, layout::Headers, payload::Continuation},
    hpack::encode::Encoder,
};
use sark_http::field::metadata::map::record::Entry;

use crate::common::capacity;

#[test]
fn events_are_not_replayed() {
    let mut client = Connection::<ClientRole>::with_config(Config {
        event_capacity: 1,
        ..Config::default()
    })
    .expect("construct the one-event client");
    client
        .ingress()
        .poll_event()
        .expect("consume the initial settings event");
    let mut server = Connection::<ServerRole>::try_new().expect("construct the ping server");
    server.control().ping([1; 8]).expect("queue the first ping");
    server
        .control()
        .ping([2; 8])
        .expect("queue the second ping");

    assert!(
        client.ingress().ingest(server.output().first())
            == Err(Error::EventFull { used: 1, limit: 1 })
    );
    assert!(matches!(
        client.ingress().poll_event(),
        Some(Event::SettingsApplied)
    ));
    assert!(client.ingress().resume() == Err(Error::EventFull { used: 1, limit: 1 }));
    assert!(matches!(
    client.ingress().poll_event(),
    Some(Event::Ping {
                ack: Ack::Required,
                opaque: actual,
            }) if actual == [1; 8]
    ));
    client
        .ingress()
        .resume()
        .expect("resume after releasing the first ping event");
    assert!(matches!(
    client.ingress().poll_event(),
    Some(Event::Ping {
                ack: Ack::Required,
                opaque: actual,
            }) if actual == [2; 8]
    ));
    assert!(client.ingress().poll_event().is_none());
}

#[test]
fn headers_resume_once() {
    let (mut client, mut server, id) = capacity::Upload::new(Config {
        event_capacity: 2,
        ..Config::default()
    })
    .try_connect()
    .expect("capacity fixture");
    client.control().ping([1; 8]).expect("queue the first ping");
    client
        .control()
        .ping([2; 8])
        .expect("queue the second ping");
    let mut wire = client.output().first().to_vec();
    client.transfer().drain(usize::MAX);

    let mut encoder = Encoder::try_new(4096).expect("HPACK encoder capacity");
    let mut block = Vec::new();
    encoder
        .try_encode([Entry::new(b"x-trailer", b"retained")], &mut block)
        .expect("HPACK table");
    let split = block.len() / 2;
    Headers::new(id, Boundary::End, Block::More, None, &block[..split])
        .expect("construct the pressured HEADERS fragment")
        .encode(&mut wire)
        .expect("frame allocation");
    Continuation::new(id, Block::End, &block[split..])
        .expect("construct the pressured CONTINUATION fragment")
        .encode(&mut wire)
        .expect("frame allocation");

    assert!(server.ingress().ingest(&wire) == Err(Error::EventFull { used: 2, limit: 2 }));
    assert!(matches!(server.ingress().poll_event(), Some(Event::Ping {
            ack: Ack::Required,
            opaque,
        }) if opaque == [1; 8]));
    assert!(matches!(server.ingress().poll_event(), Some(Event::Ping {
            ack: Ack::Required,
            opaque,
        }) if opaque == [2; 8]));

    server
        .ingress()
        .resume()
        .expect("resume the fragmented header block");
    assert!(
        matches!(server.ingress().poll_event(), Some(Event::Headers {
            stream_id: actual,
            boundary: Boundary::End,
            section: Section::Trailing,
            ..
        }) if actual == id)
    );
}

#[test]
fn cancelled_headers_resume_once() {
    let (mut client, mut server, id) = capacity::Upload::new(Config {
        event_capacity: 2,
        ..Config::default()
    })
    .try_connect()
    .expect("capacity fixture");
    for value in [1, 2] {
        client.control().ping([value; 8]).expect("fill events");
    }
    let mut wire = client.output().first().to_vec();
    Headers::new(id, Boundary::End, Block::More, None, &[])
        .expect("pending trailers")
        .encode(&mut wire)
        .expect("header frame");
    server.ingress().ingest(&wire).expect("header prefix");
    let key = server.status().stream_key(id).expect("active stream");
    server.streams().cancel(key, ErrorCode::Cancel);

    let mut encoder = Encoder::try_new(4096).expect("HPACK encoder");
    let mut block = Vec::new();
    encoder
        .try_encode([Entry::new(b"x-trailer", b"cancelled")], &mut block)
        .expect("HPACK block");
    wire.clear();
    Continuation::new(id, Block::More, &block)
        .expect("nonfinal continuation")
        .encode(&mut wire)
        .expect("continuation frame");
    Continuation::new(id, Block::End, &[])
        .expect("final continuation")
        .encode(&mut wire)
        .expect("continuation frame");
    assert_eq!(
        server.ingress().ingest(&wire),
        Err(Error::EventFull { used: 2, limit: 2 })
    );
    for retry in 0..64 {
        assert_eq!(
            server.ingress().resume(),
            Err(Error::EventFull { used: 2, limit: 2 }),
            "retry {retry} must not consume the continuation budget"
        );
    }
    for value in [1, 2] {
        assert!(matches!(server.ingress().poll_event(), Some(Event::Ping {
            ack: Ack::Required,
            opaque,
        }) if opaque == [value; 8]));
    }
    server.ingress().resume().expect("finish cancelled headers");
    assert!(server.ingress().poll_event().is_none());
    assert!(!server.status().has_stream(id));
}

#[test]
fn reset_resumes_once() {
    let (mut client, mut server, id) = capacity::Upload::new(Config {
        event_capacity: 2,
        ..Config::default()
    })
    .try_connect()
    .expect("capacity fixture");
    client.control().ping([1; 8]).expect("queue the first ping");
    client
        .control()
        .ping([2; 8])
        .expect("queue the second ping");
    let mut wire = client.output().first().to_vec();
    client.transfer().drain(usize::MAX);

    let mut encoder = Encoder::try_new(4096).expect("HPACK encoder capacity");
    let mut block = Vec::new();
    encoder
        .try_encode([Entry::new(b"x-trailer", b"rejected")], &mut block)
        .expect("HPACK table");
    for frame in capacity::Fragments::new(id, Boundary::More, &block)
        .try_split(NonZero::new(block.len() / 2).expect("fragment size"))
        .expect("header fragments")
    {
        wire.extend_from_slice(&frame);
    }

    assert!(server.ingress().ingest(&wire) == Err(Error::EventFull { used: 2, limit: 2 }));
    assert!(
        matches!(server.ingress().poll_event(), Some(Event::Ping { opaque, .. }) if opaque == [1; 8])
    );
    assert!(
        matches!(server.ingress().poll_event(), Some(Event::Ping { opaque, .. }) if opaque == [2; 8])
    );
    server
        .ingress()
        .resume()
        .expect("resume the rejected fragmented header block");
    let event = server
        .ingress()
        .poll_event()
        .expect("rejected trailer event");
    let Event::StreamReset {
        stream_id: actual,
        error,
        retired,
        validation: check,
    } = event
    else {
        assert!(matches!(event, Event::StreamReset { .. }), "expected reset");
        return;
    };
    assert_eq!(actual, id);
    assert_eq!(error, ErrorCode::ProtocolError);
    assert!(retired.is_none());
    assert!(
        check.is_none(),
        "boundary rejection precedes header validation"
    );
    assert!(server.ingress().poll_event().is_none());
    assert!(!server.status().has_stream(id));
}

#[test]
fn malformed_cause() {
    let (_, mut server, id) = capacity::Upload::new(Config::default())
        .try_connect()
        .expect("connected upload");
    let mut encoder = Encoder::try_new(4096).expect("HPACK encoder");
    let mut block = Vec::new();
    encoder
        .try_encode([Entry::new(b":method", b"GET")], &mut block)
        .expect("HPACK table");
    let mut wire = Vec::new();
    Headers::new(id, Boundary::End, Block::End, None, &block)
        .expect("malformed trailer")
        .encode(&mut wire)
        .expect("encoded trailer");
    server.ingress().ingest(&wire).expect("trailer rejection");
    let event = server.ingress().poll_event().expect("rejection event");
    let Event::StreamReset {
        stream_id: actual,
        error,
        retired,
        validation: check,
    } = event
    else {
        assert!(matches!(event, Event::StreamReset { .. }), "expected reset");
        return;
    };
    assert_eq!(actual, id);
    assert_eq!(error, ErrorCode::ProtocolError);
    assert!(retired.is_none());
    let check = check.expect("exact rejected header validation");
    assert!(matches!(check.admit, Admission::Admitted), "{check:?}");
    assert!(matches!(check.sealed, Validation::Valid), "{check:?}");
    assert!(matches!(check.finished, Validation::Malformed), "{check:?}");
    assert!(server.ingress().poll_event().is_none());
    assert!(!server.status().has_stream(id));
}
