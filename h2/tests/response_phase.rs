pub mod common;

use std::num::NonZero;

use common::{
    capacity::Fragments,
    hpack::Fields,
    protocol::{Session, conn::Controls, session::Frame},
};
use sark_core::manifold::stream::{Boundary, Section};
use sark_h2::{
    ClientRole,
    conn::{Error, config::Config, event::Event, machine::Connection},
    frame::{Ack, Block, numbers::ErrorCode},
    hpack::encode::Encoder,
};
use sark_http::field::metadata::map::record::Entry;

#[test]
fn keeps_initial_phase() {
    for chunk in [NonZero::<usize>::MAX, NonZero::<usize>::MIN] {
        let mut conn = Connection::<ClientRole>::with_config(Config {
            event_capacity: 1,
            ..Config::default()
        })
        .expect("single-event client");
        conn.ingress().poll_event().expect("initial preface event");
        Session::new(&mut conn).try_prime().expect("peer preface");
        let stream = Session::new(&mut conn).try_open().expect("request");
        let mut encoder = Encoder::try_new(4096).expect("peer encoder");
        let interim = Fields::new(&[
            Entry::new(b":status", b"103"),
            Entry::new(b"x-phase", b"retained"),
        ])
        .try_with(&mut encoder)
        .expect("informational block");
        for wire in Fragments::new(stream, Boundary::More, &interim)
            .try_split(chunk)
            .expect("informational frames")
        {
            conn.ingress().ingest(&wire).expect("informational ingress");
        }
        let block = Fields::new(&[
            Entry::new(b":status", b"200"),
            Entry::new(b"content-length", b"4"),
            Entry::new(b"x-phase", b"retained"),
        ])
        .try_with(&mut encoder)
        .expect("final block using the same HPACK table");
        let frames = Fragments::new(stream, Boundary::More, &block)
            .try_split(chunk)
            .expect("final frames");
        for (index, wire) in frames.iter().enumerate() {
            let result = conn.ingress().ingest(wire);
            if index + 1 == frames.len() {
                assert!(matches!(
                    result,
                    Err(Error::EventFull { used: 1, limit: 1 })
                ));
            } else {
                result.expect("non-final fragment needs no event slot");
            }
        }
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Headers {
                stream_id,
                head,
                section: Section::Initial,
                boundary: Boundary::More,
            }) if stream_id == stream && head.fields().get(b":status") == Some(b"103".as_slice())
        ));
        conn.ingress().resume().expect("resume final response once");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Headers {
                stream_id,
                head,
                section: Section::Initial,
                boundary: Boundary::More,
            }) if stream_id == stream
                && head.fields().get(b":status") == Some(b"200".as_slice())
                && head.fields().get(b"x-phase") == Some(b"retained".as_slice())
        ));
        let data = Frame::new(stream.into_u32(), b"body")
            .try_data(Boundary::End)
            .expect("final DATA");
        conn.ingress()
            .ingest(&data)
            .expect("DATA follows final response");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Data {
                data,
                boundary: Boundary::End,
                ..
            }) if data.contiguous() == Some(b"body".as_slice())
        ));
        assert!(conn.ingress().poll_event().is_none());
    }
}

#[test]
fn informational_cannot_end() {
    for chunk in [NonZero::<usize>::MAX, NonZero::<usize>::MIN] {
        let mut conn = Connection::<ClientRole>::try_new().expect("client");
        Session::new(&mut conn).try_prime().expect("peer preface");
        let mut encoder = Encoder::try_new(4096).expect("peer encoder");
        for status in [b"103", b"200"] {
            let stream = Session::new(&mut conn).try_open().expect("request");
            let block = Fields::new(&[
                Entry::new(b":status", status),
                Entry::new(b"x-phase", b"retained"),
            ])
            .try_with(&mut encoder)
            .expect("response block");
            for wire in Fragments::new(stream, Boundary::End, &block)
                .try_split(chunk)
                .expect("response frames")
            {
                conn.ingress().ingest(&wire).expect("response ingress");
            }
            let event = conn.ingress().poll_event().expect("response disposition");
            if status == b"103" {
                assert!(matches!(
                    event,
                    Event::StreamReset {
                        stream_id,
                        error: ErrorCode::ProtocolError,
                        validation: None,
                        ..
                    } if stream_id == stream
                ));
            } else {
                assert!(matches!(
                    event,
                    Event::Headers {
                        head,
                        section: Section::Initial,
                        boundary: Boundary::End,
                        ..
                    } if head.fields().get(b"x-phase") == Some(b"retained".as_slice())
                ));
            }
            assert!(!conn.status().has_stream(stream));
        }
    }
}

#[test]
fn data_needs_final() {
    for interim in [false, true] {
        let mut conn = Connection::<ClientRole>::with_config(Config {
            event_capacity: 1,
            ..Config::default()
        })
        .expect("single-event client");
        conn.ingress().poll_event().expect("initial preface event");
        Session::new(&mut conn).try_prime().expect("peer preface");
        let stream = Session::new(&mut conn).try_open().expect("request");
        if interim {
            let block = Fields::new(&[Entry::new(b":status", b"103")])
                .try_encode()
                .expect("informational block");
            let wire = Frame::new(stream.into_u32(), &block)
                .try_headers(Boundary::More, Block::End)
                .expect("informational frame");
            conn.ingress().ingest(&wire).expect("informational ingress");
            conn.ingress().poll_event().expect("informational event");
        }
        conn.ingress()
            .ingest(&Controls::new(Ack::Complete).try_ping([7; 8]).expect("ping"))
            .expect("occupy the event slot");
        let before = conn.status().recv_window().value();
        let data = Frame::new(stream.into_u32(), b"bad")
            .try_padded(Boundary::End, 2)
            .expect("premature padded DATA");
        assert!(matches!(
            conn.ingress().ingest(&data),
            Err(Error::EventFull { used: 1, limit: 1 })
        ));
        assert_eq!(conn.status().recv_window().value(), before);
        assert!(conn.status().has_stream(stream));
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Ping { .. })
        ));
        conn.ingress().resume().expect("commit the deferred reset");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::StreamReset {
                stream_id,
                error: ErrorCode::ProtocolError,
                validation: None,
                ..
            }) if stream_id == stream
        ));
        assert_eq!(conn.status().recv_window().value(), before - 6);
        assert!(!conn.status().has_stream(stream));
        assert!(conn.ingress().poll_event().is_none());
    }
}
