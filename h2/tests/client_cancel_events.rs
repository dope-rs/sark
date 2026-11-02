pub mod common;

use common::{
    capacity::RETAINED_BODY,
    protocol::{
        Session,
        conn::{Control, Controls},
        session::{Frame, OK_RESPONSE, Wire},
    },
};
use sark_core::manifold::{
    client::{
        Connection as _,
        event::{Reset, Response},
    },
    stream::{Boundary, Section},
};
use sark_h2::{
    ClientRole,
    conn::{
        config::{Config, capacity::Payload},
        event::Event,
        machine::Connection,
    },
    frame::{Ack, Block, numbers::ErrorCode},
};

#[test]
fn preserves_reset_reasons() {
    for (error, reason) in [
        (ErrorCode::EnhanceYourCalm, Reset::ResourceExhausted),
        (ErrorCode::InadequateSecurity, Reset::InadequateSecurity),
    ] {
        let mut conn = Connection::<ClientRole>::try_new().expect("client");
        Session::new(&mut conn).try_prime().expect("primed client");
        let id = Session::new(&mut conn).try_open().expect("request");
        let key = conn.status().stream_key(id).expect("request stream");
        conn.ingress()
            .ingest(
                &Control::new(id.into_u32())
                    .try_reset(error)
                    .expect("peer reset"),
            )
            .expect("ingest reset");
        assert!(
            matches!(conn.poll(), Some(Response::Reset { stream, reason: actual })
            if stream == key && actual == reason)
        );

        let id = Session::new(&mut conn).try_open().expect("next request");
        let key = conn.status().stream_key(id).expect("next stream");
        conn.reset(key, reason).expect("local reset");
        assert_eq!(
            Wire::new(conn.output().first())
                .try_reset()
                .expect("reset frame"),
            Some((id, error))
        );
    }
}

#[test]
fn cancels_closed_events() {
    for reset in [false, true] {
        let mut conn = Connection::<ClientRole>::with_config(Config {
            stream_capacity: 1,
            payload_capacity: Payload::new(1, 4).expect("one DATA permit"),
            ..Config::default()
        })
        .expect("client");
        Session::new(&mut conn).try_prime().expect("primed client");
        let id = Session::new(&mut conn).try_open().expect("first request");
        let old = conn.status().stream_key(id).expect("first stream");
        conn.ingress()
            .ingest(
                &Frame::new(id.into_u32(), OK_RESPONSE)
                    .try_headers(Boundary::More, Block::End)
                    .expect("response headers"),
            )
            .expect("queue response");
        conn.ingress()
            .ingest(
                &Controls::new(Ack::Complete)
                    .try_settings(&[])
                    .expect("settings acknowledgment"),
            )
            .expect("queue connection event");
        conn.ingress()
            .ingest(
                &Frame::new(id.into_u32(), &RETAINED_BODY)
                    .try_data(Boundary::More)
                    .expect("response DATA"),
            )
            .expect("queue sole DATA permit");
        let terminal = if reset {
            Control::new(id.into_u32())
                .try_reset(ErrorCode::Cancel)
                .expect("peer reset")
        } else {
            Frame::new(id.into_u32(), &[])
                .try_headers(Boundary::End, Block::End)
                .expect("response trailers")
        };
        conn.ingress()
            .ingest(&terminal)
            .expect("close native stream");
        assert!(!conn.status().has_key(old));

        let id = Session::new(&mut conn)
            .try_open()
            .expect("reused stream slot");
        let current = conn.status().stream_key(id).expect("second stream");
        conn.ingress()
            .ingest(
                &Frame::new(id.into_u32(), OK_RESPONSE)
                    .try_headers(Boundary::More, Block::End)
                    .expect("second response"),
            )
            .expect("queue unaffected response");
        assert_eq!(conn.reset(old, Reset::Cancelled), Err(old));
        conn.cancel(old, Reset::Cancelled);
        assert!(conn.status().has_key(current));
        conn.ingress()
            .ingest(
                &Frame::new(id.into_u32(), &RETAINED_BODY)
                    .try_data(Boundary::More)
                    .expect("second DATA"),
            )
            .expect("cancelled DATA permit is reusable");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::SettingsAck)
        ));
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Headers {
                stream_id,
                section: Section::Initial,
                ..
            }) if stream_id == current
        ));
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::Data { stream_id, .. }) if stream_id == current
        ));
        assert!(conn.ingress().poll_event().is_none());
    }
}
