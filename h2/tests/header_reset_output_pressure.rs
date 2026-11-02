pub mod common;

use std::num::NonZero;

use common::{
    capacity::{Fragments, RESPONSE_OK, RETAINED_BODY, Upload},
    hpack::{Fields, GET},
};
use o3::buffer::storage::Shared;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    conn::{DataOutcome, config::Config, event::Event},
    frame::numbers::ErrorCode,
};

#[test]
fn retires_before_pressure() {
    for split in [false, true] {
        let (_, mut server, id) = Upload::new(Config {
            outbound_capacity: 63,
            ..Config::default()
        })
        .try_connect()
        .expect("upload");
        for _ in 0..3 {
            server.control().ping([0; 8]).expect("fill output");
        }
        let block = Fields::new(&GET).try_encode().expect("malformed trailers");
        let chunk = if split { 1 } else { block.len() };
        let frames = Fragments::new(id, Boundary::End, &block)
            .try_split(NonZero::new(chunk).expect("fragment"))
            .expect("header frames");
        for frame in &frames[..frames.len() - 1] {
            server.ingress().ingest(frame).expect("continuation prefix");
        }
        let last = frames.last().expect("last header frame");
        let result = if split {
            server.ingress().ingest(last)
        } else {
            let mut bytes = last.as_slice();
            let result = server.ingress().borrow(&mut bytes);
            assert!(bytes.is_empty());
            result
        };
        assert!(result.is_err_and(|error| error.is_pressure()));
        assert!(server.ingress().is_blocked());
        assert!(!server.status().has_stream(id));
        assert!(
            matches!(server.ingress().poll_event(), Some(Event::StreamReset {
            stream_id: stream, error: ErrorCode::ProtocolError, ..
        }) if stream.id() == id)
        );
        server.transfer().drain(usize::MAX);
        server.ingress().resume().expect("resume after reset");
        assert!(server.ingress().poll_event().is_none());
    }
}

#[test]
fn retires_output_once() {
    for split in [false, true] {
        let (_, mut server, id) = Upload::new(Config::default())
            .try_connect()
            .expect("upload");
        server
            .streams()
            .headers()
            .respond(id, RESPONSE_OK, Boundary::More)
            .expect("response");
        server.transfer().drain(usize::MAX);
        let credit = server.status().send_window();
        let body = Shared::try_copy(&RETAINED_BODY).expect("body owner");
        let sent = server
            .streams()
            .try_shared(id, &body, Boundary::More)
            .expect("retained body");
        assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == RETAINED_BODY.len()));
        let block = Fields::new(&GET).try_encode().expect("malformed trailers");
        let chunk = if split { 1 } else { block.len() };
        let frames = Fragments::new(id, Boundary::End, &block)
            .try_split(NonZero::new(chunk).expect("fragment"))
            .expect("header frames");
        for frame in frames {
            server.ingress().ingest(&frame).expect("reject trailers");
        }
        let retired = server
            .ingress()
            .poll_event()
            .and_then(|event| match event {
                Event::StreamReset { retired, .. } => retired,
                _ => None,
            })
            .expect("retained output retirement");
        assert_eq!(retired.charge.logical_bytes, RETAINED_BODY.len());
        assert!(server.status().send_window() == credit);
        assert!(server.ingress().poll_event().is_none());
    }
}
