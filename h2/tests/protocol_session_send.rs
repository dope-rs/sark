use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        conn::{Control, Controls},
        session::{Authority, Frame},
    },
};
use sark_http::field;

use crate::common::headers::entry;

pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, ServerRole,
    conn::{DataBlock, DataOutcome, Error, config::Config, machine::Connection},
    frame,
    frame::{Ack, numbers::SettingId},
    stream,
    stream::Id,
};

#[test]
fn bad_data_keeps_credit() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let id = conn
        .streams()
        .headers()
        .request(
            [
                field::literal!(entry(b":method", b"POST")),
                field::literal!(entry(b":scheme", b"http")),
                field::literal!(entry(b":path", b"/")),
            ],
            Boundary::End,
        )
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);
    let conn_old = conn.status().send_window();
    let flow_old = conn
        .status()
        .send_for(id)
        .expect("protocol scenario precondition");

    assert!(
        conn.streams().try_send(id, b"rejected", Boundary::More)
            == Err(Error::Transition(
                id,
                stream::Error::StreamClosed {
                    state: stream::State::HalfClosedLocal,
                    event: stream::Event::Data {
                        boundary: Boundary::More,
                    },
                    side: stream::Side::Local,
                },
            ))
    );
    assert!(conn.output().first().is_empty());
    assert!((conn.status().send_window()) == (conn_old));
    assert!((conn.status().send_for(id)) == (Some(flow_old)));
    assert!((conn.status().stream_state(id)) == (Some(stream::State::HalfClosedLocal)));
}

#[test]
fn stalled_data_chunks() {
    let mut conn = Connection::<ClientRole>::with_config(Config {
        outbound_capacity: 16_393,
        ..Config::default()
    })
    .expect("one full DATA frame fits");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let id = conn
        .streams()
        .headers()
        .request(
            [field::literal!(entry(b":method", b"POST"))],
            Boundary::More,
        )
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);

    let to_send = vec![1u8; 80_000];
    let n1 = conn
        .streams()
        .try_send(id, &to_send, Boundary::More)
        .expect("protocol scenario precondition");
    let n1 = match n1 {
        DataOutcome::Sent(n) => Some(n),
        _ => None,
    }
    .expect("first DATA makes progress");
    assert_eq!(n1.get(), 16_384);
    let output = conn.output().len();
    let credit = conn.status().send_window();
    let flow = conn.status().send_for(id);
    let state = conn.status().stream_state(id);

    assert!(matches!(
        conn.streams()
            .try_send(id, &to_send[n1.get()..], Boundary::More)
            .expect("stalled stream remains writable"),
        DataOutcome::Blocked(DataBlock::Egress)
    ));
    assert!(matches!(
        conn.streams()
            .try_send(id, &to_send, Boundary::More)
            .expect("stalled stream remains writable"),
        DataOutcome::Blocked(DataBlock::Egress)
    ));
    assert_eq!(conn.output().len(), output);
    assert!((conn.status().send_window()) == (credit));
    assert!((conn.status().send_for(id)) == (flow));
    assert!((conn.status().stream_state(id)) == (state));

    conn.transfer().drain(usize::MAX);
    assert!(matches!(
        conn.streams()
            .try_send(id, &to_send[n1.get()..], Boundary::More)
            .expect("drained output admits another DATA frame"),
        DataOutcome::Sent(sent) if sent.get() == 16_384
    ));
}

#[test]
fn window_resumes_data() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let id = conn
        .streams()
        .headers()
        .request(
            [field::literal!(entry(b":method", b"POST"))],
            Boundary::More,
        )
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);

    let mut sent = 0;
    let buf = vec![7u8; 100_000];
    loop {
        let n = conn
            .streams()
            .try_send(id, &buf[sent..], Boundary::More)
            .expect("protocol scenario precondition");
        match n {
            DataOutcome::Sent(n) => {
                sent += n.get();
                conn.transfer().drain(usize::MAX);
            }
            blocked => {
                assert!(matches!(
                    blocked,
                    DataOutcome::Blocked(DataBlock::ConnectionFlow)
                ));
                break;
            }
        }
    }
    assert_eq!(sent, 65_535);

    conn.ingress()
        .ingest(
            &Control::new(0)
                .try_window(100_000)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    conn.ingress()
        .ingest(
            &Control::new(id.into_u32())
                .try_window(100_000)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");

    let n = conn
        .streams()
        .try_send(id, &buf[sent..], Boundary::End)
        .expect("protocol scenario precondition");
    assert!(matches!(n, DataOutcome::Sent(_)));
}

#[test]
fn settings_adjust_window() {
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_headers(Boundary::More, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );
    conn.ingress()
        .ingest(
            &Frame::new(3, &block)
                .try_headers(Boundary::More, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(
        conn.ingress().poll_event().is_some(),
        "scenario emits the expected event"
    );

    let s1_old = conn
        .status()
        .send_for(Id::new(1).expect("fixture domain"))
        .expect("protocol scenario precondition")
        .value();
    let s3_old = conn
        .status()
        .send_for(Id::new(3).expect("fixture domain"))
        .expect("protocol scenario precondition")
        .value();

    let peer = Controls::new(Ack::Required)
        .try_settings(&[(SettingId::InitialWindowSize as u16, 100_000)])
        .expect("protocol fixture");
    conn.ingress()
        .ingest(&peer)
        .expect("protocol scenario precondition");

    let delta = 100_000i32 - 65_535;
    assert_eq!(
        conn.status()
            .send_for(Id::new(1).expect("fixture domain"))
            .expect("protocol scenario precondition")
            .value(),
        s1_old + delta
    );
    assert_eq!(
        conn.status()
            .send_for(Id::new(3).expect("fixture domain"))
            .expect("protocol scenario precondition")
            .value(),
        s3_old + delta
    );
}
