pub mod common;

use common::{
    conn::{Events, build::Fields as _, payload::Data as _},
    field::entry,
    message::{Frame as _, promise::Authority},
};
use dope_quic::{errors::Code, wire::stream::Id};
use o3::buffer::{resident, storage};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection, CreditMode, Error,
        event::{Event, Origin, Role},
        io::{Receive, Resume, Send},
        options::Config,
    },
    frame::TYPE_DATA,
    link::{Handoff, Status},
};
use sark_http::{field::literal, payload::response::Body as _};

#[test]
fn queued_payload_holds_budget_until_polled() {
    let mut config = Config::default();
    config.protocol.max_frame_bytes = 8;
    config.ingress.retained_bytes = 10;
    let mut conn = Connection::with_config(Role::Client, config).expect("connection");
    Send::new(&mut conn)
        .headers(Id::ZERO, Authority, Boundary::End)
        .expect("originating request");
    Receive::new(&mut conn)
        .owned(
            Id::ZERO,
            [literal!(entry(b":status", b"200"))]
                .try_headers()
                .expect("response headers"),
            Boundary::More,
        )
        .expect("headers");
    let _ = Events::new(&mut conn).poll_token().expect("headers");

    let frame = b"12345678".try_frame(TYPE_DATA).expect("DATA frame");
    Receive::new(&mut conn)
        .borrowed(Id::ZERO, &frame, Boundary::More)
        .expect("queued payload");
    assert_eq!(
        Receive::new(&mut conn).borrowed(Id::ZERO, &frame, Boundary::More),
        Err(Error::ReceiveCapacity {
            retained: 8,
            additional: 10,
            limit: 10,
        }),
    );

    let data = Events::new(&mut conn)
        .poll_token()
        .expect("DATA event")
        .into_data()
        .expect("payload");
    Receive::new(&mut conn)
        .borrowed(Id::ZERO, &frame, Boundary::More)
        .expect("queue budget released");
    assert!(
        data.segments()
            .flatten()
            .copied()
            .eq(b"12345678".iter().copied())
    );
}

#[test]
fn reset_reclaims_queued_open_budget() {
    let mut config = Config::default();
    config.protocol.max_frame_bytes = 8;
    config.ingress.retained_bytes = 10;
    let mut conn = Connection::with_config(Role::Client, config).expect("connection");
    let frame = b"12345678".try_frame(TYPE_DATA).expect("DATA frame");
    for raw in [0, 4] {
        let id = Id::new(raw).expect("request stream");
        Send::new(&mut conn)
            .headers(id, Authority, Boundary::End)
            .expect("originating request");
        Receive::new(&mut conn)
            .owned(
                id,
                [literal!(entry(b":status", b"200"))]
                    .try_headers()
                    .expect("response headers"),
                Boundary::More,
            )
            .expect("headers");
        let _ = Events::new(&mut conn).poll_token().expect("headers");
        let data = resident::Shared::from(storage::Shared::try_copy(&frame).expect("frame"));
        assert!(matches!(
            Receive::new(&mut conn).hold(id, data, Boundary::End),
            Handoff::Accepted(Ok(Status::Open))
        ));
        for code in [1, 2] {
            Receive::new(&mut conn)
                .reset(id, Some(id.into()), Code::from_u16(code))
                .expect("same-origin reset");
        }
        assert!(matches!(
            Events::new(&mut conn).poll_token().expect("reset"),
            Some(Event::Reset { stream_id, error_code, origin: Origin::Transport, released })
                if stream_id == id.into() && error_code == Code::from_u16(2)
                    && released == frame.len()
        ));
        assert!(
            Events::new(&mut conn)
                .poll_token()
                .expect("drained")
                .is_none()
        );
    }
}

#[test]
fn reuses_uncredited_budget() {
    let mut config = Config::default();
    config.ingress.retained_bytes = 2;
    let mut conn = Connection::with_config(Role::Server, config).expect("connection");
    let id = Id::new(2).expect("unidirectional stream");
    Receive::new(&mut conn)
        .borrowed(id, &[0x40], Boundary::More)
        .expect("partial type");
    assert_eq!(
        Receive::new(&mut conn).owned(id, vec![0x21, 9], Boundary::More),
        Err(Error::ReceiveCapacity {
            retained: 1,
            additional: 2,
            limit: 2
        }),
    );
    let tail = resident::Shared::from(storage::Shared::try_copy(&[0x21]).expect("tail"));
    Receive::new(&mut conn)
        .shared(id, tail, Boundary::More)
        .expect("unknown type completed");
    Receive::new(&mut conn)
        .owned(id, vec![1, 2], Boundary::More)
        .expect("budget reused");
    assert!(conn.events().poll_credit().expect("credit").is_none());
}

#[test]
fn refunds_without_credit() {
    let mut config = Config::default();
    config.ingress.retained_bytes = 2;
    let mut conn = Connection::with_config(Role::Server, config).expect("connection");
    let id = Id::new(2).expect("unidirectional stream");
    let first = resident::Shared::from(storage::Shared::try_copy(&[0x40]).expect("first"));
    assert!(matches!(
        Receive::new(&mut conn).hold(id, first, Boundary::More),
        Handoff::Accepted(Ok(Status::Open))
    ));
    assert_eq!(
        Receive::new(&mut conn).borrowed(id, &[0x21], Boundary::More),
        Err(Error::ReceiveState {
            buffered: 1,
            held: CreditMode::Held
        }),
    );
    let tail = resident::Shared::from(storage::Shared::try_copy(&[0x21]).expect("tail"));
    assert!(matches!(
        Receive::new(&mut conn).hold(id, tail, Boundary::More),
        Handoff::Accepted(Ok(Status::Open))
    ));
    assert!(
        matches!(conn.events().poll_credit().expect("credit"), Some((stream, 2, Resume::Idle)) if stream == id)
    );
}
