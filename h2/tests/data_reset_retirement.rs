pub mod common;

use common::{
    capacity::{RESPONSE_OK, RETAINED_BODY, Upload},
    protocol::session::Frame,
};
use o3::buffer::{Residency as _, hold, pool::Pool, storage::Shared};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    conn::{
        DataOutcome,
        config::{Config, Settings},
        event::Event,
    },
    frame::numbers::ErrorCode,
};

#[test]
fn retires_rejected_data() {
    let (_, mut server, id) = Upload::new(Config {
        local_settings: Settings {
            initial_window_size: 1,
            ..Settings::DEFAULT
        },
        ..Config::default()
    })
    .try_connect()
    .expect("upload");
    server
        .streams()
        .headers()
        .respond(id, RESPONSE_OK, Boundary::More)
        .expect("response");
    server.transfer().drain(usize::MAX);
    let credit = server.status().send_window();
    let mut bytes = Vec::with_capacity(256);
    bytes.extend_from_slice(&RETAINED_BODY);
    let body = Shared::try_from(&mut bytes).expect("body owner");
    for _ in 0..2 {
        let sent = server
            .streams()
            .try_shared(id, &body, Boundary::More)
            .expect("retained body");
        assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == RETAINED_BODY.len()));
    }
    let wire = Frame::new(id.into_u32(), &[])
        .try_padded(Boundary::More, 1)
        .expect("flow violation");
    server.ingress().ingest(&wire).expect("reset stream");
    let (stream, error, retired) = server
        .ingress()
        .poll_event()
        .and_then(|event| match event {
            Event::StreamReset {
                stream_id: stream,
                error,
                retired: Some(retired),
                ..
            } => Some((stream, error, retired)),
            _ => None,
        })
        .expect("reset with retirement");
    assert_eq!(stream.id(), id);
    assert_eq!(error, ErrorCode::FlowControl);
    assert_eq!(
        retired.charge,
        hold::Charge::from_commit(2 * RETAINED_BODY.len(), 2 * body.resident(), 2)
    );
    assert!(server.status().send_window() == credit);
    assert!(server.ingress().poll_event().is_none());
}

#[test]
fn releases_before_notify() {
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
    let pool = Pool::try_new(1, RETAINED_BODY.len()).expect("body pool");
    let mut slot = pool.acquire().expect("body slot");
    slot.try_append(&RETAINED_BODY).expect("body fits").commit();
    let body = slot.freeze().into_shared();
    let charge = hold::Charge::from_owner(body.len(), body.resident());
    let sent = server
        .streams()
        .try_shared(id, &body, Boundary::More)
        .expect("retained body");
    assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == RETAINED_BODY.len()));
    drop(body);

    let mut notified = 0;
    server.transfer().discard(|retired| {
        assert_eq!(pool.available(), 1, "owner released before notification");
        assert_eq!(retired.stream_id.id(), id);
        assert_eq!(retired.charge, charge);
        notified += 1;
    });
    assert_eq!(notified, 1);
    assert!(server.status().send_window() == credit);
}
