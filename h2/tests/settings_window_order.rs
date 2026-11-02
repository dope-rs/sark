pub mod common;

use common::{
    capacity::Upload,
    protocol::conn::{Control, Controls},
};
use sark_h2::{
    conn::{Error, config::Config},
    flow,
    frame::{
        Ack,
        numbers::{ErrorCode, SettingId},
    },
};

#[test]
fn rejects_window_overflow() {
    let (_, mut server, stream) = Upload::new(Config::default())
        .try_connect()
        .expect("open request stream");
    let initial = server.settings().peer().initial_window_size;
    server
        .ingress()
        .ingest(
            &Control::new(stream.into_u32())
                .try_window(1)
                .expect("window update"),
        )
        .expect("one extra byte of stream credit");
    let wire = Controls::new(Ack::Required)
        .try_settings(&[
            (SettingId::InitialWindowSize as u16, i32::MAX as u32),
            (SettingId::InitialWindowSize as u16, initial),
        ])
        .expect("repeated window settings");
    let error = wire
        .chunks(2)
        .try_for_each(|chunk| server.ingress().ingest(chunk))
        .expect_err("the intermediate window exceeds the protocol maximum");
    assert_eq!(
        error,
        Error::Adjust(flow::Adjust {
            value: initial as i32 + 1,
            committed: initial as i32 + 1,
            delta: i32::MAX - initial as i32,
        })
    );
    assert_eq!(ErrorCode::from(&error), ErrorCode::FlowControl);
    assert_eq!(server.settings().peer().initial_window_size, initial);
    assert!(server.output().is_empty());
    assert!(server.ingress().poll_event().is_none());
}

#[test]
fn validates_before_adjust() {
    let (_, mut server, stream) = Upload::new(Config::default())
        .try_connect()
        .expect("open request stream");
    let initial = server.settings().peer().initial_window_size;
    let before = server
        .status()
        .send_for(stream)
        .expect("stream window")
        .value();
    let wire = Controls::new(Ack::Required)
        .try_settings(&[
            (SettingId::InitialWindowSize as u16, 1),
            (SettingId::InitialWindowSize as u16, 2),
            (SettingId::MaxFrameSize as u16, 15_000),
        ])
        .expect("invalid final setting");
    assert_eq!(
        server.ingress().ingest(&wire),
        Err(Error::Setting {
            id: SettingId::MaxFrameSize,
            value: 15_000,
        })
    );
    assert_eq!(
        server
            .status()
            .send_for(stream)
            .expect("stream window")
            .value(),
        before
    );
    assert_eq!(server.settings().peer().initial_window_size, initial);
    assert!(server.output().is_empty());
}
