use std::num::NonZero;

use common::{
    capacity::POST,
    recipe::{config, listener},
};
use dope::{
    core::driver::{route::KeyTag, settings, settings::claim::Needs},
    manifold::timing,
};
use sark_h2::{
    conn::config::{Error, capacity::Payload, payload::Error::RetentionOverflow},
    frame,
    server::{self, runtime, sync},
};

pub mod common;

#[test]
fn receive_layout() {
    let receive = settings::Receive::fixed::<2, { 64 << 10 }>();
    let machine = server::Plan::new(config(), receive)
        .expect("server limits admit the receive layout")
        .build()
        .expect("receive layout builds an H2 machine");

    assert_eq!(
        machine.inbound_capacity,
        receive.buffer_len().into_nonzero().get() as usize
            + frame::HEADER_LEN
            + machine.local_settings.max_frame_size.into_usize()
            - 1
    );
}

#[test]
fn preserves_protocol() {
    use sark_h2::{
        ServerRole,
        conn::config::{Config, Validated},
    };

    let protocol = Config {
        stream_capacity: 3,
        event_capacity: 7,
        recv_window_target: 8192,
        ..Config::default()
    };
    let plan = server::Plan::new(config(), settings::Receive::fixed::<2, 4096>())
        .expect("server plan")
        .with_config(protocol);
    let machine = plan.build().expect("server headroom");
    let admitted = Validated::<ServerRole>::new(machine).expect("connection admission");
    assert_eq!(admitted.stream_count().get(), 3);
    let machine = plan
        .with_streams(NonZero::<usize>::MIN.saturating_add(511))
        .build()
        .expect("server headroom");
    assert_eq!(machine.event_capacity, 7);
    assert_eq!(machine.recv_window_target, 8192);
    let admitted = Validated::<ServerRole>::new(machine).expect("connection admission");
    assert_eq!(admitted.stream_count().get(), 512);
}

#[test]
fn receives_after_partial() {
    use o3::buffer::storage;
    use sark_core::manifold::stream::Boundary;
    use sark_h2::{
        ClientRole, ServerRole,
        conn::{event::Event, machine::Connection},
        frame::{layout::Header, numbers},
    };

    const RECEIVE: usize = 64 << 10;
    let machine = server::Plan::new(
        config(),
        settings::Receive::fixed::<2, { RECEIVE as u32 }>(),
    )
    .expect("server plan")
    .build()
    .expect("receive capacity");
    let size = machine.local_settings.max_frame_size.into_usize();
    let mut server = Connection::<ServerRole>::with_config(machine).expect("server");
    let mut client = Connection::<ClientRole>::try_new().expect("client");
    let stream = client
        .streams()
        .headers()
        .request(POST, Boundary::More)
        .expect("request");
    server
        .ingress()
        .ingest(client.output().first())
        .expect("request ingress");
    while server.ingress().poll_event().is_some() {}
    let mut wire = Vec::new();
    for _ in 0..5 {
        wire.extend_from_slice(
            &Header {
                length: numbers::Length::new(size as u32).expect("frame length"),
                kind: numbers::Type::Data,
                flags: numbers::Flags(0),
                stream_id: stream,
            }
            .wire_bytes(),
        );
        wire.resize(wire.len() + size, b'a');
    }
    let split = frame::HEADER_LEN + size - 1;
    let parts = [
        &wire[..split],
        &wire[split..split + RECEIVE],
        &wire[split + RECEIVE..],
    ];
    let mut total = 0;
    for part in parts {
        let bytes = storage::Shared::try_copy(part).expect("receive owner");
        server
            .ingress()
            .ingest_owned(bytes)
            .expect("fragment followed by full receive buffer");
        while let Some(event) = server.ingress().poll_event() {
            if let Event::Data { data, .. } = event {
                data.for_each(|part| {
                    assert!(part.iter().all(|byte| *byte == b'a'));
                    total += part.len();
                });
            }
        }
    }
    assert_eq!(total, 5 * size);
}

#[test]
fn extends_segments() {
    const RETAINED: usize = 37;

    let base = Payload::default();
    let machine = server::Plan::new(
        server::Config {
            max_connection_body_chunks: RETAINED,
            ..config()
        },
        settings::Receive::fixed::<2, { 64 << 10 }>(),
    )
    .expect("retained chunk limit is valid")
    .build()
    .expect("retained chunks fit the H2 machine");

    assert_eq!(machine.payload_capacity.data(), base.data());
    assert_eq!(machine.payload_capacity.headers(), base.headers());
    assert_eq!(
        machine.payload_capacity.segments(),
        base.segments() + RETAINED
    );
}

#[test]
fn rejects_chunk_overflow() {
    let result = server::Plan::new(
        server::Config {
            max_connection_body_chunks: usize::MAX,
            ..config()
        },
        settings::Receive::fixed::<2, { 64 << 10 }>(),
    )
    .expect("unbounded retained chunks are a valid preliminary plan")
    .build();

    assert!(matches!(
        result.err().expect("retained segment overflow must fail"),
        Error::PayloadCapacity(RetentionOverflow {
            extra: usize::MAX,
            ..
        })
    ));
}

#[test]
fn endpoint_rejects_plan() {
    let plan = server::Plan::new(
        server::Config {
            max_connection_body_chunks: usize::MAX,
            ..config()
        },
        settings::Receive::fixed::<2, { 64 << 10 }>(),
    )
    .expect("unbounded retained chunks are a valid preliminary plan");
    let result = sync::Endpoint::try_new((), plan);

    assert!(matches!(
        result.err().expect("endpoint must retain the plan failure"),
        Error::PayloadCapacity(RetentionOverflow {
            extra: usize::MAX,
            ..
        })
    ));
}

#[test]
fn listener_receive_layout() {
    let receive = settings::Receive::fixed::<8, 4096>();
    let plan = runtime::Plan::<KeyTag<7>>::new(listener(), receive, config())
        .expect("listener and H2 limits form a valid runtime plan")
        .with_streams(NonZero::<usize>::MIN.saturating_add(511));
    let reqs = Needs::from_tree(&plan).expect("runtime plan declares finite resources");
    let resolved = reqs
        .exact::<timing::Throughput>()
        .expect("declared receive layout resolves exactly")
        .receive();

    assert!((resolved) == (receive));
}

#[test]
fn rejects_zero_outbound() {
    assert!(
        (server::Config {
            max_outbound_bytes: 0,
            ..config()
        }
        .validate())
            == (Err(server::ConfigError::ZeroOutbound))
    );
}
