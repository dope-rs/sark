use sark_h2::{
    ClientRole, ServerRole,
    conn::{config::Config, event::Event, machine::Connection},
    frame::{Ack, payload::Ping},
};

#[test]
fn outbound_wrap_has_slices() {
    let mut conn = Connection::<ServerRole>::with_config(Config {
        outbound_capacity: 64,
        ..Config::default()
    })
    .expect("construct the wrapping output ring");
    conn.transfer().drain(usize::MAX);
    conn.control().ping([1; 8]).expect("queue ping one");
    conn.control().ping([2; 8]).expect("queue ping two");
    conn.control().ping([3; 8]).expect("queue ping three");
    conn.transfer().drain(40);
    conn.control().ping([4; 8]).expect("queue ping four");
    conn.control().ping([5; 8]).expect("queue ping five");
    conn.control()
        .ping([6; 8])
        .expect("cross the physical terminal reserve");

    let (first, second) = conn.output().slices();
    assert!(!first.is_empty());
    assert!(!second.is_empty());
    let expected = [first, second].concat();
    let mut actual = [0; 64];
    let written = conn.transfer().drain_into(&mut actual);
    assert_eq!(&actual[..written], expected);
    assert!(conn.output().first().is_empty());
}

#[test]
fn inbound_wrap_parses() {
    let client = Connection::<ClientRole>::try_new().expect("construct the frame source client");
    let mut server = Connection::<ServerRole>::with_config(Config {
        inbound_capacity: 64,
        ..Config::default()
    })
    .expect("construct the wrapping ingress ring");
    for chunk in client.output().first().chunks(32) {
        server
            .ingress()
            .ingest(chunk)
            .expect("ingest one handshake chunk");
    }
    while server.ingress().poll_event().is_some() {}

    let mut frames = Vec::new();
    for byte in 1..=4 {
        Ping {
            ack: Ack::Required,
            opaque: [byte; 8],
        }
        .encode(&mut frames)
        .expect("frame allocation");
    }
    server
        .ingress()
        .ingest(&frames[..52])
        .expect("ingest frames through the ring tail");
    server
        .ingress()
        .ingest(&frames[52..])
        .expect("ingest frames through the wrapped ring head");

    let mut seen = Vec::new();
    while let Some(event) = server.ingress().poll_event() {
        if let Event::Ping { opaque, .. } = event {
            seen.push(opaque[0]);
        }
    }
    assert_eq!(seen, [1, 2, 3, 4]);
}
