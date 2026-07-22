use sark_grpc::client::{Config, Session};
use sark_grpc::metadata::Metadata;
use sark_grpc::status::Code;
use sark_h2::{Conn, ErrorCode, ServerRole};

fn open(client: &mut Session, count: usize) -> Vec<sark_h2::StreamId> {
    (0..count)
        .map(|_| {
            client
                .start_stream_raw(b"/svc/Method", None, &Metadata::new())
                .unwrap()
        })
        .collect()
}

fn open_unary(client: &mut Session, count: usize) -> Vec<sark_h2::StreamId> {
    (0..count)
        .map(|_| {
            client
                .start_client_stream_raw(b"/svc/Method", None, &Metadata::new())
                .unwrap()
        })
        .collect()
}

fn reset(client: &mut Session, streams: &[sark_h2::StreamId]) -> Result<(), sark_grpc::Status> {
    let request = client.outbound().to_vec();
    client.drain_outbound(request.len());
    let mut server = Conn::<ServerRole>::new();
    server.ingest(&request).unwrap();
    while server.poll_event().is_some() {}
    for &stream_id in streams {
        server.reset_stream(stream_id, ErrorCode::Cancel).unwrap();
    }
    client.ingest(server.outbound())
}

#[test]
fn in_flight_capacity_is_reused_after_reset() {
    let mut client = Session::with_config(Config {
        max_in_flight: 1,
        max_completed: 1,
        max_events: 1,
        ..Config::default()
    });
    let streams = open(&mut client, 1);
    let status = client
        .start_stream_raw(b"/svc/Method", None, &Metadata::new())
        .unwrap_err();
    assert_eq!(status.code(), Code::ResourceExhausted);

    reset(&mut client, &streams).unwrap();
    assert!(client.poll_event().is_some());
    assert!(client.poll_unary().is_none());
    assert!(
        client
            .start_stream_raw(b"/svc/Method", None, &Metadata::new())
            .is_ok()
    );
}

#[test]
fn completed_queue_bound_is_enforced() {
    let mut client = Session::with_config(Config {
        max_in_flight: 2,
        max_completed: 1,
        max_events: 2,
        ..Config::default()
    });
    let streams = open_unary(&mut client, 2);
    let status = reset(&mut client, &streams).unwrap_err();
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert!(client.poll_unary().is_some());
    assert!(client.poll_unary().is_none());
}
