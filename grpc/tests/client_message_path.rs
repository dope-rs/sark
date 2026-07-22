use sark_grpc::client::{Config, Session};
use sark_grpc::metadata::Metadata;
use sark_grpc::status::Code;
use sark_h2::conn;
use sark_h2::{ServerRole, Settings};

#[test]
fn pending_messages_use_configured_slots() {
    let mut client = Session::with_config(Config {
        max_in_flight: 2,
        max_pending_msgs: 1,
        max_pending_len: 64,
        ..Config::default()
    });
    let server = sark_h2::Conn::<ServerRole>::with_config(conn::Config {
        local_settings: Settings {
            initial_window_size: 0,
            ..Settings::DEFAULT
        },
        ..conn::Config::default()
    });
    client.ingest(server.outbound()).unwrap();

    let first = client
        .start_stream_raw(b"/svc/Method", None, &Metadata::new())
        .unwrap();
    client.send_message_raw(first, b"first").unwrap();
    let second = client
        .start_stream_raw(b"/svc/Method", None, &Metadata::new())
        .unwrap();
    let status = client.send_message_raw(second, b"second").unwrap_err();
    assert_eq!(status.code(), Code::ResourceExhausted);
}
