use sark_grpc::client::{Config, Session};
use sark_grpc::frame::MessageFrame;
use sark_grpc::headers::HeaderBlock;
use sark_grpc::metadata::Metadata;
use sark_grpc::status::Code;
use sark_h2::{Conn, ServerRole};

fn framed(count: usize, size: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let payload = vec![7u8; size];
    for _ in 0..count {
        MessageFrame::encode(false, &payload, &mut out).unwrap();
    }
    out
}

#[test]
fn client_buffered_stream_over_cap_is_resource_exhausted() {
    let mut client = Session::with_config(Config {
        max_message_len: 1 << 20,
        max_buffered_len: 1000,
        max_buffered_msgs: 1 << 20,
        ..Config::default()
    });
    let stream_id = client
        .start_stream_raw(b"/svc/Method", None, &Metadata::new())
        .expect("start stream");
    let req = client.outbound().to_vec();
    client.drain_outbound(req.len());

    let mut server = Conn::<ServerRole>::new();
    server.ingest(&req).expect("server ingests client request");
    while server.poll_event().is_some() {}

    let resp_headers = HeaderBlock::for_response(&Metadata::new()).expect("response headers");
    let h2_headers = resp_headers.as_h2();
    server
        .send_response(stream_id, h2_headers.iter().copied(), false)
        .expect("send response headers");

    let body = framed(15, 100);
    let sent = server
        .send_data(stream_id, &body, false)
        .expect("send response data");
    assert_eq!(sent, body.len(), "flow window must accept the test body");

    let resp = server.outbound().to_vec();
    let status = client
        .ingest(&resp)
        .expect_err("client must reject buffering past the cap");
    assert_eq!(status.code(), Code::ResourceExhausted);
}
