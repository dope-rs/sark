use sark_grpc::client::Session;
use sark_grpc::frame::MessageFrame;
use sark_grpc::headers::HeaderBlock;
use sark_grpc::metadata::Metadata;
use sark_grpc::status::Status;
use sark_h2::{Conn, ServerRole};

#[test]
fn unary_response_has_one_owner() {
    let mut client = Session::new();
    let stream_id = client
        .start_unary_raw(b"/svc/Method", None, &Metadata::new(), b"request")
        .unwrap();
    let mut server = Conn::<ServerRole>::new();
    server.ingest(client.outbound()).unwrap();
    while server.poll_event().is_some() {}

    let headers = HeaderBlock::for_response(&Metadata::new()).unwrap();
    server
        .send_response(stream_id, headers.as_h2().iter().copied(), false)
        .unwrap();
    let frame = MessageFrame::header(false, 8).unwrap();
    server
        .send_data_parts(stream_id, &frame, b"response", false)
        .unwrap();
    let trailers = HeaderBlock::for_trailers(&Status::ok(), &Metadata::new()).unwrap();
    server.send_trailers(stream_id, &trailers.as_h2()).unwrap();

    client.ingest(server.outbound()).unwrap();
    assert!(client.poll_event().is_none());
    let payload = client.poll_unary().unwrap().into_single_payload().unwrap();
    assert_eq!(payload.as_slice(), b"response");
}
