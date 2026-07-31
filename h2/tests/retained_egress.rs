use o3::buffer::{Owned, Shared};
use sark_h2::conn::{Event, OutboundWrite};
use sark_h2::hpack::Header;
use sark_h2::{ClientRole, Conn, ServerRole};

fn connected_request() -> (Conn<ClientRole>, Conn<ServerRole>, sark_h2::StreamId) {
    let mut client = Conn::<ClientRole>::new();
    let stream = client
        .start_request(
            &[
                Header::new(b":method", b"GET"),
                Header::new(b":scheme", b"http"),
                Header::new(b":path", b"/"),
                Header::new(b":authority", b"localhost"),
            ],
            true,
        )
        .unwrap();
    let mut server = Conn::<ServerRole>::new();
    server.ingest(client.outbound()).unwrap();
    while server.poll_event().is_some() {}
    server.drain_outbound(usize::MAX);
    (client, server, stream)
}

#[test]
fn shared_data_crosses_the_h2_write_boundary_without_copying() {
    let (mut client, mut server, stream) = connected_request();
    server
        .send_response(stream, [Header::new(b":status", b"200")], false)
        .unwrap();
    let body = Owned::try_filled(4096, b'x').unwrap().freeze();
    let body_ptr = body.as_slice().as_ptr();

    assert_eq!(server.send_data_shared(stream, &body, true).unwrap(), 4096);

    let mut prefix = [0; 16 * 1024];
    let OutboundWrite::Split { prefix_len, body } = server.take_write(&mut prefix) else {
        panic!("shared DATA must be emitted as a split write");
    };
    assert_eq!(body.as_slice().as_ptr(), body_ptr);
    assert_eq!(body.len(), 4096);

    client.ingest(&prefix[..prefix_len]).unwrap();
    client
        .ingest_retained(o3::buffer::Bytes::from(body))
        .unwrap();
    let data = std::iter::from_fn(|| client.poll_event())
        .find_map(|event| match event {
            Event::Data {
                data, end_stream, ..
            } => Some((data, end_stream)),
            _ => None,
        })
        .unwrap();
    assert_eq!(data.0.as_ref(), &[b'x'; 4096]);
    assert!(data.1);
}

#[test]
fn buffered_prefixes_remain_ordered_between_shared_payloads() {
    let (_client, mut server, stream) = connected_request();
    server
        .send_response(stream, [Header::new(b":status", b"200")], false)
        .unwrap();
    let first = Shared::from_static(b"first");
    let second = Shared::from_static(b"second");
    server.send_data_shared(stream, &first, false).unwrap();
    server.ping([7; 8]).unwrap();
    server.send_data_shared(stream, &second, true).unwrap();
    let mut prefix = [0; 1024];

    let OutboundWrite::Split { body, .. } = server.take_write(&mut prefix) else {
        panic!("first shared DATA write");
    };
    assert_eq!(body.as_slice(), b"first");
    let OutboundWrite::Split { prefix_len, body } = server.take_write(&mut prefix) else {
        panic!("second shared DATA write");
    };
    assert!(prefix_len > sark_h2::frame::HEADER_LEN);
    assert_eq!(body.as_slice(), b"second");
}
