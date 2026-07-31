use o3::buffer::{Bytes, Leased, Retained, Shared, SharedPool};
use sark_h2::conn::Event;
use sark_h2::hpack::Header;
use sark_h2::{ClientRole, Conn, ServerRole};

fn request_with_data() -> (Vec<u8>, usize) {
    let mut client = Conn::<ClientRole>::new();
    let stream = client
        .start_request(
            &[
                Header::new(b":method", b"POST"),
                Header::new(b":scheme", b"http"),
                Header::new(b":path", b"/"),
                Header::new(b":authority", b"localhost"),
            ],
            false,
        )
        .unwrap();
    client.send_data(stream, b"retained-payload", true).unwrap();
    let wire = client.outbound().to_vec();
    let payload_offset = wire
        .windows(b"retained-payload".len())
        .position(|bytes| bytes == b"retained-payload")
        .unwrap();
    (wire, payload_offset)
}

#[test]
fn retained_data_event_slices_the_receive_owner() {
    let (wire, payload_offset) = request_with_data();
    let pool = SharedPool::try_new(1, wire.len()).unwrap();
    let mut lease = pool.try_acquire().unwrap();
    lease.spare_writer().try_extend_from_slice(&wire).unwrap();
    let pooled = lease.freeze();
    let expected_payload = pooled.as_slice().as_ptr() as usize + payload_offset;
    let retained = Bytes::<Leased>::from(pooled).into_retained();
    let mut server = Conn::<ServerRole>::new();

    server.ingest_retained(retained).unwrap();

    let data = std::iter::from_fn(|| server.poll_event())
        .find_map(|event| match event {
            Event::Data { data, .. } => Some(data),
            _ => None,
        })
        .unwrap();
    assert_eq!(data.as_ptr() as usize, expected_payload);
    assert_eq!(&*data, b"retained-payload");
}

#[test]
fn fragmented_data_is_joined_only_when_the_payload_crosses_owners() {
    let (wire, payload_offset) = request_with_data();
    let split = payload_offset + 4;
    let first = Bytes::<Retained>::from(Shared::copy_from_slice(&wire[..split]));
    let second = Bytes::<Retained>::from(Shared::copy_from_slice(&wire[split..]));
    let mut server = Conn::<ServerRole>::new();

    server.ingest_retained(first).unwrap();
    server.ingest_retained(second).unwrap();

    let data = std::iter::from_fn(|| server.poll_event())
        .find_map(|event| match event {
            Event::Data { data, .. } => Some(data),
            _ => None,
        })
        .unwrap();
    assert_eq!(&*data, b"retained-payload");
}
