use sark_core::http::Field;
use sark_h3::{Conn, Event, FrameHeader, Role, StreamId};

#[test]
fn owned_data_event_retains_the_transport_allocation() {
    let stream_id = StreamId::new(0);
    let mut sender = Conn::with_role(Role::Client);
    let mut receiver = Conn::with_role(Role::Server);

    sender
        .send_headers(
            stream_id,
            [
                Field::new(b":method", b"POST"),
                Field::new(b":scheme", b"https"),
                Field::new(b":authority", b"example.test"),
                Field::new(b":path", b"/upload"),
            ],
            false,
        )
        .unwrap();
    let headers = sender.poll_write().unwrap();
    receiver
        .ingest_stream_owned(stream_id, headers.bytes, false)
        .unwrap();
    assert!(matches!(
        receiver.poll_event(),
        Some(Event::Headers {
            stream_id: StreamId(0),
            ..
        })
    ));

    sender
        .send_data(stream_id, b"transport-owned", false)
        .unwrap();
    let data = sender.poll_write().unwrap().bytes;
    let header_len = FrameHeader::parse(&data).unwrap().header_len;
    let payload_ptr = data.as_ptr().wrapping_add(header_len);

    receiver
        .ingest_stream_owned(stream_id, data, false)
        .unwrap();

    let Some(Event::Data { data, .. }) = receiver.poll_event() else {
        panic!("DATA event");
    };
    assert_eq!(data.as_slice(), b"transport-owned");
    assert_eq!(data.as_slice().as_ptr(), payload_ptr);
}
