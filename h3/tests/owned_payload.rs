use std::mem::size_of;

use dope_quic::varint::VarInt;
use sark_core::http::Field;
use sark_h3::{
    Conn, Event, Frame, FrameHeader, INLINE_PREFIX_CAPACITY, InlineBytes, Role, StreamId,
    StreamTransport, TYPE_DATA, TYPE_HEADERS, TYPE_PUSH_PROMISE, WritePayload, WritePrefix,
    pump_writes,
};

#[test]
fn owned_response_data_keeps_its_allocation_segmented() {
    let stream_id = StreamId::new(0);
    let mut sender = Conn::with_role(Role::Server);
    let body = b"owned response body".to_vec();
    let body_ptr = body.as_ptr();

    sender.send_data_owned(stream_id, body, true).unwrap();

    let write = sender.poll_write().unwrap();
    let frame = FrameHeader::parse(&write.prefix).unwrap();
    assert_eq!(frame.length.get(), 19);
    assert_eq!(size_of::<WritePrefix>(), 32);
    assert!(matches!(&write.prefix, WritePrefix::Inline(_)));
    let Some(WritePayload::Owned(body)) = write.payload else {
        panic!("owned DATA body segment");
    };
    assert_eq!(body.as_ptr(), body_ptr);
    assert_eq!(body, b"owned response body");
    assert!(write.fin);
}

#[test]
fn small_qpack_field_section_stays_inline() {
    let stream_id = StreamId::new(0);
    let mut sender = Conn::with_role(Role::Server);

    sender
        .send_headers(
            stream_id,
            [
                Field::new(b":status", b"200"),
                Field::new(b"content-type", b"text/plain"),
            ],
            false,
        )
        .unwrap();

    let write = sender.poll_write().unwrap();
    let frame = FrameHeader::parse(&write.prefix).unwrap();
    assert_eq!(frame.kind, TYPE_HEADERS);
    assert!(matches!(&write.prefix, WritePrefix::Inline(_)));
    assert_eq!(
        frame.length.get() as usize,
        write.prefix.len() - frame.header_len
    );
    assert!(write.payload.is_none());
}

#[test]
fn large_qpack_field_section_stays_segmented() {
    let stream_id = StreamId::new(0);
    let mut sender = Conn::with_role(Role::Server);
    let value = [b'x'; INLINE_PREFIX_CAPACITY];

    sender
        .send_headers(
            stream_id,
            [
                Field::new(b":status", b"200"),
                Field::new(b"x-large", &value),
            ],
            false,
        )
        .unwrap();

    let write = sender.poll_write().unwrap();
    let frame = FrameHeader::parse(&write.prefix).unwrap();
    let Some(WritePayload::Owned(field_lines)) = write.payload else {
        panic!("large QPACK field-line segment");
    };
    assert!(matches!(&write.prefix, WritePrefix::Inline(_)));
    assert_eq!(
        frame.length.get() as usize,
        write.prefix.len() - frame.header_len + field_lines.len()
    );
}

#[test]
fn small_push_promise_stays_inline() {
    let stream_id = StreamId::new(0);
    let mut sender = Conn::with_role(Role::Server);

    sender
        .send_push_promise(
            stream_id,
            64,
            [
                Field::new(b":method", b"GET"),
                Field::new(b":path", b"/asset"),
            ],
        )
        .unwrap();

    let write = sender.poll_write().unwrap();
    let frame = FrameHeader::parse(&write.prefix).unwrap();
    assert_eq!(frame.kind, TYPE_PUSH_PROMISE);
    assert!(matches!(&write.prefix, WritePrefix::Inline(_)));
    let (push_id, push_id_len) = VarInt::decode(&write.prefix[frame.header_len..]).unwrap();
    assert_eq!(push_id.get(), 64);
    assert!(write.prefix.len() > frame.header_len + push_id_len);
    assert_eq!(
        frame.length.get() as usize,
        write.prefix.len() - frame.header_len
    );
    assert!(write.payload.is_none());

    assert!(matches!(
        Frame::parse(&write.prefix, usize::MAX),
        Ok((
            Frame::PushPromise { push_id, block: _ },
            _
        )) if push_id.get() == 64
    ));
}

#[test]
fn owned_data_event_retains_the_transport_allocation() {
    let stream_id = StreamId::new(0);
    let mut sender = Conn::with_role(Role::Client);
    let mut receiver = request_receiver(stream_id);

    sender
        .send_data(stream_id, b"transport-owned", false)
        .unwrap();
    let data = joined_write(sender.poll_write().unwrap());
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

#[test]
fn fragmented_data_keeps_the_first_transport_allocation() {
    let stream_id = StreamId::new(0);
    let mut receiver = request_receiver(stream_id);
    let mut wire = Vec::with_capacity(128);
    Frame::encode(TYPE_DATA, b"fragmented-body", &mut wire).unwrap();
    let header_len = FrameHeader::parse(&wire).unwrap().header_len;
    let tail = wire.split_off(header_len + 3);
    let payload_ptr = wire.as_ptr().wrapping_add(header_len);

    receiver
        .ingest_stream_owned(stream_id, wire, false)
        .unwrap();
    assert!(receiver.poll_event().is_none());
    receiver
        .ingest_stream_owned(stream_id, tail, false)
        .unwrap();

    let Some(Event::Data { data, .. }) = receiver.poll_event() else {
        panic!("DATA event");
    };
    assert_eq!(data.as_slice(), b"fragmented-body");
    assert_eq!(data.as_slice().as_ptr(), payload_ptr);
}

#[test]
fn adjacent_data_events_share_one_transport_allocation() {
    let stream_id = StreamId::new(0);
    let mut receiver = request_receiver(stream_id);
    let first = vec![b'a'; 300];
    let second = vec![b'b'; 300];
    let mut wire = Vec::with_capacity(640);
    Frame::encode(TYPE_DATA, &first, &mut wire).unwrap();
    let second_frame = wire.len();
    Frame::encode(TYPE_DATA, &second, &mut wire).unwrap();
    let first_header = FrameHeader::parse(&wire).unwrap().header_len;
    let second_header = FrameHeader::parse(&wire[second_frame..])
        .unwrap()
        .header_len;
    let allocation = wire.as_ptr();

    receiver
        .ingest_stream_owned(stream_id, wire, false)
        .unwrap();

    let Some(Event::Data { data: first, .. }) = receiver.poll_event() else {
        panic!("first DATA event");
    };
    let Some(Event::Data { data: second, .. }) = receiver.poll_event() else {
        panic!("second DATA event");
    };
    assert_eq!(
        first.as_slice().as_ptr(),
        allocation.wrapping_add(first_header)
    );
    assert_eq!(
        second.as_slice().as_ptr(),
        allocation.wrapping_add(second_frame + second_header)
    );
}

#[derive(Default)]
struct SegmentCapture {
    inline_writes: usize,
    owned_writes: usize,
    finishes: usize,
}

impl StreamTransport for SegmentCapture {
    type SendError = std::convert::Infallible;

    fn recv_stream(&mut self, _stream_id: u64) -> Option<Vec<u8>> {
        None
    }

    fn recv_stream_finished(&self, _stream_id: u64) -> bool {
        false
    }

    fn send_stream(&mut self, _stream_id: u64, _bytes: &[u8]) -> Result<(), Self::SendError> {
        Ok(())
    }

    fn send_stream_owned(
        &mut self,
        _stream_id: u64,
        _bytes: Vec<u8>,
    ) -> Result<(), Self::SendError> {
        self.owned_writes += 1;
        Ok(())
    }

    fn send_stream_inline(
        &mut self,
        _stream_id: u64,
        _bytes: InlineBytes,
    ) -> Result<(), Self::SendError> {
        self.inline_writes += 1;
        Ok(())
    }

    fn finish_stream(&mut self, _stream_id: u64) -> Result<(), Self::SendError> {
        self.finishes += 1;
        Ok(())
    }
}

#[test]
fn transport_keeps_frame_prefixes_inline() {
    let stream_id = StreamId::new(0);
    let mut sender = Conn::with_role(Role::Server);
    sender
        .send_headers(stream_id, [Field::new(b":status", b"200")], false)
        .unwrap();
    sender
        .send_data_owned(stream_id, b"body".to_vec(), true)
        .unwrap();
    let mut capture = SegmentCapture::default();

    pump_writes(&mut sender, &mut capture).unwrap();

    assert_eq!(capture.inline_writes, 2);
    assert_eq!(capture.owned_writes, 1);
    assert_eq!(capture.finishes, 1);
}

#[test]
fn oversized_prefix_promotes_to_owned_storage() {
    let mut prefix = WritePrefix::Inline(InlineBytes::new());

    prefix.extend([0; INLINE_PREFIX_CAPACITY + 1]);

    assert!(matches!(&prefix, WritePrefix::Owned(_)));
    assert_eq!(prefix.len(), INLINE_PREFIX_CAPACITY + 1);
}

fn joined_write(write: sark_h3::Write) -> Vec<u8> {
    let mut bytes = write.prefix.into_vec();
    if let Some(payload) = write.payload {
        bytes.extend_from_slice(payload.as_slice());
    }
    bytes
}

fn request_receiver(stream_id: StreamId) -> Conn {
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
    receiver
        .ingest_stream_owned(stream_id, joined_write(sender.poll_write().unwrap()), false)
        .unwrap();
    assert!(matches!(
        receiver.poll_event(),
        Some(Event::Headers {
            stream_id: StreamId(0),
            ..
        })
    ));
    receiver
}
