use sark_http::field::literal;

use crate::common::{
    conn::{
        Connection as _, Events,
        payload::{Capture, Data as _, Owned as _, Write as _},
    },
    field::entry,
    stream::Stream as _,
};

pub mod common;

use std::mem::size_of;

use dope_quic::wire::{stream::Id, varint::VarInt};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection, INLINE_PREFIX_CAPACITY,
        event::Role,
        io::{Receive, Send},
        output::WritePrefix,
    },
    frame::{Header, Payload, TYPE_DATA, TYPE_HEADERS, TYPE_PUSH_PROMISE},
    link::{Flush, Transport},
};
use sark_http::payload::response::Body as _;

#[test]
fn owned_data_segments() {
    let stream = Id::ZERO;
    let mut sender =
        Connection::with_role(Role::Server).expect("fixed owned-payload scenario must succeed");
    let body = b"owned response body".to_vec();
    let body_ptr = body.as_ptr();

    Send::new(&mut sender)
        .owned(stream, body, Boundary::End)
        .expect("fixed owned-payload scenario must succeed");

    let write = sender
        .events()
        .poll_write()
        .expect("fixed owned-payload scenario must succeed")
        .expect("fixed owned-payload scenario must succeed");
    let frame = Header::parse(&write.prefix).expect("fixed owned-payload scenario must succeed");
    assert_eq!(frame.length.get(), 19);
    assert_eq!(size_of::<WritePrefix>(), 32);
    assert!(matches!(&write.prefix, WritePrefix::Inline(_)));
    let boundary = write.boundary();
    let body = write
        .payload
        .into_owned()
        .expect("DATA retains its owned body segment");
    assert_eq!(body.as_ptr(), body_ptr);
    assert_eq!(body, b"owned response body");
    assert!(matches!(boundary, Boundary::End));
}

#[test]
fn small_fields_inline() {
    let stream = Id::ZERO;
    let mut sender =
        Connection::with_role(Role::Server).expect("fixed owned-payload scenario must succeed");

    let charge = Send::new(&mut sender)
        .headers(
            stream,
            [
                literal!(entry(b":status", b"200")),
                literal!(entry(b"content-type", b"text/plain")),
            ],
            Boundary::More,
        )
        .expect("fixed owned-payload scenario must succeed");

    let write = sender
        .events()
        .poll_write()
        .expect("fixed owned-payload scenario must succeed")
        .expect("fixed owned-payload scenario must succeed");
    assert_eq!(write.charge(), Some(charge));
    let frame = Header::parse(&write.prefix).expect("fixed owned-payload scenario must succeed");
    assert!(frame.kind == TYPE_HEADERS);
    assert!(matches!(&write.prefix, WritePrefix::Inline(_)));
    assert_eq!(
        frame.length.get() as usize,
        write.prefix.len() - frame.header_len
    );
    assert!(write.payload.is_none());
}

#[test]
fn large_fields_segmented() {
    let stream = Id::ZERO;
    let mut sender =
        Connection::with_role(Role::Server).expect("fixed owned-payload scenario must succeed");
    let value = [b'x'; INLINE_PREFIX_CAPACITY];

    let charge = Send::new(&mut sender)
        .headers(
            stream,
            [
                literal!(entry(b":status", b"200")),
                entry(b"x-large", &value).expect("field fixture"),
            ],
            Boundary::More,
        )
        .expect("fixed owned-payload scenario must succeed");

    let write = sender
        .events()
        .poll_write()
        .expect("fixed owned-payload scenario must succeed")
        .expect("fixed owned-payload scenario must succeed");
    assert_eq!(write.charge(), Some(charge));
    let frame = Header::parse(&write.prefix).expect("fixed owned-payload scenario must succeed");
    let fields = write
        .payload
        .into_owned()
        .expect("large QPACK field line owns its segment");
    assert!(matches!(&write.prefix, WritePrefix::Inline(_)));
    assert_eq!(
        frame.length.get() as usize,
        write.prefix.len() - frame.header_len + fields.len()
    );
}

#[test]
fn small_push_inline() {
    let stream = Id::ZERO;
    let mut sender =
        Connection::with_role(Role::Server).expect("fixed owned-payload scenario must succeed");
    sender.allow_push(Role::Server, 64).expect("push grant");

    let charge = Send::new(&mut sender)
        .promise(
            stream,
            64,
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":path", b"/asset")),
            ],
        )
        .expect("fixed owned-payload scenario must succeed");

    let write = sender
        .events()
        .poll_write()
        .expect("fixed owned-payload scenario must succeed")
        .expect("fixed owned-payload scenario must succeed");
    assert_eq!(write.charge(), Some(charge));
    let frame = Header::parse(&write.prefix).expect("fixed owned-payload scenario must succeed");
    assert!(frame.kind == TYPE_PUSH_PROMISE);
    assert!(matches!(&write.prefix, WritePrefix::Inline(_)));
    let (push_id, id_len) = VarInt::decode(&write.prefix[frame.header_len..])
        .expect("fixed owned-payload scenario must succeed");
    assert_eq!(push_id.get(), 64);
    assert!(write.prefix.len() > frame.header_len + id_len);
    assert_eq!(
        frame.length.get() as usize,
        write.prefix.len() - frame.header_len
    );
    assert!(write.payload.is_none());

    assert!(matches!(
        Payload::parse(&write.prefix, usize::MAX),
        Ok((
            Payload::PushPromise { push_id, block: _ },
            _
        )) if push_id.get() == 64
    ));
}

#[test]
fn owned_event_keeps_alloc() {
    let stream = Id::ZERO;
    let mut sender =
        Connection::with_role(Role::Client).expect("fixed owned-payload scenario must succeed");
    let (mut receiver, charge, actual, event) =
        Connection::try_request(stream).expect("request fixture");
    assert_eq!(actual, Some(charge));
    assert!(event == Some(Id::ZERO.into()));

    Send::new(&mut sender)
        .data(stream, b"transport-owned", Boundary::More)
        .expect("fixed owned-payload scenario must succeed");
    let wire = sender
        .events()
        .poll_write()
        .expect("fixed owned-payload scenario must succeed")
        .expect("fixed owned-payload scenario must succeed")
        .into_bytes();
    let head_len = Header::parse(&wire)
        .expect("fixed owned-payload scenario must succeed")
        .header_len;
    let data_ptr = wire.as_ptr().wrapping_add(head_len);

    Receive::new(&mut receiver)
        .owned(stream, wire, Boundary::More)
        .expect("fixed owned-payload scenario must succeed");

    let data = Events::new(&mut receiver)
        .poll_token()
        .expect("fixed owned-payload scenario must succeed")
        .into_data()
        .expect("complete DATA frame emits one data event");
    let mut segments = data.segments();
    let segment = segments
        .next()
        .expect("fixed owned-payload scenario must succeed");
    assert_eq!(segment, b"transport-owned");
    assert_eq!(segment.as_ptr(), data_ptr);
    assert!(segments.next().is_none());
}

#[test]
fn fragment_keeps_alloc() {
    let stream = Id::ZERO;
    let (mut receiver, charge, actual, event) =
        Connection::try_request(stream).expect("request fixture");
    assert_eq!(actual, Some(charge));
    assert!(event == Some(Id::ZERO.into()));
    let mut wire = Vec::with_capacity(128);
    Payload::encode(TYPE_DATA, b"fragmented-body", &mut wire)
        .expect("fixed owned-payload scenario must succeed");
    let head_len = Header::parse(&wire)
        .expect("fixed owned-payload scenario must succeed")
        .header_len;
    let tail = wire.split_off(head_len + 3);
    let data_ptr = wire.as_ptr().wrapping_add(head_len);
    let tail_ptr = tail.as_ptr();

    Receive::new(&mut receiver)
        .owned(stream, wire, Boundary::More)
        .expect("fixed owned-payload scenario must succeed");
    let first = Events::new(&mut receiver)
        .poll_token()
        .expect("first fragment event")
        .into_data()
        .expect("DATA streams before the entire frame arrives");
    let mut segments = first.segments();
    let data = segments.next().expect("first fragment");
    assert_eq!(data, b"fra");
    assert_eq!(data.as_ptr(), data_ptr);
    assert!(segments.next().is_none());
    Receive::new(&mut receiver)
        .owned(stream, tail, Boundary::More)
        .expect("fixed owned-payload scenario must succeed");

    let data = Events::new(&mut receiver)
        .poll_token()
        .expect("fixed owned-payload scenario must succeed")
        .into_data()
        .expect("the remaining DATA emits a second event");
    let mut segments = data.segments();
    let second = segments
        .next()
        .expect("fixed owned-payload scenario must succeed");
    assert_eq!(second, b"gmented-body");
    assert_eq!(second.as_ptr(), tail_ptr);
    assert!(segments.next().is_none());
}

#[test]
fn adjacent_events_share() {
    let stream = Id::ZERO;
    let (mut receiver, charge, actual, event) =
        Connection::try_request(stream).expect("request fixture");
    assert_eq!(actual, Some(charge));
    assert!(event == Some(Id::ZERO.into()));
    let first = vec![b'a'; 300];
    let second = vec![b'b'; 300];
    let mut wire = Vec::with_capacity(640);
    Payload::encode(TYPE_DATA, &first, &mut wire)
        .expect("fixed owned-payload scenario must succeed");
    let offset = wire.len();
    Payload::encode(TYPE_DATA, &second, &mut wire)
        .expect("fixed owned-payload scenario must succeed");
    let first_h = Header::parse(&wire)
        .expect("fixed owned-payload scenario must succeed")
        .header_len;
    let second_h = Header::parse(&wire[offset..])
        .expect("fixed owned-payload scenario must succeed")
        .header_len;
    let base = wire.as_ptr();

    Receive::new(&mut receiver)
        .owned(stream, wire, Boundary::More)
        .expect("fixed owned-payload scenario must succeed");

    let first = Events::new(&mut receiver)
        .poll_token()
        .expect("fixed owned-payload scenario must succeed")
        .into_data()
        .expect("first frame emits the first data event");
    let second = Events::new(&mut receiver)
        .poll_token()
        .expect("fixed owned-payload scenario must succeed")
        .into_data()
        .expect("second frame emits the second data event");
    let first = first
        .segments()
        .next()
        .expect("fixed owned-payload scenario must succeed");
    let second = second
        .segments()
        .next()
        .expect("fixed owned-payload scenario must succeed");
    assert_eq!(first.as_ptr(), base.wrapping_add(first_h));
    assert_eq!(second.as_ptr(), base.wrapping_add(offset + second_h));
}

#[test]
fn wire_prefixes_inline() {
    let stream = Id::ZERO;
    let mut sender =
        Connection::with_role(Role::Server).expect("fixed owned-payload scenario must succeed");
    let charge = Send::new(&mut sender)
        .headers(
            stream,
            [literal!(entry(b":status", b"200"))],
            Boundary::More,
        )
        .expect("fixed owned-payload scenario must succeed");
    assert!(charge.is_empty());
    Send::new(&mut sender)
        .owned(stream, b"body".to_vec(), Boundary::End)
        .expect("fixed owned-payload scenario must succeed");
    let mut capture = Capture::default();

    assert!(
        capture.pump_writes(&mut sender) == Ok(Flush::Drained),
        "fixed owned-payload scenario must succeed"
    );

    assert_eq!(capture.inline_writes, 2);
    assert_eq!(capture.owned_writes, 1);
    assert_eq!(capture.finishes, 1);
}
