use std::mem;

use dope_test::alloc::Tracker;
use o3::alloc;
use sark_ws::{fragment, inbound};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

pub mod common;

use common::protocol::{self, Prepare as _};

#[test]
fn pending_size() {
    assert_eq!(
        mem::size_of::<inbound::Inbound<inbound::ClientPeer>>(),
        6 * mem::size_of::<usize>()
    );
}

#[test]
fn client_ctl() {
    let mut input = inbound::Inbound::<inbound::ClientPeer>::new(usize::MAX);
    let head = [0x89, 0xfe, 0x00, 0x7e, 1, 2, 3, 4];
    assert!(matches!(
        input.decode(&head, usize::MAX),
        Err(inbound::Fault::Control {
            fin: true,
            length: 126
        })
    ));
}

#[test]
fn server_ctl() {
    let mut input = inbound::Inbound::<inbound::ServerPeer>::new(usize::MAX);
    let head = [0x89, 0x7e, 0x00, 0x7e];
    assert!(matches!(
        input.decode(&head, usize::MAX),
        Err(inbound::Fault::Control {
            fin: true,
            length: 126
        })
    ));
}

#[test]
fn pending_work() {
    let mut input = inbound::Inbound::<inbound::ClientPeer>::new(usize::MAX);
    let head = [0x82, 0xfe, 0x01, 0x00, 1, 2, 3, 4];
    let (more, alloc) = Tracker::usage(|| {
        matches!(
            input.decode(&head, usize::MAX),
            Ok(inbound::Decode::NeedMore)
        )
    });
    assert!(more);
    assert_eq!(alloc, (0, 0));
    assert!(matches!(
        input.decode(&[0xff, 0xff], usize::MAX),
        Ok(inbound::Decode::NeedMore)
    ));
}

#[test]
fn direct_borrow() {
    let mut input = inbound::Inbound::<inbound::ServerPeer>::new(usize::MAX);
    let wire = [0x81, 2, b'o', b'k'];
    let want = wire[2..].as_ptr();
    let ready = match input.try_prepare(&wire) {
        Ok(Some(inbound::Prepared::Message(ready))) => Some(ready),
        _ => None,
    }
    .expect("data frame cannot require a reply");
    let data = match ready.commit() {
        fragment::Message::WholeText(data) => Some(data),
        _ => None,
    }
    .expect("complete text must remain direct");
    assert_eq!(data.as_ptr(), want);
}

#[test]
fn ping_retry() {
    let mut input = inbound::Inbound::<inbound::ServerPeer>::new(usize::MAX);
    let wire = [0x89, 2, b'o', b'k'];

    let need = match input.try_prepare(&wire) {
        Ok(Some(inbound::Prepared::Required(need))) => Some(need),
        _ => None,
    }
    .expect("ping requires a pong");
    let held = need.defer();
    assert_eq!(held.reply.opcode(), 0xa);
    assert_eq!(held.reply.payload(), b"ok");

    let need = match input.try_prepare(&wire) {
        Ok(Some(inbound::Prepared::Required(need))) => Some(need),
        _ => None,
    }
    .expect("ping must remain retryable");
    let held = need.defer();
    assert_eq!(held.reply.opcode(), 0xa);
    assert_eq!(held.reply.payload(), b"ok");
    assert!(matches!(held.event, inbound::Event::Ping(b"ok")));
}

#[test]
fn close_retry() {
    let mut input = inbound::Inbound::<inbound::ServerPeer>::new(usize::MAX);
    let wire = [0x88, 5, 0x03, 0xe8, b'b', b'y', b'e'];

    let need = match input.try_prepare(&wire) {
        Ok(Some(inbound::Prepared::Required(need))) => Some(need),
        _ => None,
    }
    .expect("close requires a reply");
    let held = need.defer();
    assert_eq!(held.reply.opcode(), 0x8);
    assert_eq!(held.reply.payload(), &wire[2..]);

    let need = match input.try_prepare(&wire) {
        Ok(Some(inbound::Prepared::Required(need))) => Some(need),
        _ => None,
    }
    .expect("close must remain retryable");
    let held = need.defer();
    let close = match held.event {
        inbound::Event::Close(close) => Some(close),
        _ => None,
    }
    .expect("close event must retain its kind");
    assert_eq!(close.code(), Some(1000));
    assert_eq!(close.reason(), "bye");
}

#[test]
fn split_text() {
    for (first, last, expected) in protocol::SPLIT_TEXT {
        let mut input = inbound::Inbound::<inbound::ServerPeer>::new(usize::MAX);
        let ready = match input.try_prepare(first) {
            Ok(Some(inbound::Prepared::Fragment(ready))) => Some(ready),
            _ => None,
        }
        .expect("text fragment cannot require a reply");
        ready.commit();

        let Ok(expected) = expected else {
            assert_eq!(
                input.try_prepare(last).err(),
                expected.err().map(inbound::Fault::Text)
            );
            continue;
        };
        drop(input.try_prepare(last));
        let ready = match input.try_prepare(last) {
            Ok(Some(inbound::Prepared::Message(ready))) => Some(ready),
            _ => None,
        }
        .expect("continuation cannot require a reply");
        let text = match ready.commit() {
            fragment::Message::JoinedText(text) => Some(text),
            _ => None,
        }
        .expect("final text must assemble");
        assert_eq!(text, expected);
    }
}

#[test]
fn bad_text() {
    let mut input = inbound::Inbound::<inbound::ServerPeer>::new(usize::MAX);
    let first = [0x01, 1, b'a'];
    let ready = match input.try_prepare(&first) {
        Ok(Some(inbound::Prepared::Fragment(ready))) => Some(ready),
        _ => None,
    }
    .expect("text fragment cannot require a reply");
    ready.commit();

    let bad = [0x80, 1, 0xff];
    for pass in 0..2 {
        let frame = match input
            .decode(&bad, usize::MAX)
            .expect("invalid continuation retry must decode")
        {
            inbound::Decode::Ready(frame) => Some(frame),
            inbound::Decode::NeedMore => None,
        }
        .expect("invalid continuation retry must be admitted for validation");
        let range = frame.range();
        let error = (frame.prepare(&bad[range]))
            .err()
            .expect("invalid continuation retry must be rejected");
        assert!(
            error
                == inbound::Fault::Text(fragment::Utf8 {
                    valid: 1,
                    error: Some(1),
                }),
            "retry {pass}"
        );
    }
}

#[test]
fn invalid_split_tail() {
    use inbound::{Decode, Fault, Inbound, Prepared, ServerPeer};
    let mut input = Inbound::<ServerPeer>::new(usize::MAX);
    let first = match input.try_prepare(&[0x01, 1, 0xe2]).expect("frame") {
        Some(Prepared::Fragment(first)) => Some(first),
        _ => None,
    }
    .expect("text");
    first.commit();
    let last = match input.decode(&[0x80, 1, b'A'], usize::MAX).expect("frame") {
        Decode::Ready(last) => Some(last),
        Decode::NeedMore => None,
    }
    .expect("frame");
    let error = fragment::Utf8 {
        valid: 0,
        error: Some(1),
    };
    assert_eq!(last.prepare(b"A").err(), Some(Fault::Text(error)));
}

#[test]
fn fragment_retry() {
    use common::protocol::{TEXT_PREFIX, TEXT_SUFFIX};

    let mut input = inbound::Inbound::<inbound::ServerPeer>::new(usize::MAX);
    let error = inbound::Fault::Allocation(alloc::Error::Exhausted);
    assert_eq!(
        Tracker::fail_after(0, || input.try_prepare(TEXT_PREFIX)).err(),
        Some(error),
    );
    assert_eq!(error.close_code(), 1011);
    let first = match input.try_prepare(TEXT_PREFIX).expect("retry") {
        Some(inbound::Prepared::Fragment(first)) => Some(first),
        _ => None,
    }
    .expect("fragment");
    let (_, usage) = Tracker::usage(|| first.commit());
    assert_eq!(usage, (0, 0));

    assert_eq!(
        Tracker::fail_after(0, || input.try_prepare(TEXT_SUFFIX)).err(),
        Some(error),
    );
    if let Err(error) = input.try_prepare(TEXT_SUFFIX) {
        panic!("reserve without commit: {error:?}");
    }
    let last = match input.try_prepare(TEXT_SUFFIX).expect("retry") {
        Some(inbound::Prepared::Message(last)) => Some(last),
        _ => None,
    }
    .expect("message");
    let (event, usage) = Tracker::usage(|| last.commit());
    assert_eq!(usage, (0, 0));
    assert!(matches!(
        event,
        fragment::Message::JoinedText(text) if text == "€abcdefghijklmnop"
    ));
}
