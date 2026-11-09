use common::codec::{Retain, Scope as _, decoder::Decoder, only};
use o3::buffer::resident::staging::{Buf, Layout, Plan, Pool};
use sark_client::protocol::response::Event;
use sark_http::h1::head::Boundary;

pub mod common;

use dope::manifold::client::session::codec;
use dope_test::alloc::Tracker;
use o3::buffer::{Residency as _, resident, storage, storage::Shared, write::Sink as _};
use sark_client::{
    protocol,
    protocol::codec::{Codec, Limits, output},
};

use crate::common::codec::Gate;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retention_is_atomic() {
    resident::Domain::try_scope(128, |budget| {
        let codec = Codec::default();

        {
            let source =
                (Shared::try_copy(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-Proof: yes\r\n\r\n"))
                    .expect("source fixture storage");
            let mut state = codec::Stateful::state(&codec, budget);
            let decoder = Decoder::try_new(&codec).expect("codec staging");
            let mut stored = decoder.storage(&source, &state);
            let blocker = storage::Owned::try_allocate(&budget, budget.available())
                .expect("resident blocker");
            Retain {
                result: decoder.try_stored(&mut state, &mut stored, &budget),
                budget,
            }
            .try_verify()
            .ok()
            .expect("exact resident retention error");
            drop(blocker);
            let (parsed, _) = (decoder
                .try_stored(&mut state, &mut stored, &budget)
                .expect("codec parse"))
            .expect("head retention retry must parse");
            assert!(matches!(
                only(parsed),
                Some(Ok(protocol::response::Event::Head(_)))
            ));
        }

        {
            let mut state = codec::Stateful::state(&codec, budget);
            let head = (Shared::try_copy(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n"))
                .expect("head fixture storage");
            assert!(
                Decoder::try_new(&codec)
                    .expect("codec staging")
                    .try_parse(&mut state, &head, &budget)
                    .expect("codec parse")
                    .is_some()
            );
            let body = (Shared::try_copy(b"hello")).expect("body fixture storage");
            let decoder = Decoder::try_new(&codec).expect("codec staging");
            let mut stored = decoder.storage(&body, &state);
            let blocker = storage::Owned::try_allocate(&budget, budget.available())
                .expect("resident blocker");
            Retain {
                result: decoder.try_stored(&mut state, &mut stored, &budget),
                budget,
            }
            .try_verify()
            .ok()
            .expect("exact resident retention error");
            drop(blocker);
            let (parsed, _) = (decoder
                .try_stored(&mut state, &mut stored, &budget)
                .expect("codec parse"))
            .expect("body retention retry must parse");
            let (data,) = match only(parsed) {
                Some(Ok(Event::Data(data))) => Some((data,)),
                _ => None,
            }
            .expect("body retention retry must emit data");
            assert_eq!(data.as_slice(), b"hello");
        }

        {
            let mut state = codec::Stateful::state(&codec, budget);
            let head = (Shared::try_copy(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n"))
                .expect("head fixture storage");
            assert!(
                Decoder::try_new(&codec)
                    .expect("codec staging")
                    .try_parse(&mut state, &head, &budget)
                    .expect("codec parse")
                    .is_some()
            );
            let body = (storage::Shared::try_from(&mut vec![b'x'; output::DATA_LIMIT]))
                .expect("body backing must retain its owner");
            let body = Gate::closed(body);
            let decoder = Decoder::try_new(&codec).expect("codec staging");
            let mut stored = decoder.storage(&body, &state);

            Retain {
                result: decoder.try_stored(&mut state, &mut stored, &budget),
                budget,
            }
            .try_verify()
            .ok()
            .expect("exact resident retention error");
            assert_eq!(body.plans(), 1);
            body.open();
            let (parsed, _) = (decoder
                .try_stored(&mut state, &mut stored, &budget)
                .expect("codec parse"))
            .expect("retention retry must parse");
            assert_eq!(body.plans(), 2, "retention must reuse the approved prefix");
            let (data,) = match only(parsed) {
                Some(Ok(Event::Data(data))) => Some((data,)),
                _ => None,
            }
            .expect("EOF retention retry must emit data");
            assert_eq!(data.len(), output::DATA_LIMIT);
        }
    })
    .expect("resident test domain");
}

#[test]
fn eof_retry_is_atomic() {
    resident::Domain::try_scope(48, |budget| {
        let planned = Plan::new(8, 8).and_then(|plan| Layout::new(1, plan, 1, 0));
        let layout = (planned).expect("the EOF staging layout must fit");
        let pool = (Pool::try_new(&budget, layout)).expect("the EOF staging pool must allocate");
        let mut window = (Buf::try_from(&pool)).expect("the EOF receive owner must be retained");
        window.write_slice(b"body").expect("EOF body");

        let codec = Codec::default();
        let mut state = codec::Stateful::state(&codec, budget);
        let head = (Shared::try_copy(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n"))
            .expect("head fixture storage");
        assert!(
            Decoder::try_new(&codec)
                .expect("codec staging")
                .try_parse(&mut state, &head, &budget)
                .expect("codec parse")
                .is_some()
        );
        let decoder = Decoder::try_new(&codec).expect("codec staging");
        let stored = decoder.storage(&window, &state);
        let blocker = storage::Owned::try_allocate(&budget, budget.available())
            .expect("resident blocker");
        let ((stored, state), result) = decoder.try_finish(state, stored, &budget);
        Retain { result, budget }
            .try_verify()
            .ok()
            .expect("exact resident retention error");
        assert_eq!(window.as_slice(), b"body");
        drop(blocker);
        let (_owners, finish) = decoder.try_finish(state, stored, &budget);
        let (head,) = match Some(finish) {
            Some(Ok(codec::Finish::Item(head))) => Some((head,)),
            _ => None,
        }
        .expect("retry must complete the EOF body");
        let end = head.completion();
        let (data,) = match only(head) {
            Some(Ok(Event::Data(data))) => Some((data,)),
            _ => None,
        }
        .expect("EOF completion must emit data");
        assert_eq!(data.as_slice(), b"body");
        assert!((end) == (output::Completion::Complete));
    })
    .expect("resident test domain");
}

#[test]
fn chunk_retry_is_atomic() {
    resident::Domain::try_scope(output::DATA_LIMIT, |budget| {
        let codec = Codec::default();
        let mut state = codec::Stateful::state(&codec, budget);
        let head = (Shared::try_copy(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"))
            .expect("head fixture storage");
        let seed = output::DATA_LIMIT * 3 / 4;
        let size_hex = format!("{seed:x}\r\n");
        let mut fragment = Vec::with_capacity(size_hex.len() + seed + 2);
        fragment.extend_from_slice(size_hex.as_bytes());
        fragment.resize(fragment.len() + seed, b'a');
        fragment.extend_from_slice(b"\r\n");
        let fragment = (storage::Shared::try_from(&mut fragment))
            .expect("first chunk backing must retain its owner");
        assert!(
            Decoder::try_new(&codec)
                .expect("codec staging")
                .try_parse(&mut state, &head, &budget)
                .expect("codec parse")
                .is_some()
        );
        let (parsed, allocs) = Tracker::count(|| {
            Decoder::try_new(&codec)
                .expect("codec staging")
                .try_parse(&mut state, &fragment, &budget)
                .expect("codec parse")
        });
        assert_eq!(allocs, 1);
        let (_, consumed) = (parsed).expect("fragment must parse");

        let size = format!("{:x}\r\n", output::DATA_LIMIT);
        let prefix = &fragment.as_slice()[consumed.get()..];
        let mut wire = Vec::with_capacity(prefix.len() + size.len() + output::DATA_LIMIT + 2);
        wire.extend_from_slice(prefix);
        wire.extend_from_slice(size.as_bytes());
        wire.resize(wire.len() + output::DATA_LIMIT, b'x');
        wire.extend_from_slice(b"\r\n");
        let source = (storage::Shared::try_from(&mut wire))
            .expect("second chunk backing must retain its owner");
        let source = Gate::closed(source);
        let decoder = Decoder::try_new(&codec).expect("codec staging");
        let mut stored = decoder.storage(&source, &state);

        Retain {
            result: decoder.try_stored(&mut state, &mut stored, &budget),
            budget,
        }
        .try_verify()
        .ok()
        .expect("exact resident retention error");

        source.open();
        let (parsed, allocs) = Tracker::count(|| {
            decoder
                .try_stored(&mut state, &mut stored, &budget)
                .expect("codec parse")
        });
        assert_eq!(allocs, 0);
        let (head, _) = (parsed).expect("retention retry must parse");
        {
            let mut events = head.into_events();
            let (data,) = match events.next() {
                Some(Ok(Event::Data(data))) => Some((data,)),
                _ => None,
            }
            .expect("coalesced event must contain data");
            let (tail,) = match events.next() {
                Some(Ok(Event::Data(tail))) => Some((tail,)),
                _ => None,
            }
            .expect("retained tail event must contain data");
            assert!(events.next().is_none());
            assert_eq!(data.len(), output::DATA_LIMIT);
            assert!(data.as_slice()[..seed].iter().all(|byte| *byte == b'a'));
            assert!(data.as_slice()[seed..].iter().all(|byte| *byte == b'x'));
            assert_eq!(tail.len(), seed);
            assert!(tail.as_slice().iter().all(|byte| *byte == b'x'));
        }
        assert_eq!(budget.resident(), 0);
    })
    .expect("resident test domain");
}

#[test]
fn invalid_length_fails() {
    resident::Domain::try_scope(0, |budget| {
        let response = (Shared::try_copy(
            b"HTTP/1.1 200 OK\r\nContent-Length: invalid\r\nConnection: close\r\n\r\n",
        ))
        .expect("response fixture storage");
        let codec = Codec::default();
        let mut state = codec::Stateful::state(&codec, budget);

        assert!(matches!(
            Decoder::try_new(&codec)
                .expect("codec staging")
                .try_parse(&mut state, &response, &budget),
            Err(common::codec::decoder::Error::Protocol(
                protocol::Error::Head(_)
            ))
        ));
    })
    .expect("resident test domain");
}

#[test]
fn head_limit_is_separate() {
    resident::Domain::try_scope(0, |budget| {
        let response = (Shared::try_copy(
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-Padding: padding\r\n\r\n",
        ))
        .expect("response fixture storage");
        let codec = Codec::new(Limits::new(32, usize::MAX));
        let mut state = codec::Stateful::state(&codec, budget);

        assert!(matches!(
            Decoder::try_new(&codec)
                .expect("codec staging")
                .try_parse(&mut state, &response, &budget),
            Err(common::codec::decoder::Error::Protocol(
                protocol::Error::HeadLimit(Boundary::TooLarge {
                    actual: 58,
                    limit: 32,
                })
            ))
        ));
    })
    .expect("resident test domain");
}

#[test]
fn small_ranges_compact() {
    resident::Domain::try_scope(128, |budget| {
        let response = (storage::Shared::try_from(
            &mut b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello".to_vec(),
        ))
        .expect("response backing must retain its owner");
        let codec = Codec::default();
        let mut state = codec::Stateful::state(&codec, budget);
        let (parsed, head_len) = (Decoder::try_new(&codec)
            .expect("codec staging")
            .try_parse(&mut state, &response, &budget)
            .expect("codec parse"))
        .expect("response head must parse");
        let (head,) = match only(parsed) {
            Some(Ok(Event::Head(head))) => Some((head,)),
            _ => None,
        }
        .expect("response head must emit a head event");
        let value = (head.headers().get("content-length")).expect("content-length");
        assert_eq!(value, b"5");
        assert_ne!(value.as_ptr(), response.as_slice()[33..].as_ptr());
        let body = (response.try_get(head_len.get()..))
            .expect("response body")
            .expect("response body");
        let (event, consumed) = (Decoder::try_new(&codec)
            .expect("codec staging")
            .try_parse(&mut state, &body, &budget)
            .expect("codec parse"))
        .expect("response body must parse");
        assert_eq!(consumed.get(), 5);
        let (data,) = match only(event) {
            Some(Ok(Event::Data(data))) => Some((data,)),
            _ => None,
        }
        .expect("response body must emit data");
        assert_eq!(data.as_slice(), b"hello");
        assert_ne!(data.as_slice().as_ptr(), body.as_slice().as_ptr());
        assert_eq!(data.resident(), 5);
    })
    .expect("resident test domain");
}

#[test]
fn large_chunk_is_shared() {
    resident::Domain::try_scope(output::DATA_LIMIT * 2, |budget| {
        let size = format!("{:x}\r\n", output::DATA_LIMIT);
        let mut wire = Vec::with_capacity(size.len() + output::DATA_LIMIT + 2);
        wire.extend_from_slice(size.as_bytes());
        wire.resize(wire.len() + output::DATA_LIMIT, b'x');
        wire.extend_from_slice(b"\r\n");
        let wire =
            (storage::Shared::try_from(&mut wire)).expect("chunk backing must retain its owner");

        let codec = Codec::default();
        let mut state = codec::Stateful::state(&codec, budget);
        let head = (Shared::try_copy(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"))
            .expect("head fixture storage");
        assert!(
            Decoder::try_new(&codec)
                .expect("codec staging")
                .try_parse(&mut state, &head, &budget)
                .expect("codec parse")
                .is_some()
        );
        let (parsed, allocs) = Tracker::count(|| {
            Decoder::try_new(&codec)
                .expect("codec staging")
                .try_parse(&mut state, &wire, &budget)
                .expect("codec parse")
        });
        let (event, _) = (parsed).expect("chunk event must parse");
        assert_eq!(allocs, 0);
        let (data,) = match only(event) {
            Some(Ok(Event::Data(data))) => Some((data,)),
            _ => None,
        }
        .expect("chunk event must emit data");
        assert_eq!(data.len(), output::DATA_LIMIT);
        assert_eq!(
            data.as_slice().as_ptr(),
            wire.as_slice()[size.len()..].as_ptr()
        );
    })
    .expect("resident test domain");
}
