pub mod common;
use common::codec::{Retain, Scope as _, decoder::Decoder, only};
use dope::manifold::client::session::codec;
use dope_test::alloc::Tracker;
use o3::{
    alloc::Error,
    buffer::{Residency as _, resident, storage, storage::Shared},
};
use sark_client::{
    protocol,
    protocol::{
        codec::{Codec, output},
        response::Event,
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn finish_releases_spare() {
    resident::Domain::try_scope(output::DATA_LIMIT, |budget| {
        let codec = Codec::default();
        let mut state = codec::Stateful::state(&codec, budget);
        let head = (Shared::try_copy(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"))
            .expect("head fixture storage");
        let fragment = (Shared::try_copy(b"3\r\nabc\r\n")).expect("fragment fixture storage");
        assert!(
            Decoder::try_new(&codec)
                .expect("codec staging")
                .try_parse(&mut state, &head, &budget)
                .expect("codec parse")
                .is_some()
        );

        let end = (Shared::try_copy(b"\r\n0\r\n\r\n")).expect("end fixture storage");
        {
            let (data, allocs) = Tracker::count(|| {
                let (head, consumed) = (Decoder::try_new(&codec)
                    .expect("codec staging")
                    .try_parse(&mut state, &fragment, &budget)
                    .expect("codec parse"))
                .expect("fragment must parse");
                assert!(head.into_events().next().is_none());
                assert_eq!(&fragment.as_slice()[consumed.get()..], b"\r\n");
                let (head, _) = (Decoder::try_new(&codec)
                    .expect("codec staging")
                    .try_parse(&mut state, &end, &budget)
                    .expect("codec parse"))
                .expect("chunk terminator must parse");
                let (data,) = match only(head) {
                    Some(Ok(Event::Data(data))) => Some((data,)),
                    _ => None,
                }
                .expect("chunk terminator must emit data");
                Some(data)
            });

            let data = (data).expect("chunk terminator data");
            assert_eq!(allocs, 2);
            assert_eq!(data.as_slice(), b"abc");
            assert_eq!(data.resident(), 3);
            assert_eq!(budget.resident(), 3);
        }
        assert_eq!(budget.resident(), 0);
    })
    .expect("resident test domain");
}

#[test]
fn rejects_incomplete_eof() {
    resident::Domain::try_scope(128, |budget| {
        let codec = Codec::default();
        for head in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
        ] {
            let mut state = codec::Stateful::state(&codec, budget);
            let head = (Shared::try_copy(head)).expect("response head fixture");
            assert!(
                Decoder::try_new(&codec)
                    .expect("codec staging")
                    .try_parse(&mut state, &head, &budget)
                    .expect("codec parse")
                    .is_some()
            );
            let empty: Shared = Shared::new();
            let decoder = Decoder::try_new(&codec).expect("codec staging");
            let stored = decoder.storage(&empty, &state);
            let (_owners, finish) = decoder.try_finish(state, stored, &budget);
            assert!(matches!(
                finish,
                Err(common::codec::decoder::Error::Protocol(
                    protocol::Error::IncompleteBody | protocol::Error::IncompleteChunked
                ))
            ));
        }
        let state = codec::Stateful::state(&codec, budget);
        let partial = (Shared::try_copy(b"HTTP/1.1 200")).expect("partial head fixture");
        let decoder = Decoder::try_new(&codec).expect("codec staging");
        let stored = decoder.storage(&partial, &state);
        let (_owners, finish) = decoder.try_finish(state, stored, &budget);
        assert!(matches!(
            finish,
            Err(common::codec::decoder::Error::Protocol(
                protocol::Error::IncompleteHead
            ))
        ));

        assert_eq!(partial.as_slice(), b"HTTP/1.1 200");
    })
    .expect("resident test domain");
}

#[test]
fn finish_alloc_failure_releases() {
    resident::Domain::try_scope(output::DATA_LIMIT + 6, |budget| {
        let codec = Codec::default();
        let mut state = codec::Stateful::state(&codec, budget);
        let decoder = Decoder::try_new(&codec).expect("codec staging");
        let head = Shared::try_copy(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .expect("head fixture storage");
        let fragment = Shared::try_copy(b"3\r\nabc\r\n").expect("fragment fixture storage");
        assert!(
            decoder
                .try_parse(&mut state, &head, &budget)
                .expect("codec parse")
                .is_some()
        );
        let (_, consumed) = decoder
            .try_parse(&mut state, &fragment, &budget)
            .expect("codec parse")
            .expect("fragment must parse");
        assert_eq!(&fragment.as_slice()[consumed.get()..], b"\r\n");

        let end = Shared::try_copy(b"\r\n0\r\nX: y\r\n\r\n").expect("end fixture storage");
        let mut stored = decoder.storage(&end, &state);
        let failed =
            Tracker::fail_after(1, || decoder.try_stored(&mut state, &mut stored, &budget));
        assert!(matches!(
            failed,
            Err(common::codec::decoder::Error::Protocol(
                protocol::Error::Compact(Error::Exhausted)
            ))
        ));
        assert_eq!(stored.source().as_ref(), end.as_slice());
        assert_eq!(budget.resident(), 0);
        assert!(
            decoder
                .try_stored(&mut state, &mut stored, &budget)
                .expect("failed codec")
                .is_none()
        );
    })
    .expect("resident test domain");
}

#[test]
fn chunk_retries_budget() {
    resident::Domain::try_scope(output::DATA_LIMIT, |budget| {
        let codec = Codec::default();
        let mut state = codec::Stateful::state(&codec, budget);
        let head = (Shared::try_copy(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"))
            .expect("head fixture storage");
        let fragment = (Shared::try_copy(b"3\r\nabc\r\n")).expect("fragment fixture storage");
        assert!(
            Decoder::try_new(&codec)
                .expect("codec staging")
                .try_parse(&mut state, &head, &budget)
                .expect("codec parse")
                .is_some()
        );
        let decoder = Decoder::try_new(&codec).expect("codec staging");
        let mut stored = decoder.storage(&fragment, &state);
        let blocker =
            storage::Owned::try_allocate(&budget, budget.available()).expect("resident blocker");
        Retain {
            result: decoder.try_stored(&mut state, &mut stored, &budget),
            budget,
        }
        .try_verify()
        .ok()
        .expect("exact resident retention error");
        drop(blocker);
        let (_, consumed) = (decoder
            .try_stored(&mut state, &mut stored, &budget)
            .expect("codec parse"))
        .expect("fragment retry must parse");
        assert_eq!(&fragment.as_slice()[consumed.get()..], b"\r\n");
        let end = (Shared::try_copy(b"\r\n0\r\n\r\n")).expect("end fixture storage");
        let (head, _) = (Decoder::try_new(&codec)
            .expect("codec staging")
            .try_parse(&mut state, &end, &budget)
            .expect("codec parse"))
        .expect("chunk terminator must parse");
        let (data,) = match only(head) {
            Some(Ok(Event::Data(data))) => Some((data,)),
            _ => None,
        }
        .expect("chunk terminator must emit data");
        assert_eq!(data.as_slice(), b"abc");
    })
    .expect("resident test domain");
}

#[test]
fn split_trailers_retain() {
    resident::Domain::try_scope(128, |budget| {
        let codec = Codec::default();
        let decoder = Decoder::try_new(&codec).expect("codec staging");
        let head = Shared::try_copy(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .expect("head fixture storage");
        let wire =
            storage::Shared::try_from(&mut b"0\r\nX-Proof: yes\r\nX-Trace: 7\r\n\r\n".to_vec())
                .expect("trailer backing must retain its owner");
        for (first, second) in [(0, 0), (0, 9), (9, 15)] {
            let mut state = codec::Stateful::state(&codec, budget);
            assert!(
                decoder
                    .try_parse(&mut state, &head, &budget)
                    .expect("codec parse")
                    .is_some()
            );
            for range in [0..first, first..second, second..wire.len()] {
                if range.is_empty() {
                    continue;
                }
                let complete = range.end == wire.len();
                let input = wire
                    .try_get(range)
                    .expect("trailer input")
                    .expect("trailer input");
                let mut stored = decoder.storage(&input, &state);
                let blocker = storage::Owned::try_allocate(&budget, budget.available())
                    .expect("occupy trailer budget");
                assert!(
                    Retain {
                        result: decoder.try_stored(&mut state, &mut stored, &budget),
                        budget,
                    }
                    .try_verify()
                    .is_ok()
                );
                drop(blocker);
                assert_eq!(stored.source().as_ref(), input.as_slice());
                let parsed = decoder
                    .try_stored(&mut state, &mut stored, &budget)
                    .expect("retry trailer input");
                assert!(stored.source().as_ref().is_empty());
                if !complete {
                    assert!(parsed.is_none());
                    continue;
                }
                let (parsed, consumed) = parsed.expect("trailer produces an item");
                assert_eq!(consumed.get(), input.len());
                let trailers = match only(parsed) {
                    Some(Ok(Event::Trailers(trailers))) => trailers,
                    _ => panic!("complete trailer must emit fields"),
                };
                let value = trailers.get("x-proof").expect("x-proof trailer");
                assert_eq!(value, b"yes");
                assert_ne!(value.as_ptr(), wire.as_slice()[12..].as_ptr());
                let trace = trailers.get("x-trace").expect("x-trace trailer");
                assert_eq!(trace, b"7");
                assert_ne!(trace.as_ptr(), wire.as_slice()[26..].as_ptr());
                assert!(trailers.resident() <= 2 * trailers.as_bytes().len());
            }
        }
    })
    .expect("resident test domain");
}

#[test]
fn amplified_body_compacts() {
    resident::Domain::try_scope(128, |budget| {
        let codec = Codec::default();
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

        let owner = (storage::Shared::try_from(&mut vec![b'x'; 4096]))
            .expect("amplified body backing must retain its owner");
        let body = (owner.project(128..133)).expect("body range must fit its backing");
        let original = body.as_slice().as_ptr();
        let (parsed, allocs) = Tracker::count(|| {
            Decoder::try_new(&codec)
                .expect("codec staging")
                .try_parse(&mut state, &body, &budget)
                .expect("codec parse")
        });
        assert_eq!(allocs, 1);
        let (head, _) = (parsed).expect("body must parse");
        {
            let event = only(head);
            let (data,) = match event {
                Some(Ok(Event::Data(data))) => Some((data,)),
                _ => None,
            }
            .expect("body must emit data");
            assert_eq!(data.len(), 5);
            assert_eq!(data.resident(), 5);
            assert_eq!(data.as_slice(), b"xxxxx");
            assert_ne!(data.as_slice().as_ptr(), original);
        }
        assert_eq!(budget.resident(), 0);
    })
    .expect("resident test domain");
}
