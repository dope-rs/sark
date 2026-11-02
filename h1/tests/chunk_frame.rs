pub mod common;

use std::num::NonZero;

use common::body::chunks;
use dope::net::link::egress::{self, data::raw::Payload as _};
use dope_test::alloc::Tracker;
use o3::buffer::{
    raw::Scatter as _,
    resident::{self, bound},
    storage,
};
use sark_h1::server::{
    payload::{self, End, Frame},
    task::runtime::Stream,
};
use sark_http::payload::response::dispatch::chunk::Chunk;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn scatter_matches_wire() {
    for &(body, expected) in chunks::FRAMING {
        let owner = storage::Shared::try_copy(body);
        let owner = owner.expect("test chunk allocation succeeds");
        let body = Chunk::try_new(resident::Shared::from(owner));
        let body = body.expect("chunk fixture must be non-empty");
        let frame = Frame::chunk(body);
        assert_eq!(frame.len(), expected.len());
        assert_eq!(
            chunks::try_collect(&frame, NonZero::<usize>::MIN).expect("non-empty frame"),
            expected
        );
        assert_eq!(
            chunks::try_collect(&frame, NonZero::new(7).expect("non-zero cap"))
                .expect("non-empty frame"),
            expected
        );
    }
}

#[test]
fn offsets_match_suffix() {
    let owner = storage::Shared::try_copy(b"123456789abcdef0");
    let owner = owner.expect("test chunk allocation succeeds");
    let body = Chunk::try_new(resident::Shared::from(owner));
    let body = body.expect("chunk fixture must be non-empty");
    let frame = Frame::chunk(body);
    let expected = b"10\r\n123456789abcdef0\r\n";
    let cap = NonZero::<usize>::new(3);
    let cap = cap.expect("non-zero traversal cap");

    for offset in 0..expected.len() {
        let cursor =
            frame.cursor(chunks::try_position(offset).expect("offset fits transfer domain"));
        let cursor = cursor.expect("in-frame offset has a cursor");
        let mut chunks = cursor;
        let mut wire = Vec::with_capacity(expected.len() - offset);
        while let Some(chunk) = chunks.take_bytes(cap.get()).next() {
            wire.extend_from_slice(chunk.bytes());
        }
        assert_eq!(wire, expected[offset..]);
    }
    assert!(
        frame
            .cursor(chunks::try_position(expected.len()).expect("offset fits transfer domain"))
            .is_none()
    );
}

#[test]
fn chunk_retains_owner() {
    let body = storage::Shared::try_copy(&[b'x'; 4096]);
    let body = body.expect("4096-byte test body allocation succeeds");
    let pointer = body.as_slice().as_ptr();

    let body = Chunk::try_new(resident::Shared::from(body));
    let body = body.expect("chunk fixture must be non-empty");
    let (frame, allocs) = Tracker::count(|| Frame::chunk(body));
    assert_eq!(allocs, 0);

    let body = frame.into_chunk();
    let body = body.expect("data frame owns one body");
    assert_eq!(body.owner().as_slice().as_ptr(), pointer);
}

#[test]
fn terminator_is_inline() {
    let (frame, allocs) = Tracker::count(Frame::terminator);

    assert_eq!(allocs, 0);
    assert_eq!(
        chunks::try_collect(&frame, NonZero::new(2).expect("non-zero cap"))
            .expect("non-empty frame"),
        b"0\r\n\r\n"
    );
    assert!(frame.into_chunk().is_none());
}

#[test]
fn deferred_end_is_last() {
    let body = egress::NonEmpty::try_new(payload::Stream::new(Frame::terminator(), 7));
    let body = body.expect("terminator has non-empty transport geometry");
    let (body, end) = Stream::new(
        body.try_bound::<bound::Unbounded>()
            .ok()
            .expect("unbounded geometry"),
    )
    .into_parts();
    let (frame, tag) = body.into_inner().into_inner().into_parts();

    assert_eq!(tag, 7);
    assert!(matches!(end, End::Last));
    assert!(frame.into_chunk().is_none());
}

#[test]
fn deferred_body_retains() {
    let owner = storage::Shared::try_copy(b"same body through every retry");
    let owner = owner.expect("test operation must succeed");
    let pointer = owner.as_slice().as_ptr();
    let chunk = Chunk::try_new(resident::Shared::from(owner));
    let chunk = chunk.expect("test operation must succeed");
    let body = egress::NonEmpty::try_new(payload::Stream::new(Frame::chunk(chunk), 19));
    let body = body
        .expect("test operation must succeed")
        .try_bound::<bound::Unbounded>()
        .ok()
        .expect("unbounded geometry");
    let count = body.geometry().wire();
    let charge = body.charge();

    let (body, allocs) = Tracker::count(|| {
        let staged = Stream::new(body);
        let (body, end) = staged.into_parts();
        assert!(matches!(end, End::More));
        body
    });
    assert_eq!(allocs, 0);
    assert!((body.geometry().wire()) == (count));
    assert!((body.charge()) == (charge));

    let (frame, tag) = body.into_inner().into_inner().into_parts();
    assert_eq!(tag, 19);
    let chunk = frame.into_chunk();
    let chunk = chunk.expect("data frame retains its exact chunk owner");
    assert_eq!(chunk.owner().as_slice().as_ptr(), pointer);
}
