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
    payload::{End, Frame, Stream},
    task::runtime::{Runtime, Stream as Staged},
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
        let frame = Stream::new(Frame::chunk(body), ());
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
    let frame = Stream::new(Frame::chunk(body), ());
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
fn deferred_frames_keep_their_owner_and_order() {
    let owner = storage::Shared::try_copy(b"same body through every retry").expect("chunk owner");
    let pointer = owner.as_slice().as_ptr();
    let chunk = Chunk::try_new(resident::Shared::from(owner)).expect("non-empty chunk");
    let ((body, end), allocations) = Tracker::count(|| {
        let [body, end] = [
            Stream::new(Frame::chunk(chunk), 19),
            Stream::new(Frame::terminator(), 7),
        ]
        .map(|frame| {
            egress::NonEmpty::try_new(frame)
                .expect("non-empty frame")
                .try_bound::<bound::Unbounded>()
                .ok()
                .expect("unbounded frame")
        });
        let charge = body.charge();
        let wire = body.geometry().wire();
        let bytes = body.as_inner().charge().resident_bytes;
        let mut runtime = Runtime::<(), (), _>::default();
        assert!(runtime.try_stage(Staged::new(body)).is_ok());
        let end = runtime.try_stage(Staged::new(end)).expect_err("full slot");
        assert_eq!(runtime.staged_bytes(), bytes);
        let (body, kind) = runtime.take_stream().expect("staged data").into_parts();
        assert!(matches!(kind, End::More));
        assert!(body.charge() == charge);
        assert_eq!(body.geometry().wire(), wire);
        assert!(runtime.try_stage(end).is_ok());
        let (end, kind) = runtime
            .take_stream()
            .expect("staged terminator")
            .into_parts();
        assert!(matches!(kind, End::Last));
        (
            body.into_inner().into_inner(),
            end.into_inner().into_inner(),
        )
    });
    assert_eq!(allocations, 0);
    let mut cursor = body.start();
    let bytes = cursor.take_bytes(usize::MAX).nth(1).expect("chunk payload");
    assert_eq!(bytes.bytes().as_ptr(), pointer);
    assert_eq!(body.release(), 19);
    assert_eq!(
        chunks::try_collect(&end, NonZero::new(2).expect("non-zero cap")).expect("terminator"),
        b"0\r\n\r\n"
    );
    assert_eq!(end.release(), 7);
}
