pub mod common;

use std::mem;

use common::{Request, body::Retain};
use dope_test::alloc::Tracker;
use o3::buffer::{Complete as _, storage};
use sark_h1::{
    Input, session,
    wire::parsing::{self, framing, raw},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retention_follows_window() {
    for (offset, capacity, buffered, expected) in Retain::CASES {
        let mut input = vec![0; capacity];
        let end = offset + Request::HEAD.len();
        input[offset..end].copy_from_slice(Request::HEAD);
        let owner = buffered.then(|| storage::Shared::try_from(&mut input).expect("head owner"));
        let head = match &owner {
            Some(owner) => Input::Buffered(owner.view().try_get(offset..end).expect("head window")),
            None => Input::Borrowed(&input[offset..end]),
        };
        let pointer = head.as_slice().as_ptr();
        let parsed = common::head::parsed(head, u8::MAX);
        let mut fixture = Retain::try_new().expect("response resources");
        let mut conn = session::Conn::default();
        let (pending, allocs) = Tracker::count(|| {
            let head = framing::Head::<Request>::parse::<false>(raw::Bound::empty(&parsed))
                .ok()
                .flatten()
                .expect("complete framing");
            fixture.pending(&mut conn, head)
        });
        assert_eq!(allocs, expected);
        assert!(fixture.state.get().0.is_null());
        let pending = pending.ok().expect("pending body");
        assert_eq!(pending.head_len() as usize, Request::HEAD.len());
        conn.retire();
        drop(owner);
        input.fill(0);

        let retained = fixture.finish(pending);
        assert_eq!(retained == pointer, expected == 0);
    }
}

#[test]
fn retains_fragmented_head() {
    let split = Request::HEAD.len() - 2;
    let mut input = Request::HEAD[..split].to_vec();
    let source = common::head::parsed(Input::Borrowed(&input), u8::MAX);
    let (progress, usage) = Tracker::activity(|| {
        framing::Head::<Request>::start::<false>(raw::Bound::empty(&source))
            .ok()
            .expect("fields")
    });
    assert!(usage.allocated_bytes <= split + 64);
    let pending = progress.break_value().expect("pending head");
    input.fill(0);
    let mut fixture = Retain::try_new().expect("response resources");
    let mut conn = session::Conn::default();
    let outcome = pending
        .try_resume(Input::Borrowed(&Request::HEAD[split..]), |head| {
            fixture.pending(&mut conn, head)
        })
        .ok()
        .expect("head resumed");
    let pending = outcome
        .continue_value()
        .expect("completed head")
        .ok()
        .expect("pending body");
    conn.retire();
    fixture.finish(pending);
}

const _: () = {
    assert!(
        mem::size_of::<raw::Bound<&'static parsing::Head<'static>, Request>>()
            == mem::size_of::<usize>()
    );
};
