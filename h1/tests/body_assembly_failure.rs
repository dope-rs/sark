pub mod common;

use common::{
    Request,
    body::{Credits, Retain},
};
use dope_test::alloc::Tracker;
use o3::{
    alloc,
    buffer::{segments, segments::assembly, storage},
};
use sark_core::body::credit;
use sark_h1::{
    Input, session,
    wire::{
        Failure,
        parsing::{framing, raw},
    },
};
use sark_http::{h1::codec::canned, payload::owned};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn keeps_cause_and_credit() {
    let source = common::head::parsed(Input::Borrowed(Request::HEAD), u8::MAX);
    let head = framing::Head::<Request>::parse::<false>(raw::Bound::empty(&source))
        .ok()
        .flatten()
        .expect("complete framing");
    let mut fixture = Retain::try_new().expect("response resources");
    let mut conn: session::Conn = session::Conn::default();
    let outcome = fixture.dispatch(&mut conn, session::Permit::new(), head);
    let mut pending = match outcome {
        session::RouteOutcome::AwaitBody { pending, .. } => Some(pending),
        _ => None,
    }
    .expect("pending body");
    conn.retire();

    let mut evidence = Credits(Vec::with_capacity(3));
    let accepted = storage::Shared::try_copy(b"z").expect("first segment");
    pending
        .push_body(owned::Segments::from(accepted), &mut evidence)
        .expect("first owner needs no assembly");
    let rejected = storage::Shared::try_copy(b"x").expect("second segment");
    let error = Tracker::fail_after(0, || {
        pending.push_body(owned::Segments::from(rejected), &mut evidence)
    })
    .expect_err("metadata allocation fails");
    assert_eq!(error, assembly::Error::Metadata(alloc::Error::Exhausted));
    let failure = Failure::Assembly(error);
    assert_eq!(
        failure.body().as_slice(),
        canned::SERVICE_UNAVAILABLE.as_slice()
    );

    pending.cancel(&mut evidence);
    let retained = credit::Amount::new(1, segments::Count::ONE);
    assert!(
        evidence.0
            == [
                credit::Mutation::Reserved(retained),
                credit::Mutation::Released {
                    credit: retained,
                    disposition: credit::Disposition::Discarded,
                },
            ]
    );
}
