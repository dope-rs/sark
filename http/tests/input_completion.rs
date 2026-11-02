pub mod common;

use common::field::{
    huffman,
    sections::{self, CompactPlan},
};
use o3::buffer::pool;
use sark_http::field::{
    Indexing,
    metadata::{
        block,
        huffman::decoder::{self, DecodeError},
        lower,
        map::record::Span,
        packed,
        packed::prefix::Prefix,
        section::{
            rules,
            state::{
                Decoder,
                decode::{Completion as _, Omit, Sink, Start},
                fixed,
            },
            storage,
        },
        strings::{Cursor, Encoding},
    },
};

#[test]
fn keeps_scratch_prefix() {
    let mut scratch = storage::Scratch::default();
    let lease = pool::Span::try_new(32).expect("allocate");
    let writer = storage::Writer::<storage::Fields>::try_new(lease).expect("prefix");
    let mut input = lower::Input::new(writer);
    input.try_scratch(&mut scratch).expect("scratch");
    assert!(matches!(
        input.string(
            &mut scratch,
            Cursor::new(huffman::LONG.len(), Encoding::Plain),
            huffman::LONG,
        ),
        lower::Step::Ready { .. }
    ));
    input.rollback(&mut scratch);
    let pointer = scratch.authority().as_ptr();
    let mut state = <Decoder<CompactPlan> as Sink<CompactPlan>>::h2(
        rules::Kind::Request,
        block::Connect::Disabled,
    );
    let done = match state
        .begin(
            &mut input,
            block::Named::from_slice(sections::REQUEST[3].name),
            Indexing::Allowed,
        )
        .expect("begin")
    {
        Start::Scratch(done) => Some(done),
        _ => None,
    }
    .expect("scratch selection");
    input.try_scratch(&mut scratch).expect("ready");
    assert!(matches!(
        input.string(
            &mut scratch,
            Cursor::new(huffman::PREFIX.len(), Encoding::Plain),
            huffman::PREFIX,
        ),
        lower::Step::Ready { .. }
    ));
    let progress = match input.string(
        &mut scratch,
        Cursor::new(huffman::INVALID.len(), Encoding::Huffman),
        huffman::INVALID,
    ) {
        lower::Step::Decode {
            cause: DecodeError::InvalidEncoding(decoder::Error),
            progress,
        } => Some(progress),
        _ => None,
    }
    .expect("invalid encoding");
    assert_eq!((progress.consumed(), progress.decoded()), (5, 3));
    done.try_finish::<rules::RejectPush>(&mut state, &mut input, &mut scratch)
        .expect("retain decoded prefix");
    assert_eq!(scratch.authority().as_ptr(), pointer);
    assert_eq!(
        scratch.authority().split_at(huffman::PREFIX.len()),
        (huffman::PREFIX, huffman::SHORT)
    );
    assert!(input.try_seal().is_ok());
}

#[test]
fn rejects_pending_scratch() {
    let lease = pool::Span::try_new(Prefix::LEN).expect("allocate");
    let writer = storage::Writer::<packed::Decoded>::try_new(lease).expect("prefix");
    let mut input = lower::Input::new(writer);
    input
        .try_scratch(&mut storage::Scratch::default())
        .expect("ready");
    assert!(matches!(input.try_seal(), Err(lower::State::Scratch)));
}

#[test]
fn finishes_ready_input() {
    let lease = pool::Span::try_new(Prefix::LEN).expect("allocate");
    let writer = storage::Writer::<packed::Decoded>::try_new(lease).expect("prefix");
    let writer = lower::Input::new(writer).try_seal().expect("ready");
    let decoded = <packed::Decoded as storage::Block>::from(writer.finish());
    assert!(decoded.is_empty());
}

#[test]
fn rejects_invalid_start() {
    let lease = pool::Span::try_new(Prefix::LEN).expect("allocate");
    let writer = storage::Writer::<packed::Decoded>::try_new(lease).expect("prefix");
    let mut input = lower::Input::new(writer);
    let mut scratch = storage::Scratch::default();
    assert!(matches!(
        input.string(&mut scratch, Cursor::new(1, Encoding::Plain), b"x"),
        lower::Step::Store {
            cause: lower::Error::State(lower::State::Ready)
        }
    ));
    input.try_scratch(&mut scratch).expect("ready");
    assert_eq!(input.try_ready(), Err(lower::State::Scratch));
    assert_eq!(input.try_scratch(&mut scratch), Err(lower::State::Scratch));
    input.rollback(&mut scratch);
    input.rollback(&mut scratch);
    assert!(input.try_seal().is_ok());
}

#[test]
fn rejects_early_finish() {
    let lease = pool::Span::try_new(32).expect("allocate");
    let writer = storage::Writer::<storage::Fields>::try_new(lease).expect("prefix");
    let mut input = lower::Input::new(writer);
    let mut scratch = storage::Scratch::default();
    let mut state = <fixed::Discard as Sink<rules::Discard>>::h2(
        rules::Kind::Request,
        block::Connect::Disabled,
    );
    let name = Span::from_slice(b":authority");
    let start = <fixed::Discard as Sink<rules::Discard>>::begin(
        &mut state,
        &mut input,
        block::Named::from_span(name),
        Indexing::Allowed,
    )
    .expect("begin");
    assert!(matches!(start, Start::Direct(_)));
    assert!(matches!(
        input.string(&mut scratch, Cursor::new(2, Encoding::Plain), b"a"),
        lower::Step::Ready { .. }
    ));
    let cause = Omit
        .try_finish::<rules::RejectPush>(&mut state, &mut input, &mut scratch)
        .expect_err("early completion");
    assert!(matches!(cause, lower::Error::State(lower::State::Tagged)));
    let cause = Omit
        .try_finish::<rules::RejectPush>(&mut state, &mut input, &mut scratch)
        .expect_err("repeated completion");
    assert!(matches!(cause, lower::Error::State(lower::State::Ready)));
    let lease = input.try_seal().expect("rolled back").finish().into_lease();
    assert!(lease.is_empty());
}

#[test]
fn rejects_stored_mutation() {
    let lease = pool::Span::try_new(32).expect("allocate");
    let writer = storage::Writer::<storage::Fields>::try_new(lease).expect("prefix");
    let mut input = lower::Input::new(writer);
    let mut scratch = storage::Scratch::default();
    let mut state = <fixed::Discard as Sink<rules::Discard>>::h2(
        rules::Kind::Request,
        block::Connect::Disabled,
    );
    let name = Span::from_slice(b":authority");
    <fixed::Discard as Sink<rules::Discard>>::begin(
        &mut state,
        &mut input,
        block::Named::from_span(name),
        Indexing::Allowed,
    )
    .expect("begin");
    assert!(matches!(
        input.string(&mut scratch, Cursor::new(1, Encoding::Plain), b"a"),
        lower::Step::Ready { .. }
    ));
    assert!(matches!(
        input.string(&mut scratch, Cursor::new(1, Encoding::Plain), b"b"),
        lower::Step::Store {
            cause: lower::Error::State(lower::State::Stored)
        }
    ));
    assert!(matches!(
        <fixed::Discard as Sink<rules::Discard>>::field::<rules::RejectPush>(
            &mut state,
            &mut input,
            &mut scratch,
            fixed::Omit::Authority,
            name,
            Span::from_slice(b"b"),
            Indexing::Allowed,
        ),
        Err(lower::Error::State(lower::State::Stored))
    ));
    Omit.try_finish::<rules::RejectPush>(&mut state, &mut input, &mut scratch)
        .expect("stored completion");
    assert!(input.try_seal().is_ok());
}

#[test]
fn uses_stored_length() {
    let lease = pool::Span::try_new(32).expect("allocate");
    let writer = storage::Writer::<storage::Fields>::try_new(lease).expect("prefix");
    let mut input = lower::Input::new(writer);
    let mut scratch = storage::Scratch::default();
    let mut state = <fixed::Discard as Sink<rules::Discard>>::h2(
        rules::Kind::Request,
        block::Connect::Disabled,
    );
    let name = Span::from_slice(b":authority");
    <fixed::Discard as Sink<rules::Discard>>::begin(
        &mut state,
        &mut input,
        block::Named::from_span(name),
        Indexing::Allowed,
    )
    .expect("begin");
    assert!(matches!(
        input.string(&mut scratch, Cursor::new(2, Encoding::Plain), b"a"),
        lower::Step::Ready { .. }
    ));
    assert!(matches!(
        input.string(&mut scratch, Cursor::new(1, Encoding::Plain), b"b"),
        lower::Step::Ready { .. }
    ));
    Omit.try_finish::<rules::RejectPush>(&mut state, &mut input, &mut scratch)
        .expect("complete");
    let lease = input.try_seal().expect("ready").finish().into_lease();
    let prefix = storage::Tag::AUTHORITY.prefix(2).expect("length");
    assert_eq!(lease.as_slice(), [prefix.as_slice(), b"ab"].concat());
}
