pub mod common;

use std::mem;

use common::field::{
    dynamics,
    sections::{self, CompactPlan, INVALID, PATH, PENDING, REPLACEMENT},
};
use o3::buffer::pool;
use sark_http::field::{
    Indexing, head,
    metadata::{
        block, lower,
        map::record::Span,
        section::{
            rules,
            state::{
                complete::Selection,
                decode::{Final, Indexed, Session, Start},
            },
            storage,
        },
        strings::{Cursor, Encoding},
    },
};

#[test]
fn retains_selected_range() {
    let mut scratch = storage::Scratch::default();
    let decoder = CompactPlan::try_complete(&mut scratch).expect("complete section");
    let complete = decoder.finish().expect("complete");
    assert!(matches!(&complete, Final::Request(_)));
    let (head, _, _, _) = complete.into_parts();
    let head = head.try_request().ok().expect("request");
    let (fields, pick, _) = head.into_parts();
    let range = pick.into_inner().expect("selected path");
    assert_eq!(fields.as_bytes().get(range), Some(PATH));
}

#[test]
fn duplicate_blocks_publish() {
    let mut scratch = storage::Scratch::default();
    let mut decoder = CompactPlan::try_complete(&mut scratch).expect("complete section");
    CompactPlan::try_push(&mut decoder, REPLACEMENT).expect("field");
    let complete = decoder.finish().expect("complete");
    assert!(matches!(&complete, Final::Rejected(_)));
    let (head, sealed, finished, response) = complete.into_parts();
    assert!(head.try_request().is_err());
    assert_eq!(sealed, block::Validation::Malformed);
    assert_eq!(finished, block::Validation::Malformed);
    assert_eq!(response, None);
}

#[test]
fn invalid_blocks_publish() {
    let mut scratch = storage::Scratch::default();
    let mut decoder = CompactPlan::try_complete(&mut scratch).expect("complete section");
    CompactPlan::try_push(&mut decoder, INVALID).expect("field");
    let (head, _, _, _) = decoder.finish().expect("complete").into_parts();
    assert!(head.try_request().is_err());
}

const _: () = assert!(!mem::needs_drop::<Selection<'static, 'static, CompactPlan>>());

#[test]
fn forgotten_blocks_publish() {
    for resume in [false, true] {
        let mut scratch = storage::Scratch::default();
        let mut decoder = CompactPlan::try_complete(&mut scratch).expect("complete section");
        let name = block::Named::from_span(Span::from_slice(PENDING.name));
        decoder.select(name);
        if resume {
            CompactPlan::try_push(&mut decoder, PENDING).expect("field");
        }
        let (head, _, _, _) = decoder.finish().expect("complete").into_parts();
        assert!(head.try_request().is_err());
    }
}

#[test]
fn blocks_pending_selection() {
    let lease = pool::Span::try_new(128).expect("lease");
    let mut session = Session::<rules::RetainRaw>::try_new(
        lease,
        rules::Kind::Request,
        block::Connect::Disabled,
        storage::Scratch::default(),
    )
    .map_err(|(_, error)| error)
    .expect("session");
    let name = block::Named::from_slice(dynamics::NAME);
    let start = session
        .begin(Some(name), Indexing::Allowed)
        .expect("select");
    assert!(matches!(start, Start::Buffered(())));
    let error = session
        .begin(Some(name), Indexing::Allowed)
        .err()
        .expect("pending selection");
    assert!(matches!(error, lower::Error::State(lower::State::Pending)));
    let error = session.indexed(Some(name)).err().expect("pending index");
    assert!(matches!(error, lower::Error::State(lower::State::Pending)));
    assert!(matches!(
        session.finish().1,
        Err(lower::Error::State(lower::State::Pending))
    ));
}

#[test]
fn consumes_selection_once() {
    let lease = pool::Span::try_new(128).expect("lease");
    let mut session = Session::<rules::RetainRaw>::try_new(
        lease,
        rules::Kind::Request,
        block::Connect::Disabled,
        storage::Scratch::default(),
    )
    .map_err(|(_, error)| error)
    .expect("session");
    let name = block::Named::from_slice(dynamics::NAME);
    let value = block::Valued::from_span(Span::from_slice(dynamics::VALID));
    let indexed = session.indexed(Some(name)).expect("select");
    assert!(matches!(indexed, Indexed::Buffered(())));
    let error = session
        .direct::<rules::RejectPush>()
        .expect_err("buffered selection");
    assert!(matches!(error, lower::Error::State(lower::State::Pending)));
    session
        .field::<rules::RejectPush>(name, value, Indexing::Allowed)
        .expect("selected field");
    let error = session
        .field::<rules::RejectPush>(name, value, Indexing::Allowed)
        .expect_err("consumed selection");
    assert!(matches!(error, lower::Error::State(lower::State::Ready)));
    assert!(session.finish().1.is_ok());
}

#[test]
fn mismatch_keeps_selection() {
    let lease = pool::Span::try_new(256).expect("lease");
    let mut session = Session::<rules::RetainRaw>::try_new(
        lease,
        rules::Kind::Request,
        block::Connect::Disabled,
        storage::Scratch::default(),
    )
    .map_err(|(_, error)| error)
    .expect("session");
    for field in sections::REQUEST {
        let name = block::Named::from_slice(field.name);
        let value = block::Valued::from_span(Span::from_slice(field.value));
        session.indexed(Some(name)).expect("select");
        let error = session
            .field::<rules::RejectPush>(
                block::Named::from_slice(dynamics::NAME),
                value,
                field.indexing,
            )
            .expect_err("mismatched name");
        assert!(matches!(error, lower::Error::Name { expected, actual }
            if expected == name.proof().head() && actual == head::Name::Regular));
        session
            .field::<rules::RejectPush>(name, value, field.indexing)
            .expect("original selection");
    }
    let (head, _, _, _) = session.finish().1.expect("finish").into_parts();
    let head = head.try_request().ok().expect("request");
    assert!(head.fields().iter().eq(sections::REQUEST));
}

#[test]
fn rejects_buffered_value() {
    let lease = pool::Span::try_new(256).expect("lease");
    let mut session = Session::<rules::RetainRaw>::try_new(
        lease,
        rules::Kind::Request,
        block::Connect::Disabled,
        storage::Scratch::default(),
    )
    .map_err(|(_, error)| error)
    .expect("session");
    for field in sections::REQUEST {
        let name = block::Named::from_slice(field.name);
        session.indexed(Some(name)).expect("select request field");
        session
            .field::<rules::RejectPush>(
                name,
                block::Valued::from_span(Span::from_slice(field.value)),
                field.indexing,
            )
            .expect("request field");
    }
    let name = block::Named::from_slice(dynamics::NAME);
    session.indexed(Some(name)).expect("select regular field");
    session
        .field::<rules::RejectPush>(
            name,
            block::Valued::from_span(Span::from_slice(dynamics::INVALID)),
            Indexing::Allowed,
        )
        .expect("record invalid value");
    let (head, _, _, _) = session.finish().1.expect("finish").into_parts();
    assert!(head.try_request().is_err());
}

#[test]
fn rejects_direct_value() {
    let lease = pool::Span::try_new(256).expect("lease");
    let mut session = Session::<CompactPlan>::try_new(
        lease,
        rules::Kind::Request,
        block::Connect::Disabled,
        storage::Scratch::default(),
    )
    .map_err(|(_, error)| error)
    .expect("session");
    for field in sections::REQUEST {
        let name = block::Named::from_slice(field.name);
        if name.proof().head() == head::Name::Path {
            let start = session
                .begin(Some(name), field.indexing)
                .expect("select path");
            assert!(matches!(start, Start::Direct(())));
            let invalid = dynamics::INVALID;
            let mut cursor = Cursor::new(invalid.len(), Encoding::Plain);
            for bytes in invalid.chunks(1) {
                cursor = match session.string(cursor, bytes) {
                    lower::Step::Ready { cursor } => Some(cursor),
                    _ => None,
                }
                .expect("direct string");
            }
            session
                .direct::<rules::RejectPush>()
                .expect("record direct verdict");
        } else {
            let indexed = session.indexed(Some(name)).expect("select");
            if let Indexed::Buffered(()) = indexed {
                session
                    .field::<rules::RejectPush>(
                        name,
                        block::Valued::from_span(Span::from_slice(field.value)),
                        field.indexing,
                    )
                    .expect("request field");
            }
        }
    }
    let (head, _, _, _) = session.finish().1.expect("finish").into_parts();
    assert!(head.try_request().is_err());
}
