use crate::common::grpc::call::calls::Settings;
pub mod common;

use o3::depot::arena::slab;
use sark_grpc::{
    Code,
    client::{
        call::{bidi, request, unary},
        config::{Error, Field, Input, Limits, Output},
        session,
    },
    details::{Claim, Diagnostic},
    metadata::Metadata,
    protocol::section,
};
use sark_h2::client;

use crate::common::grpc::{call::Open as _, wire::Peer};

#[test]
fn invalid() {
    let result = Limits::default().try_calls(0);
    std::assert_matches!(
        result.err().expect("zero call capacity"),
        Error::ZeroCapacity(Field::Calls)
    );

    let defaults = Input::default();
    let result = Input::try_new(
        defaults.completed(),
        defaults.events(),
        defaults.messages(),
        0,
        defaults.bytes(),
    );
    std::assert_matches!(
        result.err().expect("zero segment capacity"),
        Error::ZeroCapacity(Field::InputSegments)
    );

    for (field, done, events, messages) in [
        (Field::CompletedResponses, 0, 1, 1),
        (Field::ResponseEvents, 1, 0, 1),
        (Field::InputMessages, 1, 1, 0),
    ] {
        std::assert_matches!(
            Input::try_new(done, events, messages, 1, 1)
                .err()
                .expect("zero input capacity"),
            Error::ZeroCapacity(actual) if actual == field
        );
    }
}

#[test]
fn input_segment_capacity() {
    let maximum = u32::MAX as usize;
    let admitted = Input::try_new(1, 1, 1, maximum, 1).expect("maximum segment count");
    assert_eq!(admitted.segments(), maximum);
    let demand = maximum + 1;
    let error = Input::try_new(1, 1, 1, demand, 1)
        .err()
        .expect("segment count exceeds its index range");
    std::assert_matches!(error, Error::MaximumCapacity {
        field: Field::InputSegments,
        maximum: actual,
        error,
    } if actual == maximum && error.requested() == demand);
}

#[test]
fn completion_capacity() {
    let max = u32::MAX as usize;
    let input = Input::try_new(1, 1, 1, 1, 0).expect("minimal input");
    let limits = Limits::try_new(
        max / 2,
        0,
        input,
        Output::try_new(1).expect("minimal output"),
    )
    .expect("maximum completion domain");
    let input = Input::try_new(2, 1, 1, 1, 0).expect("one extra completion");
    let error = limits
        .try_input(input)
        .err()
        .expect("completion domain exceeds its index range");
    let demand = max + 1;
    assert!(matches!(error, Error::MaximumCapacity {
        field: Field::CompletionSlots,
        maximum,
        error,
    } if maximum == max && error.requested() == demand));
}

#[test]
fn layout() {
    let limits = Settings::new(Some(2), Some(3), Some(Input::default().events()), None)
        .try_build()
        .expect("capacity fixture limits");
    session::Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let unary = request::Session::open(&mut client, b"/svc/Upload", None, &Metadata::new())
                .expect("open layout-test request stream");
            let bidi = bidi::Session::open(&mut client, b"/svc/Chat", None, &Metadata::new())
                .expect("open layout-test bidi stream");
            request::Session::cancel(&mut client, unary);
            bidi::Session::cancel(&mut client, bidi);
        },
    )
    .expect("canonical client capacity session");

    std::assert_matches!(
        Limits::default()
            .try_calls(u32::MAX as usize + 1)
            .err()
            .expect("call capacity exceeds its index range"),
        Error::MaximumCapacity {
            field: Field::Calls,
            maximum,
            error,
        } if maximum == u32::MAX as usize && error.requested() == u32::MAX as usize + 1
    );
}

#[test]
fn reuse() {
    let limits = Settings::new(Some(1), Some(1), Some(1), None)
        .try_build()
        .expect("capacity fixture limits");
    session::Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let streams = client.try_open(1).expect("capacity streams");
            let status = bidi::Session::open(&mut client, b"/svc/Method", None, &Metadata::new())
                .err()
                .expect("in-flight call capacity exhausted");
            assert_eq!(status.code(), Code::ResourceExhausted);
            assert!(matches!(
                Diagnostic::from(&status),
                Diagnostic::Claim(Claim::StreamCapacity(slab::Vacancy::Full))
            ));

            assert_eq!(
                Peer::try_reset(&mut client).expect("ingest in-flight stream reset"),
                streams.len()
            );
            let status = bidi::Session::try_poll(&mut client, &streams[0])
                .err()
                .expect("reset bidi call");
            assert_eq!(status.code(), Code::Cancelled);
            (bidi::Session::open(&mut client, b"/svc/Method", None, &Metadata::new()))
                .as_ref()
                .expect("test operation must succeed");
        },
    )
    .expect("in-flight capacity reuse session");
}

#[test]
fn replay() {
    let limits = Settings::new(Some(2), Some(1), Some(2), None)
        .try_build()
        .expect("capacity fixture limits");
    session::Session::<_>::with_limits(
        client::Plan::<section::Plan>::new(limits.calls()).expect("valid H2 client plan"),
        limits,
        |mut client| {
            let streams = client.try_unary(2).expect("capacity unary calls");
            assert_eq!(
                Peer::try_reset(&mut client).expect("ingest completion-backpressured resets"),
                streams.len()
            );
            for stream in &streams {
                let status = unary::Session::poll(&mut client, stream)
                    .err()
                    .expect("reset unary call");
                assert_eq!(status.code(), Code::Cancelled);
            }
            assert_eq!(client.try_unary(2).expect("capacity unary calls").len(), 2);
        },
    )
    .expect("completion capacity replay session");
}
