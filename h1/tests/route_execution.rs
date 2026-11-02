pub mod common;

use std::{
    cell::Cell,
    convert::{self, Infallible},
    num::NonZero,
    str,
};

use common::{
    body::Credits,
    discard, fields,
    tasks::{self, BUDGET, Buffered, Connect, FINISHES, HEAD, Request, STEPS, Session, Streaming},
};
use dope::core::{
    BranchError,
    driver::{self, life::recovery, schedule::ready::task::owner, settings},
};
use dope_test::scope::{self, Driver};
use o3::buffer::{Complete as _, segments, storage};
use sark_core::{
    body::{self, credit},
    duplex::machine,
    manifold::task::request,
    route::{Own as _, TaskBuild as _},
};
use sark_h1::{
    Input,
    route::{duplex, stream, task},
    session,
    switch::Enabled,
    wire::{
        Failure, head, output,
        parsing::{self, framing, raw},
    },
};
use sark_http::{
    h1::{head::Semantic, switch},
    method::Known,
    payload::owned,
    request::Seq,
};

#[test]
fn method_body_allowance() {
    for &(input, expected) in discard::BODY_POLICY.iter().chain(tasks::ALLOWED_BODIES) {
        for methods in [Known::ALL, 0] {
            let parsed = parsing::Head::parse(Input::Borrowed(input), methods)
                .expect("head")
                .expect("complete head");
            let head = framing::Head::<Streaming>::parse(raw::Bound::empty(&parsed))
                .ok()
                .expect("framing")
                .expect("complete framing");
            let mut conn: session::Conn = session::Conn::default();
            let state = Cell::new(0);
            let mut write = [0; 512];
            let mut evidence = Credits(Vec::new());
            let outcome = stream::Endpoint::new(
                output::Egress::new(&mut write, fields::DATE, parsed.response()),
                &mut conn,
            )
            .dispatch::<Streaming, _, _, _, _>(
                session::Permit::new(),
                head,
                &state,
                || Some(|(), _: &mut (), _| {}),
                &mut evidence,
            );
            let status = match outcome {
                session::RouteOutcome::Outcome(
                    session::Outcome::Streamed { .. } | session::Outcome::Complete { .. },
                ) => 200,
                session::RouteOutcome::Outcome(session::Outcome::Failed(Failure::BadRequest)) => {
                    400
                }
                session::RouteOutcome::Outcome(session::Outcome::Failed(
                    Failure::PayloadTooLarge,
                )) => 413,
                _ => panic!("body admission must finish or reject before continuation"),
            };
            assert_eq!(status, expected);
            if expected != 200 {
                assert!(evidence.0.is_empty());
            }
        }
    }
}

#[test]
fn transition_admission() {
    for &(input, connect) in tasks::PARTIAL_UPGRADE {
        for methods in [Known::ALL, 0] {
            let parsed = parsing::Head::parse(Input::Borrowed(input), methods)
                .expect("head")
                .expect("complete head");
            let result = framing::Head::<Streaming>::parse(raw::Bound::empty(&parsed));
            assert!(if connect {
                matches!(result, Ok(Some(_)))
            } else {
                matches!(
                    result,
                    Err(parsing::Request::Head(head::Error::Method(
                        Semantic::Transition(switch::Error::IncompleteUpgradeRequest)
                    )))
                )
            });
        }
    }
}

#[test]
fn discarded_stream_length() {
    for input in [tasks::HEAD, tasks::COMPLETE] {
        let parsed = parsing::Head::parse(Input::Borrowed(input), u8::MAX)
            .expect("head")
            .expect("complete head");
        let head = framing::Head::<Streaming>::parse(raw::Bound::empty(&parsed))
            .ok()
            .expect("framing")
            .expect("complete framing");
        let mut conn: session::Conn = session::Conn::default();
        let state = Cell::new(0);
        let mut write = [0; 512];
        let outcome = stream::Endpoint::new(
            output::Egress::new(&mut write, fields::DATE, parsed.response()),
            &mut conn,
        )
        .dispatch::<Streaming, _, _, _, _>(
            session::Permit::new(),
            head,
            &state,
            || Some(|(), _: &mut (), _| {}),
            credit::Disabled::new(),
        );
        let outcome = match outcome {
            session::RouteOutcome::Outcome(outcome) => outcome,
            session::RouteOutcome::AwaitBody { pending, .. } => pending.finish(
                session::Complete {
                    body_len: 1,
                    consumed: 1,
                },
                session::Permit::new(),
                stream::Execution::new(
                    output::Egress::new(&mut write, fields::DATE, parsed.response()),
                    &state,
                    &mut (),
                    || Some(|(), _: &mut (), _| {}),
                ),
                credit::Disabled::new(),
            ),
            session::RouteOutcome::AwaitHead { pending, .. } => match pending {},
            session::RouteOutcome::AwaitReject { pending, .. } => match pending {},
        };
        let written = match outcome {
            session::Outcome::Streamed { written, .. } => Some(written),
            _ => None,
        }
        .expect("stream did not start");
        let head = str::from_utf8(&write[..written]).expect("response head");
        assert!(!head.contains("Date:"));
        assert!(!head.contains("Server:"));
        assert_eq!(state.get(), 1);
    }
}

#[test]
fn stream_capacity() {
    for input in [tasks::HEAD, tasks::COMPLETE] {
        let parsed = parsing::Head::parse(Input::Borrowed(input), u8::MAX)
            .expect("head")
            .expect("complete head");
        let head = framing::Head::<Streaming>::parse(raw::Bound::empty(&parsed))
            .ok()
            .flatten()
            .expect("framing");
        let mut conn: session::Conn = session::Conn::default();
        let state = Cell::new(0);
        let mut write = [0xa5; 512];
        let outcome = stream::Endpoint::new(
            output::Egress::new(&mut write, fields::DATE, parsed.response()),
            &mut conn,
        )
        .dispatch::<Streaming, _, _, _, _>(
            session::Permit::new(),
            head,
            &state,
            || tasks::stream(false),
            credit::Disabled,
        );
        let outcome = match outcome {
            session::RouteOutcome::Outcome(outcome) => outcome,
            session::RouteOutcome::AwaitBody { pending, .. } => pending.finish(
                session::Complete {
                    body_len: 1,
                    consumed: 1,
                },
                session::Permit::new(),
                stream::Execution::new(
                    output::Egress::new(&mut write, fields::DATE, parsed.response()),
                    &state,
                    &mut (),
                    || tasks::stream(false),
                ),
                credit::Disabled,
            ),
            session::RouteOutcome::AwaitHead { pending, .. } => match pending {},
            session::RouteOutcome::AwaitReject { pending, .. } => match pending {},
        };
        assert!(matches!(outcome, session::Outcome::Close(_)));
        assert_eq!(state.get(), 0);
        assert!(write.iter().all(|&byte| byte == 0xa5));
    }
}

#[test]
fn closing_connect() {
    for input in [&tasks::CONNECT[..tasks::CONNECT.len() - 1], tasks::CONNECT] {
        let parsed = parsing::Head::parse(Input::Borrowed(input), u8::MAX)
            .expect("head")
            .expect("complete head");
        let head = framing::Head::<Connect>::parse(raw::Bound::empty(&parsed))
            .ok()
            .expect("framing")
            .expect("complete framing");
        let mut conn: session::Conn<'_, (), (), Enabled<machine::Machine<'_, Session>>> =
            session::Conn::default();
        let outcome = duplex::Endpoint::new(&(), &mut conn).dispatch::<Connect, _, _>(
            session::Permit::new(),
            head,
            convert::identity,
            credit::Disabled::new(),
        );
        assert!(matches!(
            outcome,
            session::RouteOutcome::Outcome(session::Outcome::Failed(Failure::BadRequest))
        ));
    }
}

#[test]
fn connect_keeps_version() {
    for &(input, expected) in tasks::CONNECT_VERSIONED {
        let parsed = parsing::Head::parse(Input::Borrowed(input), u8::MAX)
            .expect("head")
            .expect("complete head");
        let head = framing::Head::<Connect>::parse(raw::Bound::empty(&parsed))
            .ok()
            .flatten()
            .expect("complete framing");
        let mut conn: session::Conn<'_, (), (), Enabled<machine::Machine<'_, Session>>> =
            session::Conn::default();
        let outcome = duplex::Endpoint::new(&(), &mut conn).dispatch::<Connect, _, _>(
            session::Permit::new(),
            head,
            convert::identity,
            credit::Disabled,
        );
        let reply = match outcome {
            session::RouteOutcome::Outcome(session::Outcome::Switch {
                response: output::Switch::Static(reply),
                ..
            }) => Some(reply),
            _ => None,
        }
        .expect("CONNECT switches protocols");
        assert_eq!(reply.as_slice(), expected);
    }
}

#[test]
fn body_yields() {
    let mut events = Vec::new();
    Driver::new(settings::Config::default()).with_driver(
        |driver| {
            let binding = driver::Binding::new(
                driver.driver_ref().timer(),
                owner::Capacity::new(NonZero::<u32>::MIN),
            );
            let id = Seq::new(binding.sequence())
                .try_next()
                .expect("request identity");
            let admitted = Cell::new(0);
            let mut evidence = Credits(Vec::with_capacity(4));
            STEPS.set(0);
            FINISHES.set(0);
            let parsed = parsing::Head::parse(Input::Borrowed(HEAD), u8::MAX)
                .expect("request head")
                .expect("complete head");
            let head = framing::Head::<Request<body::Segmented>>::parse(raw::Bound::empty(&parsed))
                .ok()
                .expect("framing")
                .expect("complete framing");
            let mut conn: session::Conn = session::Conn::default();
            let outcome = task::Endpoint::new(&mut conn)
                .dispatch::<Request<body::Segmented>, _, _, _, _>(
                    id,
                    session::Permit::new(),
                    head,
                    || {
                        admitted.set(admitted.get() + 1);
                        Some(|_, consumed, close, _, _: &mut ()| session::Outcome::Park {
                            consumed,
                            close,
                        })
                    },
                    |pending, parts, proof| pending.try_retain(parts, proof),
                    &mut evidence,
                );
            let session::RouteOutcome::AwaitBody { pending, .. } = outcome else {
                panic!("dispatch must await the request body");
            };
            let source = storage::Shared::try_copy(b"x").expect("body owner");
            let pointer = source.as_slice().as_ptr();
            let pending = pending
                .try_retain(owned::Segments::from(source), &mut evidence)
                .ok()
                .expect("accepted body");
            let outcome =
                pending.finish(
                    session::Complete {
                        body_len: 1,
                        consumed: 1,
                    },
                    session::Permit::new(),
                    &mut (),
                    || {
                        admitted.set(admitted.get() + 1);
                        Some(|_, consumed, close, _, _: &mut ()| {
                            session::Outcome::<Infallible>::Park { consumed, close }
                        })
                    },
                    &mut evidence,
                );
            let session::BodyOutcome::Pending(ready) = outcome else {
                panic!("body assembly must yield on its first step");
            };
            assert_eq!(STEPS.get(), 1);
            assert_eq!(admitted.get(), 0);
            let outcome =
                ready.resume(
                    session::Permit::new(),
                    &mut (),
                    || {
                        admitted.set(admitted.get() + 1);
                        Some(|_, consumed, close, _, _: &mut ()| {
                            session::Outcome::<Infallible>::Park { consumed, close }
                        })
                    },
                    &mut evidence,
                );
            let session::BodyOutcome::Pending(ready) = outcome else {
                panic!("body assembly must yield on its second step");
            };
            assert_eq!(STEPS.get(), 2);
            assert_eq!(admitted.get(), 0);
            let outcome = ready.resume(
                session::Permit::new(),
                &mut (),
                || {
                    Some(
                        |input: request::Input<'_, Request<body::Segmented>>,
                         consumed,
                         close,
                         _,
                         _: &mut ()| {
                            let request = match input {
                                request::Input::Ready { request } => Some(request),
                                request::Input::Buffered(_) => None,
                            }
                            .expect("completed body");
                            let (_, body) = request.into_parts();
                            let bytes = body.segment(0).expect("retained body");
                            assert_eq!(bytes, b"x");
                            assert_eq!(bytes.as_ptr(), pointer);
                            session::Outcome::<Infallible>::Park { consumed, close }
                        },
                    )
                },
                &mut evidence,
            );
            assert!(matches!(
                outcome,
                session::BodyOutcome::Complete(session::Outcome::Park { consumed: 0, .. })
            ));
            assert_eq!(STEPS.get(), 3);
            assert_eq!(FINISHES.get(), 1);
            assert_eq!(BUDGET.get(), body::DEFAULT_BUILD_STEP_BUDGET.get());
            let retained = credit::Amount::new(1, segments::Count::ONE);
            assert!(
                evidence.0
                    == [
                        credit::Mutation::Head,
                        credit::Mutation::Reserved(retained),
                        credit::Mutation::BuildReady,
                        credit::Mutation::Released {
                            credit: retained,
                            disposition: credit::Disposition::Admitted,
                        },
                    ]
            );
        },
        recovery::Policy::new(|event| events.push(event), common::recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| match error {
                    scope::Error::Create(error) => BranchError::First(error.kind()),
                    scope::Error::Close(error) => BranchError::Second(error.cause()),
                })
                .expect("driver lifetime");
        },
    );
    assert!(events.is_empty());
}

#[test]
fn buffered_body_starts_at_fin() {
    let mut events = Vec::new();
    Driver::new(settings::Config::default()).with_driver(
        |driver| {
            let binding = driver::Binding::new(
                driver.driver_ref().timer(),
                owner::Capacity::new(NonZero::<u32>::MIN),
            );
            for input in [tasks::EXPECT, &tasks::EXPECT[..tasks::EXPECT.len() - 1]] {
                let expected_consumed = if input.len() == tasks::EXPECT.len() {
                    tasks::EXPECT.len()
                } else {
                    1
                };
                let id = Seq::new(binding.sequence())
                    .try_next()
                    .expect("request identity");
                let mut evidence = Credits(Vec::new());
                let mut conn: session::Conn<'_, tasks::BufferedState<'_>> =
                    session::Conn::default();
                let (outcome, pointer) = {
                    let source = storage::Shared::try_copy(input).expect("wire owner");
                    let pointer = std::ptr::from_ref(source.as_slice().last().expect("wire byte"));
                    let parsed = parsing::Head::parse(Input::Buffered(source.view()), u8::MAX)
                        .expect("request head")
                        .expect("complete head");
                    let head = framing::Head::<Buffered>::parse(raw::Bound::empty(&parsed))
                        .ok()
                        .expect("framing")
                        .expect("complete framing");
                    let outcome = task::Endpoint::new(&mut conn).dispatch::<Buffered, _, _, _, _>(
                        id,
                        session::Permit::new(),
                        head,
                        || Some(tasks::admit_buffered),
                        |pending, parts, proof| pending.try_retain(parts, proof),
                        &mut evidence,
                    );
                    (outcome, pointer)
                };
                let (outcome, pointer) = match outcome {
                    session::RouteOutcome::Outcome(outcome) => {
                        assert_eq!(input.len(), tasks::EXPECT.len());
                        assert!(evidence.0 == [credit::Mutation::Head]);
                        (outcome, pointer)
                    }
                    session::RouteOutcome::AwaitBody {
                        pending,
                        expectation,
                        ..
                    } => {
                        assert!(expectation.is_continue());
                        assert_eq!(conn.async_state.0, 0);
                        let source = storage::Shared::try_copy(b"x").expect("body owner");
                        let pointer = source.as_slice().as_ptr();
                        let pending = pending
                            .try_retain(owned::Segments::from(source), &mut evidence)
                            .ok()
                            .expect("retained body");
                        assert_eq!(conn.async_state.0, 0);
                        let outcome = pending.finish(
                            session::Complete {
                                body_len: 1,
                                consumed: 1,
                            },
                            session::Permit::new(),
                            &mut conn.async_state,
                            || Some(tasks::admit_buffered),
                            &mut evidence,
                        );
                        let session::BodyOutcome::Complete(outcome) = outcome else {
                            panic!("buffered input admits directly at FIN");
                        };
                        let retained = credit::Amount::new(1, segments::Count::ONE);
                        assert!(
                            evidence.0
                                == [
                                    credit::Mutation::Head,
                                    credit::Mutation::Reserved(retained),
                                    credit::Mutation::Released {
                                        credit: retained,
                                        disposition: credit::Disposition::Admitted,
                                    },
                                ]
                        );
                        (outcome, pointer)
                    }
                    _ => panic!("buffered request completes or waits for FIN"),
                };
                assert!(matches!(
                    outcome,
                    session::Outcome::Park { consumed, .. } if consumed == expected_consumed
                ));
                assert_eq!(conn.async_state.0, 1);
                let request = match conn.async_state.1.expect("admitted request owner") {
                    request::Input::Buffered(request) => Some(request),
                    request::Input::Ready { .. } => None,
                }
                .expect("FIN preserves buffered construction");
                let build = Buffered::start_task(request).expect("owned request");
                let body::Step::Ready { output: built, .. } =
                    build.step(body::DEFAULT_BUILD_STEP_BUDGET)
                else {
                    panic!("complete request constructs immediately");
                };
                let (_, (mode, body)) = built.into_parts();
                assert!(matches!(mode, body::admit::Stream::Buffered));
                let bytes = body.segment(0).expect("retained body");
                assert_eq!(bytes, b"x");
                assert_eq!(bytes.as_ptr(), pointer);
            }
        },
        recovery::Policy::new(|event| events.push(event), common::recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| match error {
                    scope::Error::Create(error) => BranchError::First(error.kind()),
                    scope::Error::Close(error) => BranchError::Second(error.cause()),
                })
                .expect("driver lifetime");
        },
    );
    assert!(events.is_empty());
}
