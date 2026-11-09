pub mod common;

use std::{
    convert::{self, Infallible},
    num::NonZero,
    ops::ControlFlow,
    slice,
};

use common::head::{self, OWNED_PREFIX, OWNED_SUFFIX, OWNED_TAIL, Request, SETS};
use dope_test::alloc::Tracker;
use o3::buffer::write::Sink as _;
use sark_h1::{
    Input,
    session::{Outcome, Permit, RouteOutcome},
    wire::{
        head::{Block, Error, line},
        parsing::{
            self,
            framing::{Head, HeadProgress, PendingHead, Plain},
            raw,
        },
    },
};

use sark_http::{
    field::{head::Error::ContentLength, text::IntError},
    h1::{
        codec::canned,
        head::{line::Version, request::Coding},
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn setters_and_owner() {
    SETS.set(Vec::new());
    let mut input = b"GET / HTTP/1.1\r\nHost: local\r\nX-Count: once\r\nX-Other: ".to_vec();
    let source = parsing::Head::parse(Input::Borrowed(&input), u8::MAX)
        .expect("line")
        .expect("complete line");
    let mut pending =
        match Head::<Request, Plain, head::Admission>::start(raw::Bound::empty(&source))
            .ok()
            .expect("fields")
        {
            HeadProgress::Pending(pending) => Some(pending),
            HeadProgress::Complete(_) => None,
        }
        .expect("pending fields");
    let values = SETS.take();
    assert_eq!(values.as_slice(), [b"once"]);
    SETS.set(values);
    input.fill(b'!');
    for byte in b"fragmented" {
        let outcome = pending
            .try_resume::<Infallible, Infallible, _>(
                Input::Borrowed(slice::from_ref(byte)),
                Permit::new(),
                convert::identity,
                |_, _| Outcome::Close(canned::NOT_FOUND).into(),
            )
            .ok()
            .expect("head resumed");
        pending = match outcome {
            RouteOutcome::AwaitHead { pending, .. } => Some(pending),
            _ => None,
        }
        .expect("pending field");
    }
    let outcome = pending
        .try_resume::<Infallible, Infallible, _>(
            Input::Borrowed(b"\r\n\r\n"),
            Permit::new(),
            convert::identity,
            |head, _| {
                let (head, admission) = head.into_admission();
                assert_eq!((admission.fields, admission.finishes), (3, 1));
                assert_eq!(
                    admission
                        .other
                        .expect("field span")
                        .get(head.source().input().as_slice()),
                    Some(b"fragmented".as_slice())
                );
                let whole = Head::<common::Request, Plain, head::Admission>::parse(
                    raw::Bound::empty(head.source()),
                )
                .ok()
                .flatten()
                .expect("whole head");
                assert_eq!(whole.into_admission().1.other, admission.other);
                assert!(
                    head.source()
                        .input()
                        .as_slice()
                        .starts_with(b"GET / HTTP/1.1\r\nHost: local\r\nX-Count: once\r\n")
                );
                Outcome::Close(canned::NOT_FOUND).into()
            },
        )
        .ok()
        .expect("head resumed");
    assert!(matches!(outcome, RouteOutcome::Outcome(Outcome::Close(_))));
    assert_eq!(SETS.take().as_slice(), [b"once"]);
}

#[test]
fn captured_list_members() {
    let wire = head::CSV_FIELD;
    for split in Request::PREFIX.len()..=wire.len() {
        SETS.set(Vec::new());
        let source = parsing::Head::parse(Input::Borrowed(&wire[..split]), u8::MAX)
            .expect("request line")
            .expect("complete request line");
        match Head::<Request>::start(raw::Bound::empty(&source))
            .ok()
            .expect("admitted field")
        {
            HeadProgress::Complete(_) => assert_eq!(split, wire.len()),
            HeadProgress::Pending(pending) => {
                let outcome = pending.try_resume::<Infallible, Infallible, _>(
                    Input::Borrowed(&wire[split..]),
                    Permit::new(),
                    convert::identity,
                    |_, _| Outcome::Close(canned::NOT_FOUND).into(),
                );
                assert!(matches!(
                    outcome,
                    Ok(RouteOutcome::Outcome(Outcome::Close(_)))
                ));
            }
        }
        assert_eq!(SETS.take().as_slice(), [b",gzip,"]);
    }
}

#[test]
fn bad_field_rejects() {
    SETS.set(Vec::new());
    let source = parsing::Head::parse(
        Input::Borrowed(b"GET / HTTP/1.1\r\nHost: local\r\nContent-Length: "),
        u8::MAX,
    )
    .expect("line")
    .expect("complete line");
    let pending = match Head::<Request>::start(raw::Bound::empty(&source))
        .ok()
        .expect("fields")
    {
        HeadProgress::Pending(pending) => Some(pending),
        HeadProgress::Complete(_) => None,
    }
    .expect("pending fields");
    let outcome = pending.try_resume::<Infallible, Infallible, _>(
        Input::Borrowed(b"invalid\r\n"),
        Permit::new(),
        convert::identity,
        |_, _| Outcome::Close(canned::NOT_FOUND).into(),
    );
    assert!(matches!(
        outcome,
        Err(parsing::Request::Head(Error::Line(line::Error::Field {
            cause: ContentLength(IntError::InvalidByte),
            ..
        })))
    ));
    assert!(SETS.take().is_empty());
}

#[test]
fn upgrade_errors_follow_canonical_field_admission() {
    let allowed = Request::upgrade_limit(false);
    let limit = Request::upgrade_limit(true);
    let rejected = Request::bounded(64, 0);
    for (wire, expected, through) in head::INVALID_UPGRADES
        .into_iter()
        .map(|(wire, error)| (wire, Some(error), wire.len()))
        .chain([
            (
                allowed.as_slice(),
                Some(sark_ws::upgrade::Error::Connection),
                allowed.len(),
            ),
            (limit.as_slice(), None, limit.len()),
            (
                rejected.as_slice(),
                Some(sark_ws::upgrade::Error::Connection),
                Request::PREFIX.len(),
            ),
        ])
    {
        for split in Request::PREFIX.len()..=through {
            let source = parsing::Head::parse(Input::Borrowed(&wire[..split]), u8::MAX)
                .expect("request line")
                .expect("complete request line");
            let measurement = Tracker::begin();
            let error = match Head::<Request, Plain, sark_h1::route::duplex::Upgrade>::start(
                raw::Bound::empty(&source),
            ) {
                Err(error) => error,
                Ok(HeadProgress::Pending(pending)) => match pending
                    .try_resume::<Infallible, Infallible, _>(
                        Input::Borrowed(&wire[split..]),
                        Permit::new(),
                        convert::identity,
                        |_, _| panic!("invalid opening reached the handler"),
                    ) {
                    Err(error) => error,
                    Ok(_) => panic!("invalid opening did not fail immediately"),
                },
                Ok(HeadProgress::Complete(_)) => panic!("invalid opening admitted"),
            };
            let allocation = measurement.finish();
            assert!(allocation.allocated_bytes < 32 * 1024 + 256, "{allocation:?}");
            if expected.is_none() {
                assert!(matches!(
                    error,
                    parsing::Request::Head(
                        Error::Fields {
                            actual: 129,
                            limit: 128
                        } | Error::Syntax(sark_http::h1::field::syntax::Error::TooMany)
                    )
                ));
            }
            let actual = match error.into_consume::<Infallible>() {
                Outcome::Failed(sark_h1::wire::Failure::Upgrade(error)) => Some(error),
                Outcome::Close(reply) if reply.as_slice() == canned::BAD_REQUEST.as_slice() => None,
                _ => panic!("opening error lost"),
            };
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn transfer_coding_admission() {
    for &(coding, supported) in head::TRANSFER_CODINGS {
        let wire = Request::transfer(coding);
        for split in Request::TRANSFER_PREFIX.len()..=wire.len() {
            let source = parsing::Head::parse(Input::Borrowed(&wire[..split]), u8::MAX)
                .expect("request line")
                .expect("complete request line");
            let result = match Head::<Request>::start(raw::Bound::empty(&source)) {
                Ok(HeadProgress::Complete(_)) => Ok(()),
                Ok(HeadProgress::Pending(pending)) => pending
                    .try_resume::<Infallible, Infallible, _>(
                        Input::Borrowed(&wire[split..]),
                        Permit::new(),
                        convert::identity,
                        |_, _| {
                            assert!(supported, "unsupported coding reached the handler");
                            Outcome::Close(canned::NOT_FOUND).into()
                        },
                    )
                    .map(|outcome| {
                        assert!(matches!(
                            outcome,
                            RouteOutcome::Outcome(Outcome::Close(reply))
                                if reply.as_slice() == canned::NOT_FOUND.as_slice()
                        ));
                    }),
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => assert!(supported, "accepted {coding:?} split at {split}"),
                Err(error) => {
                    assert!(!supported, "rejected {coding:?} split at {split}");
                    assert!(matches!(
                        error,
                        parsing::Request::Head(Error::UnsupportedTransferEncoding)
                    ));
                    assert!(matches!(
                        error.into_consume::<Infallible>(),
                        Outcome::Close(reply)
                            if reply.as_slice() == canned::NOT_IMPLEMENTED.as_slice()
                    ));
                }
            }
        }
    }
}

#[test]
fn http10_ignores_upgrade() {
    let wire =
        b"GET / HTTP/1.0\r\nHost: local\r\nConnection: upgrade\r\nUpgrade: web socket\r\n\r\n";
    for split in [wire.len(), wire.len() - 8, wire.len() - 3] {
        SETS.set(Vec::new());
        let source = parsing::Head::parse(Input::Borrowed(&wire[..split]), u8::MAX)
            .expect("line")
            .expect("complete line");
        match Head::<Request>::start(raw::Bound::empty(&source))
            .ok()
            .expect("HTTP/1.0 fields")
        {
            HeadProgress::Complete(_) => assert_eq!(split, wire.len()),
            HeadProgress::Pending(pending) => {
                let outcome = pending.try_resume::<Infallible, Infallible, _>(
                    Input::Borrowed(&wire[split..]),
                    Permit::new(),
                    convert::identity,
                    |head, _| {
                        assert_eq!(head.source().request().version(), Version::Ten);
                        Outcome::Close(canned::NOT_FOUND).into()
                    },
                );
                assert!(matches!(
                    outcome,
                    Ok(RouteOutcome::Outcome(Outcome::Close(_)))
                ));
            }
        }
        assert_eq!(SETS.take().as_slice(), [b"web socket"]);
    }

    let source = parsing::Head::parse(
        Input::Borrowed(b"GET / HTTP/1.0\r\nHost: local\r\nUpgrade: web"),
        u8::MAX,
    )
    .expect("line")
    .expect("complete line");
    let pending = match Head::<Request>::start(raw::Bound::empty(&source))
        .ok()
        .expect("pending fields")
    {
        HeadProgress::Pending(pending) => Some(pending),
        HeadProgress::Complete(_) => None,
    }
    .expect("fragmented field");
    let outcome = pending.try_resume::<Infallible, Infallible, _>(
        Input::Borrowed(b"\0socket\r\n\r\n"),
        Permit::new(),
        convert::identity,
        |_, _| Outcome::Close(canned::NOT_FOUND).into(),
    );
    assert!(matches!(
        outcome,
        Err(parsing::Request::Head(Error::Syntax(
            sark_http::h1::field::syntax::Error::Value { actual: 0, .. }
        )))
    ));
}

#[test]
fn whole_and_split() {
    SETS.set(Vec::new());
    for field in Request::LONG_FIELDS {
        for width in [8191, 8192, 8193] {
            let input = Request::long(width, field);
            let source = parsing::Head::parse(Input::Borrowed(&input), u8::MAX)
                .expect("line")
                .expect("complete line");
            let complete = Head::<Request>::start(raw::Bound::empty(&source));
            assert_eq!(
                matches!(complete, Ok(HeadProgress::Complete(_))),
                width <= 8192
            );
            assert!(SETS.take().is_empty());
            for split in [Request::PREFIX.len() + field.len(), input.len() - 3] {
                let source = parsing::Head::parse(Input::Borrowed(&input[..split]), u8::MAX)
                    .expect("line")
                    .expect("complete line");
                let pending = match Head::<Request>::start(raw::Bound::empty(&source)) {
                    Ok(HeadProgress::Pending(pending)) => Some(pending),
                    Err(parsing::Request::Head(Error::Line(line::Error::Limit {
                        observed: 8194,
                        limit: 8192,
                    }))) if width > 8192 => continue,
                    _ => None,
                }
                .expect("pending field");
                let outcome = pending.try_resume::<Infallible, Infallible, _>(
                    Input::Borrowed(&input[split..]),
                    Permit::new(),
                    convert::identity,
                    |_, _| Outcome::Close(canned::NOT_FOUND).into(),
                );
                assert!(if width <= 8192 {
                    matches!(outcome, Ok(RouteOutcome::Outcome(Outcome::Close(reply)))
                        if reply.as_slice() == canned::NOT_FOUND.as_slice())
                } else {
                    matches!(
                        outcome,
                        Err(parsing::Request::Head(Error::Syntax(
                            sark_http::h1::field::syntax::Error::TooLong
                        )))
                    )
                });
            }
        }
    }
    assert!(SETS.take().is_empty());
}

#[test]
fn owned_line_keeps_head() {
    SETS.set(Vec::new());
    let mut source =
        parsing::Source::try_with(NonZero::new(128).expect("capacity")).expect("source");
    source.write_slice(OWNED_PREFIX).expect("prefix");
    let pointer = source.as_ref().as_ptr();
    let source = match parsing::Owned::try_parse(source, u8::MAX)
        .ok()
        .expect("line")
    {
        ControlFlow::Continue(source) => Some(source),
        ControlFlow::Break(_) => None,
    }
    .expect("complete");
    let outcome = PendingHead::<Request>::try_start::<Infallible, Infallible, _>(
        raw::Bound::empty(source),
        Permit::new(),
        convert::identity,
        |_, _| Outcome::Close(canned::BAD_REQUEST).into(),
    )
    .ok()
    .expect("headers");
    let pending = match outcome {
        RouteOutcome::AwaitHead { pending, .. } => Some(pending),
        _ => None,
    }
    .expect("pending");
    let outcome = pending.try_resume::<Infallible, Infallible, _>(
        Input::Borrowed(OWNED_SUFFIX),
        Permit::new(),
        convert::identity,
        |head, _| {
            assert_eq!(head.source().input().as_slice().as_ptr(), pointer);
            assert!(head.source().input().as_slice().ends_with(OWNED_TAIL));
            Outcome::Close(canned::NOT_FOUND).into()
        },
    );
    assert!(
        matches!(outcome, Ok(RouteOutcome::Outcome(Outcome::Close(reply)))
        if reply.as_slice() == canned::NOT_FOUND.as_slice())
    );
    assert_eq!(SETS.take().as_slice(), [b"once"]);
}

#[test]
fn bounds_head_length() {
    let mut accepted = common::Request::field_block(64 * 1024);
    accepted.resize(accepted.len() + 64 * 1024, 0xa5);
    assert!(matches!(
        Block::<()>::parse::<common::Request>(&accepted, 0, 128, Coding::Plain, Version::Eleven),
        Ok(Some(_))
    ));
    let rejected = common::Request::field_block(64 * 1024 + 1);
    assert!(matches!(
        Block::<()>::parse::<common::Request>(&rejected, 0, 128, Coding::Plain, Version::Eleven),
        Err(Error::Length { len }) if len == 64 * 1024 + 1
    ));
}

#[test]
fn route_bound_precedes_retention() {
    const LIMIT: usize = 16 * 1024;
    for extra in [0, 1] {
        let wire = Request::bounded(LIMIT, extra);
        for split in [16, LIMIT - 2, wire.len()] {
            let source = parsing::Head::parse(Input::Borrowed(&wire[..split]), u8::MAX)
                .expect("request line")
                .expect("complete line");
            let measurement = Tracker::begin();
            let started = Head::<Request, Plain, head::Admission>::start(raw::Bound::empty(&source));
            let allocation = measurement.finish();
            match (split == wire.len(), started) {
                (true, Ok(HeadProgress::Complete(head))) => {
                    assert_eq!(extra, 0);
                    assert_eq!(allocation.allocations, 0);
                    assert_eq!(head.into_admission().1.finishes, 1);
                }
                (false, Ok(HeadProgress::Pending(pending))) => {
                    let measurement = Tracker::begin();
                    let outcome = pending.try_resume::<Infallible, Infallible, _>(
                        Input::Borrowed(&wire[split..]),
                        Permit::new(),
                        convert::identity,
                        |head, _| {
                            assert_eq!(extra, 0);
                            let (head, admission) = head.into_admission();
                            assert_eq!(admission.finishes, 1);
                            assert_eq!(head.source().input().as_slice(), wire);
                            Outcome::Close(canned::NOT_FOUND).into()
                        },
                    );
                    let allocation = measurement.finish();
                    if extra == 0 {
                        assert!(matches!(
                            outcome,
                            Ok(RouteOutcome::Outcome(Outcome::Close(_)))
                        ));
                    } else {
                        assert!(
                            matches!(outcome, Err(parsing::Request::Head(Error::Length { len })) if len == wire.len())
                        );
                        assert!(
                            allocation.allocated_bytes < 2 * LIMIT + 256,
                            "{allocation:?}"
                        );
                    }
                }
                (true, Err(parsing::Request::Head(Error::Length { len }))) => {
                    assert_eq!(extra, 1);
                    assert_eq!(len, wire.len());
                    assert_eq!(allocation.allocations, 0);
                }
                _ => panic!("header admission"),
            }
        }
    }
    let limit = 64 * 1024;
    for extra in [0, 1] {
        let wire = Request::bounded(limit, extra);
        let source = parsing::Head::parse(Input::Borrowed(&wire[..16]), u8::MAX)
            .expect("request line")
            .expect("complete line");
        let RouteOutcome::AwaitReject { pending, .. } =
            parsing::Rejected::start::<Infallible, Infallible, Infallible>(
                &source,
                Permit::new(),
                canned::NOT_FOUND,
            )
        else {
            panic!("pending rejected head");
        };
        let measurement = Tracker::begin();
        let outcome = pending.resume::<Infallible, Infallible, Infallible>(
            Input::Borrowed(&wire[16..]),
            Permit::new(),
        );
        let allocation = measurement.finish();
        let expected = if extra == 0 {
            canned::NOT_FOUND
        } else {
            canned::PAYLOAD_TOO_LARGE
        };
        assert!(matches!(
            outcome,
            RouteOutcome::Outcome(Outcome::Close(reply)) if reply.as_slice() == expected.as_slice()
        ));
        assert!(
            allocation.allocated_bytes < 2 * limit + 256,
            "{allocation:?}"
        );
    }
}

#[test]
fn keeps_prefix_cursor() {
    SETS.set(Vec::new());
    assert!(matches!(
        Block::<()>::parse::<head::Request<4>>(
            head::PREFIX_HOST,
            0,
            16,
            Coding::Plain,
            Version::Eleven
        ),
        Ok(Some(_))
    ));
    assert!(matches!(
        Block::<()>::parse::<head::Request<5>>(
            head::PREFIX_HOST,
            0,
            16,
            Coding::Plain,
            Version::Eleven
        ),
        Err(Error::Line(line::Error::Name {
            offset: 5,
            actual: b' '
        }))
    ));
    assert!(matches!(
        Block::<()>::parse::<head::Request<8>>(
            head::PREFIX_LENGTH,
            0,
            16,
            Coding::Plain,
            Version::Eleven
        ),
        Err(Error::Line(line::Error::Field {
            cause: ContentLength(IntError::InvalidByte),
            ..
        }))
    ));
    assert!(SETS.take().is_empty());
}
