use sark_http::payload::response::Method;

use dope_test::alloc::Tracker;
use o3::buffer::write::CopyError;
use request::scan;
use sark_http::h1::{
    field,
    head::{Boundary, Limit, ParseError, request, response},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn request_splits() {
    let wire = b"POST /items?q=1 HTTP/1.1\r\nHost: example.test\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\nbody";
    let head_len = wire.len() - b"body".len();
    let fields = [
        (b"Host".as_slice(), b"example.test".as_slice()),
        (b"Content-Length".as_slice(), b"4".as_slice()),
        (b"Connection".as_slice(), b"keep-alive".as_slice()),
    ];
    for split in 0..=head_len {
        let state = scan::State::new(wire[..split].to_vec(), Limit::DEFAULT);
        let mut seen = 0;
        let mut emit = |field: field::Entry<'_>| {
            assert_eq!(
                Some((field.name(), field.value())),
                fields.get(seen).copied(),
                "field {seen} at split {split}"
            );
            seen += 1;
            Ok::<_, ()>(())
        };
        let first = (state.advance(&mut emit)).expect("first fragment at {split}");
        let parts = match first {
            scan::Progress::Complete(parts) => parts,
            scan::Progress::More(mut state) => {
                (state.try_extend(&wire[split..]))
                    .as_ref()
                    .expect("test operation must succeed");
                match state
                    .advance(&mut emit)
                    .expect("second fragment at {split}")
                {
                    scan::Progress::Complete(parts) => parts,
                    outcome => {
                        assert!(
                            matches!(outcome, scan::Progress::Complete(_)),
                            "second fragment at {split}"
                        );
                        return;
                    }
                }
            }
        };
        assert_eq!(parts.wire_len().get() as usize, head_len, "split {split}");
        assert_eq!(
            parts.as_ref().line(),
            (b"POST".as_slice(), b"/items?q=1".as_slice())
        );
        assert_eq!(seen, 3, "split {split}");
    }
}

#[test]
fn response_splits() {
    let wire =
        b"HTTP/1.1 204 No Content\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\nnext";
    let head_len = wire
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .expect("response fixture has a head terminator")
        + 4;
    let fields = [
        (b"Connection".as_slice(), b"keep-alive".as_slice()),
        (b"Content-Length".as_slice(), b"0".as_slice()),
    ];
    for split in 0..=head_len {
        response::State::with(
            wire[..split].to_vec(),
            Method::Other,
            Limit::DEFAULT,
            |mut source, progress| {
                let mut seen = 0usize;
                let (progress, parts) = {
                    let mut emit = |field: field::Entry<'_>| {
                        assert_eq!(
                            Some((field.name(), field.value())),
                            fields.get(seen).copied(),
                            "field {seen} at split {split}"
                        );
                        seen += 1;
                        Ok::<_, ()>(())
                    };
                    let (progress, first) = progress
                        .advance(&source, &mut emit)
                        .expect("test operation must succeed");
                    match first {
                        Some(parts) => (progress, parts),
                        None => {
                            source
                                .try_extend(&wire[split..])
                                .expect("append response suffix");
                            let (progress, parts) = progress
                                .advance(&source, &mut emit)
                                .expect("test operation must succeed");
                            (
                                progress,
                                parts.expect("second fragment completes the response head"),
                            )
                        }
                    }
                };
                assert_eq!(parts.wire_len().get() as usize, head_len, "split {split}");
                assert_eq!(parts.status().as_u16(), 204);
                assert_eq!(seen, 2, "split {split}");
                let mut replay = 0;
                let (_, again) = progress
                    .advance(&source, |_| -> Result<(), ()> {
                        replay += 1;
                        Ok(())
                    })
                    .expect("test operation must succeed");
                assert_eq!(
                    again
                        .expect("completed head remains available")
                        .wire_len()
                        .get() as usize,
                    head_len,
                    "repeat split {split}"
                );
                assert_eq!(replay, 0, "repeat split {split}");
            },
        );
    }
}

#[test]
fn request_grammar() {
    let cases = [
        (
            b"GET / HTTP/1.1\r\nHost: example.test\r\nX-abcdefghijklmnopqrstuvwxyz0123456789-ABCDEFGHIJKLMNOPQRSTUVWXYZ: 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\r\n\r\n"
                .as_slice(),
            true,
        ),
        (b"GET / HTTP/1.1\r\nHost: example.test\r\n Folded: no\r\n\r\n".as_slice(), false),
        (b"GET / HTTP/1.1\r\nHost: example.test\n\n".as_slice(), false),
        (b"GET / HTTP/1.1\r\nHost: example.test\r\nX-A: a\0b\r\n\r\n".as_slice(), false),
        (b"GET / HTTP/1.1\r\nHost: example.test\r".as_slice(), false),
    ];
    for (wire, accepted) in cases {
        let mut whole = Vec::new();
        let complete = request::Parts::parse(wire, Limit::DEFAULT, |field| {
            whole.push((field.name_span(), field.value_span()));
            Ok::<_, ()>(())
        })
        .is_ok_and(|parts| parts.is_some_and(|head| head.wire_len().get() as usize == wire.len()));
        assert_eq!(complete, accepted);

        for split in 0..=wire.len() {
            let state = scan::State::new(wire[..split].to_vec(), Limit::DEFAULT);
            let mut part = Vec::new();
            let mut emit = |field: field::Entry<'_>| {
                part.push((field.name_span(), field.value_span()));
                Ok::<_, ()>(())
            };
            let first = state.advance(&mut emit);
            let split_ok = match first {
                Ok(scan::Progress::Complete(head)) => head.wire_len().get() as usize == wire.len(),
                Ok(scan::Progress::More(mut state)) => {
                    (state.try_extend(&wire[split..]))
                        .as_ref()
                        .expect("test operation must succeed");
                    match state.advance(&mut emit) {
                        Ok(scan::Progress::Complete(head)) => {
                            head.wire_len().get() as usize == wire.len()
                        }
                        Ok(scan::Progress::More(_)) => false,
                        Err((source, error)) => {
                            assert_eq!(source, wire);
                            assert!(!matches!(error, ParseError::Sink(())));
                            false
                        }
                    }
                }
                Err((source, error)) => {
                    assert_eq!(source, &wire[..split]);
                    assert!(!matches!(error, ParseError::Sink(())));
                    false
                }
            };
            assert_eq!(split_ok, complete, "split {split}: {wire:?}");
            assert!((part) == (whole), "field spans at split {split}: {wire:?}");
        }
    }
}

#[test]
fn request_sink_stops() {
    let wire = b"GET / HTTP/1.1\r\nHost: example.test\r\nX-Later: skipped\r\n\r\n";
    let state = scan::State::new(wire.as_slice(), Limit::DEFAULT);
    let mut seen = 0;
    let result = state.advance(|field| {
        seen += 1;
        assert_eq!(
            (field.name(), field.value()),
            (b"Host".as_slice(), b"example.test".as_slice())
        );
        Err::<(), _>("stop")
    });
    assert!(matches!(result.err(), Some((source, ParseError::Sink("stop"))) if source == wire));
    assert_eq!(seen, 1);
}

#[test]
fn response_sink_stops() {
    let wire = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    let source = wire.to_vec();
    let pointer = source[wire.len() - 5..].as_ptr();
    response::State::with(source, Method::Other, Limit::DEFAULT, |source, progress| {
        let mut seen = 0;
        let error = progress
            .advance(&source, |field| {
                seen += 1;
                assert_eq!(
                    (field.name(), field.value()),
                    (b"Content-Length".as_slice(), b"0".as_slice())
                );
                Err::<(), _>(field.value())
            })
            .err()
            .expect("sink stops progress");
        assert!(matches!(error, ParseError::Sink(value)
            if value == b"0" && value.as_ptr() == pointer));
        assert_eq!(source.as_ref(), wire);
        assert_eq!(seen, 1);
    });
}

#[test]
fn start_line_limit() {
    let wire = b"GET /\r";
    let limit = Limit::DEFAULT.with_bytes(5);
    let mut fields = 0;
    let complete = request::Parts::parse(wire, limit, |_| -> Result<(), ()> {
        fields += 1;
        Ok(())
    });
    assert!(matches!(
        complete,
        Err(ParseError::Boundary(Boundary::TooLarge {
            actual: 7,
            limit: 5
        }))
    ));
    let state = scan::State::new(wire[..5].to_vec(), limit);
    let mut state = match state
        .advance(|_| -> Result<(), ()> {
            fields += 1;
            Ok(())
        })
        .expect("the prefix is incomplete")
    {
        scan::Progress::More(state) => state,
        outcome => {
            assert!(
                matches!(outcome, scan::Progress::More(_)),
                "the prefix is incomplete"
            );
            return;
        }
    };
    (state.try_extend(&wire[5..]))
        .as_ref()
        .expect("test operation must succeed");
    assert!(matches!(state.advance(|_| -> Result<(), ()> {
            fields += 1;
            Ok(())
        }).err(), Some((source, ParseError::Boundary(Boundary::TooLarge { actual: 7, limit: 5 }))) if source == wire));
    assert_eq!(fields, 0);
}

#[test]
fn resumed_line_bounds_head() {
    let wire = b"GET / HTTP/1.1\r\nHost: x\r\n\r\npayload beyond the head limit";
    let limit = Limit::DEFAULT.with_bytes(27);
    let state = scan::State::new(wire[..15].to_vec(), limit);
    let mut fields = 0;
    let mut state = match state
        .advance(|_| -> Result<(), ()> {
            fields += 1;
            Ok(())
        })
        .expect("the prefix is incomplete")
    {
        scan::Progress::More(state) => state,
        outcome => {
            assert!(
                matches!(outcome, scan::Progress::More(_)),
                "the prefix is incomplete"
            );
            return;
        }
    };
    assert_eq!(fields, 0);
    (state.try_extend(&wire[15..]))
        .as_ref()
        .expect("test operation must succeed");
    let parts = match state
        .advance(|field| {
            assert_eq!(
                (field.name(), field.value()),
                (b"Host".as_slice(), b"x".as_slice())
            );
            fields += 1;
            Ok::<(), ()>(())
        })
        .expect("the complete head ends inside the limit")
    {
        scan::Progress::Complete(parts) => parts,
        outcome => {
            assert!(
                matches!(outcome, scan::Progress::Complete(_)),
                "the complete head ends inside the limit"
            );
            return;
        }
    };
    assert_eq!(parts.wire_len().get(), 27);
    assert_eq!(parts.as_ref().line(), (b"GET".as_slice(), b"/".as_slice()));
    assert_eq!(fields, 1);
}

#[test]
fn copy_preserves_progress() {
    let prefix = b"GET / HTTP/1.1\r\nHost: example.test\r\n";
    let state = scan::State::new(prefix.as_slice(), Limit::DEFAULT);
    let mut fields = 0;
    let state = match state
        .advance(|_| {
            fields += 1;
            Ok::<_, ()>(())
        })
        .expect("valid prefix")
    {
        scan::Progress::More(state) => state,
        outcome => {
            assert!(
                matches!(outcome, scan::Progress::More(_)),
                "unfinished head retains its state"
            );
            return;
        }
    };
    assert_eq!(fields, 1);
    let target = Vec::with_capacity(128);
    let (result, activity) = Tracker::measure(|| state.try_copy(target));
    assert_eq!(activity, [0; 4]);
    let mut state = match result {
        Ok(state) => state,
        outcome => {
            assert!(outcome.is_ok(), "empty target accepts the exact prefix");
            return;
        }
    };
    assert_eq!(state.as_ref(), prefix);
    state
        .try_extend(b"Connection: close\r\n\r\n")
        .expect("append remaining head");
    let parts = match state
        .advance(|field| {
            assert_eq!(field.name(), b"Connection");
            fields += 1;
            Ok::<_, ()>(())
        })
        .expect("complete copied head")
    {
        scan::Progress::Complete(parts) => parts,
        outcome => {
            assert!(
                matches!(outcome, scan::Progress::Complete(_)),
                "copied head completes"
            );
            return;
        }
    };
    assert_eq!(parts.as_ref().line(), (b"GET".as_slice(), b"/".as_slice()));
    let end = parts.wire_len();
    let route = parts.into_route();
    assert_eq!(route.as_ref().method(), "GET");
    assert_eq!(route.as_ref().target(), "/");
    let source = route.into_source();
    assert_eq!(source.len(), end.get() as usize);
    assert_eq!(fields, 2);
}

#[test]
fn copy_rejects_prefix() {
    let source = b"GET / HTTP/1.1\r\nHost: example.test\r\n";
    let state = scan::State::new(source.as_slice(), Limit::DEFAULT);
    let target = vec![b'x'];
    let (result, activity) = Tracker::measure(|| state.try_copy(target));
    assert_eq!(activity, [0; 4]);
    let (target, cause) = result
        .err()
        .expect("nonempty destination rejects relocation");
    assert_eq!(state.as_ref(), source);
    assert_eq!(target, b"x");
    assert!(matches!(cause, CopyError::Prefix(len) if len.get() == 1));
    let mut state = match state.try_copy(Vec::with_capacity(128)) {
        Ok(state) => state,
        outcome => {
            assert!(
                outcome.is_ok(),
                "retained parser can retry with an empty owner"
            );
            return;
        }
    };
    state
        .try_extend(b"\r\n")
        .expect("complete the original head");
    assert!(matches!(
        state.advance(|_| Ok::<_, ()>(())),
        Ok(scan::Progress::Complete(_))
    ));
}

#[test]
fn copy_rejects_capacity() {
    use o3::buffer::storage::inline;
    let prefix = b"GET / HTTP/1.1\r\nHost: example.test\r\n";
    let state = scan::State::new(prefix.as_slice(), Limit::DEFAULT);
    let target = inline::Bytes::<4>::new();
    let (result, activity) = Tracker::measure(|| state.try_copy(target));
    assert_eq!(activity, [0; 4]);
    let (target, cause) = result.err().expect("the prefix exceeds the destination");
    let cause = match cause {
        CopyError::Write(cause) => cause,
        outcome => {
            assert!(
                matches!(outcome, CopyError::Write(_)),
                "the empty destination fails only its write admission"
            );
            return;
        }
    };
    let (required, room) = cause.into_parts();
    assert_eq!(required.get(), prefix.len());
    assert_eq!(room, 4);
    assert_eq!(state.as_ref(), prefix);
    assert!(target.is_empty());
}
