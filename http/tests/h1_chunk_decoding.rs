pub mod common;

use common::wire::chunks::{self, Commit as _};
use dope_test::alloc::Tracker;
use sark_http::h1::{
    chunk::{self, Limit},
    codec::decode::chunks::{Decoder, Error, Event},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn splits_preserve_framing() {
    for (wire, expected, maximum) in chunks::FRAMING {
        let limits = Limit::with_body(maximum);
        let mut body = Vec::with_capacity(expected.len());
        for split in 0..=wire.len() {
            let mut framer = chunk::Framer::default();
            let mut complete = 0usize;
            let mut ends = 0usize;
            let mut total = 0usize;
            body.clear();
            let ((), allocs) = Tracker::count(|| {
                for mut input in [&wire[..split], &wire[split..]] {
                    loop {
                        let (next, event) = framer
                            .decode(input, &limits, usize::MAX)
                            .expect("valid chunk split");
                        framer = next;
                        let used = match event {
                            chunk::Fragment::Data {
                                bytes,
                                boundary,
                                consumed,
                            } => {
                                if boundary == chunk::Boundary::End {
                                    ends += 1;
                                }
                                body.extend_from_slice(bytes);
                                consumed
                            }
                            chunk::Fragment::Trailer(consumed) => consumed,
                            chunk::Fragment::Complete {
                                bytes: lines,
                                consumed,
                            } => {
                                assert!(
                                    consumed.get() > 0 && lines.len() <= consumed.get(),
                                    "complete fragment must make progress within the bytes it consumed"
                                );
                                complete += 1;
                                consumed
                            }
                            chunk::Fragment::More(used) => {
                                total += used;
                                input = &input[used..];
                                break;
                            }
                        };
                        total += used.get();
                        input = &input[used.get()..];
                        if input.is_empty() || complete != 0 {
                            break;
                        }
                    }
                    assert!(input.is_empty());
                }
            });
            assert_eq!(ends, 2, "two declared chunks must end");
            assert_eq!(body, expected, "split {split}");
            assert_eq!(complete, 1, "split {split}");
            assert_eq!(total, wire.len());
            assert_eq!(allocs, 0);
        }
    }
}

#[test]
fn size_boundary() {
    let digits = [b'f'; size_of::<usize>() * 2];
    let limits = Limit::with_body(usize::MAX);
    let (framer, _) = chunk::Framer::new()
        .decode(&digits, &limits, 0)
        .expect("largest hex prefix");
    let (body, _) = framer.decode(b"\r\n", &limits, 0).expect("largest chunk");
    assert_eq!(body.body_bytes(), usize::MAX);
    assert!(matches!(
        framer.decode(b"0", &limits, 0),
        Err(chunk::Error::Overflow {
            axis: chunk::Axis::Size,
            value: usize::MAX,
            added: 0,
        })
    ));
}

#[test]
fn limits_differ() {
    let body = chunk::Framer::new();
    assert!(matches!(
        body.decode(b"2\r\nok\r\n0\r\n\r\n", &Limit::with_body(1), usize::MAX),
        Err(chunk::Error::Limit {
            axis: chunk::Axis::Body,
            actual: 2,
            maximum: 1
        })
    ));
    let line = chunk::Framer::new();
    assert!(matches!(
        line.decode(
            b"1;extension=value\r\n",
            &Limit::new(1024, usize::MAX, 4, 1024, 8),
            usize::MAX
        ),
        Err(chunk::Error::Limit {
            axis: chunk::Axis::Line,
            actual: 5,
            maximum: 4
        })
    ));
    let fields = chunk::Framer::new();
    assert!(matches!(
        fields.decode(
            b"0\r\nX: y\r\n\r\n",
            &Limit::new(1024, usize::MAX, 1024, 1024, 0),
            usize::MAX
        ),
        Err(chunk::Error::Limit {
            axis: chunk::Axis::Fields,
            actual: 1,
            maximum: 0
        })
    ));
}

#[test]
fn rejects_forbidden() {
    let limits = Limit::with_body(1024);
    let (framer, consumed) = match chunk::Framer::new()
        .decode(b"0\r\nContent-Len", &limits, usize::MAX)
        .expect("partial trailer")
    {
        (framer, chunk::Fragment::Trailer(consumed)) => Some((framer, consumed)),
        _ => None,
    }
    .expect("partial trailer");
    assert_eq!(consumed.get(), 14);
    assert!(matches!(
        framer.decode(b"gth: 1\r\n\r\n", &limits, usize::MAX),
        Err(chunk::Error::Forbidden {
            offset: 17,
            field: chunk::ban::Field::ContentLength
        })
    ));
}

#[test]
fn bounds_data_only() {
    let prefix = b"4000\r\n";
    let mut wire = Vec::with_capacity(prefix.len() + chunks::DATA_BYTES + 7);
    wire.extend_from_slice(prefix);
    wire.resize(prefix.len() + chunks::DATA_BYTES, b'x');
    wire.extend_from_slice(b"\r\n0\r\n\r\n");

    let (result, allocs) = Tracker::measure(|| {
        chunk::Framer::new().decode(
            &wire,
            &Limit::with_body(chunks::DATA_BYTES),
            chunks::DATA_BYTES,
        )
    });
    assert_eq!(allocs, [0; 4]);
    let (next, consumed, bytes, boundary) = match result.expect("bounded chunk data") {
        (
            next,
            chunk::Fragment::Data {
                consumed,
                bytes,
                boundary,
            },
        ) => Some((next, consumed, bytes, boundary)),
        _ => None,
    }
    .expect("bounded chunk data");
    assert_eq!(consumed.get(), prefix.len() + chunks::DATA_BYTES);
    assert_eq!(next.body_bytes(), chunks::DATA_BYTES);
    assert_eq!(
        bytes,
        &wire[prefix.len()..prefix.len() + chunks::DATA_BYTES]
    );
    assert_eq!(bytes.as_ptr(), wire[prefix.len()..].as_ptr());
    assert!((boundary) == (chunk::Boundary::End));
}

#[test]
fn zero_keeps_payload() {
    let wire = b"1\r\nx\r\n0\r\n\r\n";
    let limits = Limit::with_body(1);
    let framer = match chunk::Framer::new()
        .decode(wire, &limits, 0)
        .expect("zero data budget may consume only framing")
    {
        (framer, chunk::Fragment::More(3)) => Some(framer),
        _ => None,
    }
    .expect("zero data budget may consume only framing");
    let tail = &wire[3..];
    let paused = match framer
        .decode(tail, &limits, 0)
        .expect("paused data must stay pending")
    {
        (paused, chunk::Fragment::More(0)) => Some(paused),
        _ => None,
    }
    .expect("paused data must stay pending");
    assert!((paused) == (framer));
    let (consumed, bytes, boundary) = match paused
        .decode(tail, &limits, 1)
        .expect("restored budget must resume the same payload")
    {
        (
            _,
            chunk::Fragment::Data {
                consumed,
                bytes,
                boundary,
            },
        ) => Some((consumed, bytes, boundary)),
        _ => None,
    }
    .expect("restored budget must resume the same payload");
    assert_eq!(consumed.get(), 1);
    assert_eq!(bytes, b"x");
    assert_eq!(bytes.as_ptr(), tail.as_ptr());
    assert!((boundary) == (chunk::Boundary::End));
}

#[test]
fn scan_keeps_prefix() {
    let wire = b"5\r\nhello\r\n0\r\nX: y\r\n\r\n";
    let mut pipeline = wire.to_vec();
    pipeline.extend_from_slice(b"NEXT");
    let limits = Limit::with_body(5);
    for split in 0..=wire.len() {
        let mut framer = chunk::Framer::new();
        let mut total = 0;
        for part in [&pipeline[..split], &pipeline[split..]] {
            let (result, allocs) = Tracker::measure(|| framer.scan(part, &limits));
            assert_eq!(allocs, [0; 4]);
            let run = (result).expect("valid framing split {split}");
            let prefix = run.consume(|prefix| prefix);
            assert_eq!(prefix.as_ptr(), part.as_ptr());
            assert_eq!(prefix, &wire[total..total + prefix.len()]);
            total += prefix.len();
        }
        assert_eq!(total, wire.len());
        assert_eq!(framer.body_bytes(), 5);
        let run = (framer.scan(b"", &limits)).expect("completed scan");
        assert!(run.resolve(|| false, || true), "wire must be complete");
    }
    let mut framer = chunk::Framer::new();
    let before = framer;
    assert!(matches!(
        framer.scan(b"6\r\n", &limits),
        Err(chunk::Error::Limit {
            axis: chunk::Axis::Body,
            actual: 6,
            maximum: 5,
        })
    ));
    assert!(
        (framer) == (before),
        "rejected scan must not publish progress"
    );
}

#[test]
fn trailer_suffix() {
    let limits = Limit::with_body(0);
    let (framer, _) = chunk::Framer::new()
        .decode(chunks::TRAILER_PREFIX, &limits, usize::MAX)
        .expect("trailer prefix");
    let (_, event) = framer
        .decode(chunks::TRAILER_SUFFIX, &limits, usize::MAX)
        .expect("trailer suffix");
    assert!(
        matches!(event, chunk::Fragment::Complete { consumed, bytes }
        if consumed.get() == chunks::TRAILER_FIELD_SUFFIX.len() + 2
            && bytes == chunks::TRAILER_FIELD_SUFFIX
            && bytes.as_ptr() == chunks::TRAILER_SUFFIX.as_ptr())
    );
}

#[test]
fn split_last_lf() {
    let limits = Limit::with_body(0);
    let prefix = b"0\r\nx-proof: yes\r\n\r";
    let (framer, event) = chunk::Framer::new()
        .decode(prefix, &limits, usize::MAX)
        .expect("valid trailer prefix");
    assert!(matches!(event, chunk::Fragment::Trailer(used) if used.get() == prefix.len()));

    let tail = b"\nGET /ping HTTP/1.1\r\nHost: local\r\n\r\n";
    let (result, allocs) = Tracker::measure(|| framer.decode(tail, &limits, usize::MAX));
    assert_eq!(allocs, [0; 4]);
    let (next, event) = result.expect("final LF completes the validated trailer");
    assert!(matches!(
        event,
        chunk::Fragment::Complete { consumed, bytes }
            if consumed.get() == 1 && bytes.is_empty() && bytes.as_ptr() == tail.as_ptr()
    ));
    assert_eq!(next.wire_bytes(), prefix.len() + 1);
    assert_eq!(next.body_bytes(), 0);
}

#[test]
fn one() {
    let data = b"5\r\nhello\r\n0\r\n\r\n";
    assert!(matches!(Decoder::body(data), Ok(Some(body)) if body.as_slice() == b"hello"));
}

#[test]
fn empty() {
    let data = b"0\r\n\r\n";
    assert!(matches!(Decoder::body(data), Ok(Some(body)) if body.is_empty()));
}

#[test]
fn completion_is_once() {
    let mut decoder = Decoder::with_limit(0);
    let (consumed, fields) = match decoder
        .try_take(b"0\r\n\r\n")
        .expect("complete empty body must emit trailers")
    {
        Event::Done { consumed, fields } => Some((consumed, fields)),
        _ => None,
    }
    .expect("complete empty body must emit trailers");
    assert_eq!(consumed.get(), 5);
    assert!(fields.section().is_empty());
    assert!(matches!(
        decoder.try_take(b"next"),
        Ok(Event::NeedMore(None))
    ));
}

#[test]
fn many() {
    let data = b"3\r\nabc\r\n4\r\ndefg\r\n0\r\n\r\n";
    assert!(matches!(Decoder::body(data), Ok(Some(body)) if body.as_slice() == b"abcdefg"));
    assert!(matches!(
        Tracker::fail_after(0, || Decoder::body(chunks::ALLOCATION)),
        Err(Error::Allocation(o3::alloc::Error::Exhausted))
    ));
}

#[test]
fn partial() {
    let data = b"5\r\nhel";
    assert!(matches!(Decoder::body(data), Ok(None)));
}

#[test]
fn segments() {
    let mut decoder = Decoder::with_limit(16);
    assert!(matches!(
        decoder.try_take(b"10\r\nabc"),
        Ok(Event::Chunk { consumed, bytes: b"abc", boundary: chunk::Boundary::More })
            if consumed.get() == 7
    ));
    assert!(matches!(
        decoder.try_take(b"defghijklmnop"),
        Ok(Event::Chunk { consumed, bytes: b"defghijklmnop", boundary: chunk::Boundary::End })
            if consumed.get() == 13
    ));
    assert!(matches!(
        decoder.try_take(b"\r\n0\r\n\r\n"),
        Ok(Event::Done { consumed, fields }) if consumed.get() == 7 && fields.section().is_empty()
    ));
}

#[test]
fn every_split() {
    let wire = b"3;foo=bar\r\nabc\r\n4\r\ndefg\r\n0\r\nx-proof: yes\r\n\r\n";

    for split in 0..=wire.len() {
        let mut decoder = Decoder::with_limit(7);
        let mut cursor = 0usize;
        let mut body = Vec::new();
        let mut pending = Vec::new();
        let mut done = false;
        for part in [&wire[..split], &wire[split..]] {
            pending.extend_from_slice(part);
            while !pending.is_empty() {
                let event = (decoder.try_take(&pending)).expect("valid trailer split {split}");
                let used = match event {
                    Event::NeedMore(None) => break,
                    Event::NeedMore(Some(consumed)) => consumed,
                    Event::Chunk {
                        consumed, bytes, ..
                    } => {
                        body.extend_from_slice(bytes);
                        consumed
                    }
                    Event::Done { consumed, fields } => {
                        assert_eq!(fields.section(), b"x-proof: yes\r\n");
                        done = true;
                        consumed
                    }
                };
                cursor += used.get();
                pending.drain(..used.get());
                if done {
                    break;
                }
            }
        }
        assert!(done, "decoder did not finish at split {split}");
        assert_eq!(cursor, wire.len(), "split {split}");
        assert_eq!(body, b"abcdefg", "split {split}");
    }
}

#[test]
fn invalid_hex() {
    let data = b"xyz\r\nhello\r\n0\r\n\r\n";
    assert!(Decoder::body(data).is_err());
}

#[test]
fn partial_end() {
    let data = b"5\r\nhello\r\n0\r\n";
    assert!(matches!(Decoder::body(data), Ok(None)));
}

#[test]
fn oversized() {
    let data = b"FFFFFFFF\r\nhello\r\n0\r\n\r\n";
    assert!(Decoder::body(data).is_err());
}

#[test]
fn hex_case() {
    for data in [
        b"a\r\n0123456789\r\n0\r\n\r\n".as_slice(),
        b"A\r\n0123456789\r\n0\r\n\r\n",
    ] {
        assert!(matches!(Decoder::body(data), Ok(Some(body)) if body.as_slice() == b"0123456789"));
    }
}

#[test]
fn extension() {
    let data = b"5;ext=val\r\nhello\r\n0\r\n\r\n";
    assert!(matches!(Decoder::body(data), Ok(Some(body)) if body.as_slice() == b"hello"));
}

#[test]
fn extension_error_offsets() {
    let limits = Limit::with_body(1);
    for (wire, syntax) in chunks::SYNTAX_ERRORS {
        let offset = wire.len() - 1;
        let (framer, progress) = chunk::Framer::new()
            .decode(&wire[..offset], &limits, usize::MAX)
            .expect("extension prefix");
        assert!(matches!(progress, chunk::Fragment::More(used) if used == offset));
        assert_eq!(
            framer.decode(&wire[offset..], &limits, usize::MAX).err(),
            Some(chunk::Error::Syntax {
                offset,
                syntax,
                actual: wire[offset],
            })
        );
    }
}

#[test]
fn bad_trailer() {
    let data = b"5\r\nhello\r\n0\r\nmissing-colon\r\n\r\n";
    assert!(Decoder::body(data).is_err());
}

#[test]
fn crlf_payload() {
    let data = b"6\r\nab\r\ncd\r\n0\r\n\r\n";
    assert!(matches!(Decoder::body(data), Ok(Some(body)) if body.as_slice() == b"ab\r\ncd"));
}
