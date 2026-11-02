pub mod common;

use common::codec::{
    Block, Scope as _,
    decoder::{self, Decoder},
};
use dope::manifold::client::session::{
    codec::{self, Stateful as _, storage::Slot},
    response::Direct,
};
use o3::buffer::{resident, storage::Shared};
use sark_client::protocol::{
    Error,
    codec::{
        Codec, Limits,
        output::{self, Completion, Preview},
    },
    response::Event,
};
use sark_http::payload::response::Method;

#[test]
fn sized_retry_keeps_head() {
    resident::Domain::try_scope(256, |budget| {
        let codec = Codec::default();
        let decoder = Decoder::try_new(&codec).expect("staging");
        let mut state = codec.state(budget);
        let wire = Shared::try_copy(
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nxHTTP/1.1 204 No Content\r\n\r\n",
        ).expect("wire");
        let mut stored = decoder.storage(&wire, &state);
        drop(decoder.try_stored(&mut state, &mut stored, &budget).expect("head"));
        let before = stored.source().as_ref().len();
        let mut body = None;
        for attempt in 0..3 {
            let target = &mut body;
            let step = codec.decode::<_, Direct, _>(
                &mut stored, &mut state, &budget, decoder.limit(), |preview| {
                    let (data, end) = match preview.expect("body") {
                        Preview::Body { data, completion: end, .. } => Some((data, end)),
                        Preview::Head { .. } => None,
                    }.expect("body preview");
                    assert_eq!(data, b"x");
                    assert!(end == Completion::Complete);
                    if attempt < 2 {
                        codec::Admission::RetryInput
                    } else {
                        codec::Admission::Ready(move |parsed: Result<_, Error>, state: &mut _| {
                            *target = Some(codec.admit(state, parsed.expect("body"), || None));
                        })
                    }
                },
            );
            if attempt < 2 {
                assert!(!step.has_consumed());
                assert_eq!(stored.source().as_ref().len(), before);
                assert_eq!(budget.resident(), 0);
            } else {
                assert!(step.has_consumed());
                assert_eq!(before - stored.source().as_ref().len(), 1);
            }
        }
        assert!(matches!(body.expect("redelivery").into_events().next(),
            Some(Ok(Event::Data(data))) if data.as_slice() == b"x"));
        let (next, _) = decoder.try_stored(&mut state, &mut stored, &budget)
            .expect("next head").expect("next response");
        assert!(next.completion() == Completion::Complete);
        assert!(matches!(next.into_events().next(), Some(Ok(Event::Head(head))) if head.status().as_u16() == 204));
    }).expect("domain");
}

#[test]
fn retry_keeps_info_count() {
    resident::Domain::try_scope(256, |budget| {
        let codec = Codec::default();
        let decoder = Decoder::try_new(&codec).expect("staging");
        let mut state = codec.state(budget);
        let wire = Shared::try_copy(b"HTTP/1.1 103 Early Hints\r\nLink: </x>\r\n\r\n").expect("wire");
        let mut stored = decoder.storage(&wire, &state);
        for attempt in 0..=output::INFORMATIONAL_LIMIT + 1 {
            let step = codec.decode::<_, Direct, _>(
                &mut stored, &mut state, &budget, decoder.limit(), |preview| {
                    assert!(matches!(preview, Ok(Preview::Head { parts, .. }) if parts.status().as_u16() == 103));
                    if attempt <= output::INFORMATIONAL_LIMIT {
                        codec::Admission::RetryInput
                    } else {
                        codec::Admission::Ready(move |parsed: Result<_, Error>, state: &mut _| {
                            let head = codec.admit(state, parsed.expect("head"), || Some(Method::Other));
                            assert!(matches!(head.into_events().next(), Some(Ok(Event::Informational(_)))));
                        })
                    }
                },
            );
            assert_eq!(step.has_consumed(), attempt > output::INFORMATIONAL_LIMIT);
        }
        for _ in 1..output::INFORMATIONAL_LIMIT {
            let (head, _) = decoder.try_parse(&mut state, &wire, &budget)
                .expect("admission").expect("informational response");
            assert!(matches!(head.into_events().next(), Some(Ok(Event::Informational(_)))));
        }
        let (head, _) = decoder.try_parse(&mut state, &wire, &budget)
            .expect("framing result").expect("limit result");
        assert!(matches!(head.into_events().next(), Some(Err(Error::InformationalLimit))));
    }).expect("domain");
}

#[test]
fn chunks_keep_pending() {
    resident::Domain::try_scope(output::DATA_LIMIT * 2, |budget| {
        let codec = Codec::default();
        let decoder = Decoder::try_new(&codec).expect("staging");
        let head = Shared::try_copy(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n").expect("head");
        let data = Shared::try_copy(b"3\r\nabc").expect("data");
        for (split, trailer) in [(false, false), (false, true), (true, true)] {
            let mut state = codec.state(budget);
            drop(decoder.try_parse(&mut state, &head, &budget).expect("head"));
            let mut stored = decoder.storage(&data, &state);
            for attempt in 0..2 {
                let step = codec.decode::<_, Direct, _>(
                    &mut stored, &mut state, &budget, decoder.limit(), |preview| {
                        let (pending, data) = match preview.expect("body") {
                            Preview::Body { pending, data, .. } => Some((pending, data)),
                            Preview::Head { .. } => None,
                        }.expect("body preview");
                        assert!(pending.is_empty());
                        assert_eq!(data, b"abc");
                        if attempt == 0 {
                            codec::Admission::RetryInput
                        } else {
                            codec::Admission::Ready(move |parsed: Result<_, Error>, state: &mut _| {
                                let output = codec.admit(state, parsed.expect("body"), || None);
                                assert!(output.into_events().next().is_none());
                            })
                        }
                    },
                );
                assert_eq!(step.has_consumed(), attempt != 0);
                if attempt == 0 {
                    assert_eq!(budget.resident(), 0);
                    assert_eq!(stored.source().as_ref(), data.as_slice());
                }
            }
            if split {
                let prefix = Shared::try_copy(b"\r\n0\r\nX: y\r\n\r").expect("trailer prefix");
                let mut stored = decoder.storage(&prefix, &state);
                Block::try_new(budget).expect("budget blocker").with(|| {
                    let step = codec.decode::<_, Direct, _>(
                        &mut stored, &mut state, &budget, decoder.limit(), |preview| match preview {
                            Err(error) => {
                                assert!(matches!(error, Error::Retention(_)));
                                codec::Admission::RetryInput
                            }
                            Ok(_) => codec::Admission::Ready(|parsed: Result<_, Error>, _: &mut _| assert!(parsed.is_ok())),
                        },
                    );
                    assert!(!step.has_consumed());
                    assert_eq!(stored.source().as_ref(), prefix.as_slice());
                });
                assert!(decoder.try_stored(&mut state, &mut stored, &budget)
                    .expect("trailer prefix retry").is_none());
                assert!(stored.source().as_ref().is_empty());
            }
            let tail = Shared::try_copy(if split {
                b"\n"
            } else if trailer {
                b"\r\n0\r\nX: y\r\n\r\n"
            } else {
                b"\r\n0\r\n\r\n"
            }).expect("tail");
            let mut stored = decoder.storage(&tail, &state);
            let before = budget.resident();
            let mut output = None;
            for attempt in 0..3 {
                let target = &mut output;
                let step = codec.decode::<_, Direct, _>(
                    &mut stored, &mut state, &budget, decoder.limit(), |preview| {
                        let (pending, data, fields, end) = match preview.expect("completion") {
                            Preview::Body { pending, data, trailers, completion: end } => Some((pending, data, trailers, end)),
                            Preview::Head { .. } => None,
                        }.expect("body preview");
                        assert_eq!(pending, b"abc");
                        assert!(data.is_empty());
                        assert!(end == Completion::Complete);
                        if trailer {
                            assert_eq!(fields.expect("trailer fields").concat(), b"X: y\r\n");
                        } else {
                            assert!(fields.is_none());
                        }
                        if attempt < 2 {
                            codec::Admission::RetryInput
                        } else {
                            codec::Admission::Ready(move |parsed: Result<_, Error>, state: &mut _| {
                                *target = Some(codec.admit(state, parsed.expect("completion"), || None));
                            })
                        }
                    },
                );
                if attempt < 2 {
                    assert!(!step.has_consumed());
                    assert_eq!(stored.source().as_ref(), tail.as_slice());
                    assert_eq!(budget.resident(), before);
                } else {
                    assert!(step.has_consumed());
                }
            }
            let output = output.expect("completed body");
            assert!(output.completion() == Completion::Complete);
            let mut events = output.into_events();
            assert!(matches!(events.next(), Some(Ok(Event::Data(data))) if data.as_slice() == b"abc"));
            if trailer {
                assert!(matches!(events.next(), Some(Ok(Event::Trailers(fields))) if fields.get("X") == Some(b"y".as_slice())));
            }
            assert!(events.next().is_none());
        }
    }).expect("domain");
}

#[test]
fn full_chunk_keeps_owner() {
    resident::Domain::try_scope(output::DATA_LIMIT * 2, |budget| {
        let codec = Codec::default();
        let decoder = Decoder::try_new(&codec).expect("staging");
        let mut state = codec.state(budget);
        let head = Shared::try_copy(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .expect("head");
        drop(decoder.try_parse(&mut state, &head, &budget).expect("head"));
        let prefix = Shared::try_copy(b"3\r\nabc").expect("pending data");
        drop(
            decoder
                .try_parse(&mut state, &prefix, &budget)
                .expect("pending"),
        );
        let mut wire = Vec::from(b"\r\n4000\r\n".as_slice());
        wire.resize(wire.len() + output::DATA_LIMIT, b'x');
        let wire = Shared::try_from(&mut wire).expect("full chunk");
        let mut stored = decoder.storage(&wire, &state);
        let resident = budget.resident();
        let mut output = None;
        for attempt in 0..2 {
            let target = &mut output;
            let step = codec.decode::<_, Direct, _>(
                &mut stored,
                &mut state,
                &budget,
                decoder.limit(),
                |preview| {
                    let (pending, data) = match preview.expect("chunk") {
                        Preview::Body { pending, data, .. } => Some((pending, data)),
                        Preview::Head { .. } => None,
                    }
                    .expect("body preview");
                    assert_eq!(pending, b"abc");
                    assert_eq!(data.len(), output::DATA_LIMIT);
                    if attempt == 0 {
                        codec::Admission::RetryInput
                    } else {
                        codec::Admission::Ready(move |parsed: Result<_, Error>, state: &mut _| {
                            *target = Some(codec.admit(state, parsed.expect("chunk"), || None));
                        })
                    }
                },
            );
            if attempt == 0 {
                assert!(!step.has_consumed());
                assert_eq!(budget.resident(), resident);
                assert_eq!(stored.source().as_ref(), wire.as_slice());
            } else {
                assert!(step.has_consumed());
            }
        }
        let mut events = output.expect("full body items").into_events();
        let first = match events.next() {
            Some(Ok(Event::Data(first))) => Some(first),
            _ => None,
        }
        .expect("coalesced data");
        assert_eq!(first.len(), output::DATA_LIMIT);
        assert_eq!(&first.as_slice()[..3], b"abc");
        assert!(first.as_slice()[3..].iter().all(|byte| *byte == b'x'));
        assert!(matches!(events.next(), Some(Ok(Event::Data(last))) if last.as_slice() == b"xxx"));
        assert!(events.next().is_none());
    })
    .expect("domain");
}

#[test]
fn eof_retry_keeps_framing() {
    resident::Domain::try_scope(256, |budget| {
        let codec = Codec::default();
        let decoder = Decoder::try_new(&codec).expect("staging");
        let head = Shared::try_copy(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n").expect("head");
        for bytes in [b"x".as_slice(), b""] {
            let mut state = codec.state(budget);
            drop(decoder.try_parse(&mut state, &head, &budget).expect("head"));
            let tail = Shared::try_copy(bytes).expect("tail");
            let mut stored = decoder.storage(&tail, &state);
            let mut output = None;
            for attempt in 0..3 {
                let target = &mut output;
                let (owners, finish) = Slot::new((stored, state)).run(|input| {
                    codec.finish::<_, Direct, _>(input, &budget, decoder.limit(), |preview| {
                        let (data, end) = match preview.expect("EOF") {
                            Preview::Body {
                                data,
                                completion: end,
                                ..
                            } => Some((data, end)),
                            Preview::Head { .. } => None,
                        }
                        .expect("body preview");
                        assert_eq!(data, bytes);
                        assert!(end == Completion::Complete);
                        if attempt < 2 {
                            codec::Admission::RetryInput
                        } else {
                            codec::Admission::Ready(
                                move |parsed: Result<_, Error>, state: &mut _| {
                                    *target =
                                        Some(codec.admit(state, parsed.expect("EOF"), || None));
                                },
                            )
                        }
                    })
                });
                (stored, state) = owners.into_parts();
                if attempt < 2 {
                    assert!(matches!(
                        finish,
                        Ok(codec::Finish::Item(Direct::RetryInput))
                    ));
                    assert_eq!(stored.source().as_ref(), bytes);
                } else {
                    assert!(matches!(finish, Ok(codec::Finish::Item(Direct::Committed))));
                }
            }
            let output = output.expect("EOF completion");
            assert!(output.completion() == Completion::Complete);
            let mut events = output.into_events();
            if !bytes.is_empty() {
                assert!(
                    matches!(events.next(), Some(Ok(Event::Data(data))) if data.as_slice() == bytes)
                );
            }
            assert!(events.next().is_none());
        }
    })
    .expect("domain");
}

#[test]
fn head_error_survives_eof() {
    resident::Domain::try_scope(256, |budget| {
        let codec = Codec::new(Limits::new(16, 256));
        let decoder = Decoder::try_new(&codec).expect("staging");
        let mut state = codec.state(budget);
        let wire = Shared::try_copy(b"HTTP/1.1 200 OK\r\nX: y\r\n\r\n").expect("wire");
        let mut stored = decoder.storage(&wire, &state);
        let step = codec.decode::<_, Direct, _>(
            &mut stored,
            &mut state,
            &budget,
            decoder.limit(),
            |preview| match preview {
                Err(error) => {
                    assert!(matches!(error, Error::HeadLimit(_)));
                    codec::Admission::RetryInput
                }
                Ok(_) => codec::Admission::Ready(|parsed: Result<_, Error>, _: &mut _| {
                    assert!(parsed.is_ok())
                }),
            },
        );
        assert!(!step.has_consumed());
        let (owners, finish) = Slot::new((stored, state)).run(|input| {
            codec.finish::<_, Direct, _>(input, &budget, decoder.limit(), |preview| match preview {
                Err(error) => {
                    assert!(matches!(error, Error::HeadLimit(_)));
                    codec::Admission::RetryInput
                }
                Ok(_) => codec::Admission::Ready(|parsed: Result<_, Error>, _: &mut _| {
                    assert!(parsed.is_ok())
                }),
            })
        });
        let (stored, state) = owners.into_parts();
        assert!(matches!(finish, Err(Direct::RetryInput)));
        assert_eq!(stored.source().as_ref(), wire.as_slice());
        let (_owners, finish) = decoder.try_finish(state, stored, &budget);
        assert!(matches!(
            finish,
            Err(decoder::Error::Protocol(Error::HeadLimit(_)))
        ));
    })
    .expect("domain");
}
