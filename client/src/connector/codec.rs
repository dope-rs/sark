use dope::manifold::connector;
use o3::buffer;
use sark_core::http::HeaderList;
use sark_core::http::codec::chunked::{BodyDecoder, DecodeEvent};
use sark_core::http::codec::{BodyKind, DecodeMode, ResponseDecoder};

use super::response::{ResponseEvent, ResponseHead};

const DEFAULT_MAX_RESPONSE_BODY: usize = 16 * 1024 * 1024;
pub(super) const STREAM_CHUNK_SIZE: usize = 16 * 1024;
pub(super) const MAX_INFORMATIONAL_RESPONSES: usize = 16;

pub struct Emission {
    pub event: Result<ResponseEvent, String>,
    pub bytes: usize,
}

pub struct Head {
    pub first: Option<Emission>,
    pub second: Option<Emission>,
    pub complete: bool,
}

impl Head {
    fn event(event: ResponseEvent, bytes: usize, complete: bool) -> Self {
        Self {
            first: Some(Emission {
                event: Ok(event),
                bytes,
            }),
            second: None,
            complete,
        }
    }

    fn silent(complete: bool) -> Self {
        Self {
            first: None,
            second: None,
            complete,
        }
    }

    fn error(reason: String) -> Self {
        Self {
            first: Some(Emission {
                event: Err(reason),
                bytes: 0,
            }),
            second: None,
            complete: true,
        }
    }
}

enum Framing {
    Sized {
        remaining: usize,
    },
    Chunked {
        decoder: BodyDecoder,
        pending: Vec<u8>,
    },
    UntilEof {
        received: usize,
    },
}

#[derive(Default)]
pub struct ParseState {
    framing: Option<Framing>,
    informational: usize,
    failed: bool,
}

pub struct Codec {
    pub max_response_body: usize,
}

impl Default for Codec {
    fn default() -> Self {
        Self {
            max_response_body: DEFAULT_MAX_RESPONSE_BODY,
        }
    }
}

impl Codec {
    fn fail(state: &mut ParseState, reason: impl Into<String>) -> Head {
        state.framing = None;
        state.failed = true;
        Head::error(reason.into())
    }

    fn parse_head(&self, state: &mut ParseState, bytes: &[u8]) -> Option<(Head, usize)> {
        let decoded = match ResponseDecoder::new(DecodeMode::Response).head(bytes) {
            Ok(Some(head)) => head,
            Ok(None) => return None,
            Err(error) => {
                return Some((Self::fail(state, error.to_string()), bytes.len().max(1)));
            }
        };
        let header_len = decoded.header_len;
        let informational = decoded.status.is_informational()
            && decoded.status != http::StatusCode::SWITCHING_PROTOCOLS;
        if informational {
            state.informational += 1;
            if state.informational > MAX_INFORMATIONAL_RESPONSES {
                return Some((
                    Self::fail(state, "too many informational responses"),
                    header_len,
                ));
            }
            let response = decoded.into_response(buffer::Shared::new(), []);
            return Some((
                Head::event(
                    ResponseEvent::Informational(ResponseHead::new(response)),
                    header_len,
                    false,
                ),
                header_len,
            ));
        }
        state.informational = 0;

        let complete = match decoded.body_kind {
            BodyKind::NoBody | BodyKind::ContentLength(0) => true,
            BodyKind::ContentLength(len) if len > self.max_response_body => {
                return Some((
                    Self::fail(state, "response body exceeds size limit"),
                    header_len,
                ));
            }
            BodyKind::ContentLength(len) => {
                state.framing = Some(Framing::Sized { remaining: len });
                false
            }
            BodyKind::Chunked => {
                state.framing = Some(Framing::Chunked {
                    decoder: BodyDecoder::with_limit(self.max_response_body),
                    pending: Vec::new(),
                });
                false
            }
            BodyKind::UntilEof => {
                state.framing = Some(Framing::UntilEof { received: 0 });
                false
            }
        };
        let response = decoded.into_response(buffer::Shared::new(), []);
        Some((
            Head::event(
                ResponseEvent::Head(ResponseHead::new(response)),
                header_len,
                complete,
            ),
            header_len,
        ))
    }

    fn parse_sized(
        &self,
        state: &mut ParseState,
        buf: &buffer::Shared,
        remaining: usize,
    ) -> Option<(Head, usize)> {
        let available = buf.len();
        if available == 0 || (available < STREAM_CHUNK_SIZE && available < remaining) {
            state.framing = Some(Framing::Sized { remaining });
            return None;
        }
        let take = available.min(remaining);
        let data = buf
            .get(..take)
            .expect("sized response slice must be in bounds");
        let complete = take == remaining;
        if complete {
            state.framing = None;
        } else {
            state.framing = Some(Framing::Sized {
                remaining: remaining - take,
            });
        }
        Some((Head::event(ResponseEvent::Data(data), take, complete), take))
    }

    fn parse_until_eof(
        &self,
        state: &mut ParseState,
        buf: &buffer::Shared,
        received: usize,
    ) -> Option<(Head, usize)> {
        let available = buf.len();
        if received
            .checked_add(available)
            .is_none_or(|total| total > self.max_response_body)
        {
            return Some((
                Self::fail(state, "response body exceeds size limit"),
                available.max(1),
            ));
        }
        if available < STREAM_CHUNK_SIZE {
            state.framing = Some(Framing::UntilEof { received });
            return None;
        }
        let data = buf.clone();
        state.framing = Some(Framing::UntilEof {
            received: received + available,
        });
        Some((
            Head::event(ResponseEvent::Data(data), available, false),
            available,
        ))
    }

    fn parse_chunked(
        &self,
        state: &mut ParseState,
        buf: &buffer::Shared,
        mut decoder: BodyDecoder,
        mut pending: Vec<u8>,
    ) -> Option<(Head, usize)> {
        let bytes = buf.as_ref();
        let (consumed, event) = match decoder.decode(bytes) {
            Ok(pair) => pair,
            Err(error) => {
                let reason = if matches!(error, sark_core::error::Error::PayloadTooLarge(_)) {
                    "response body exceeds size limit".to_owned()
                } else {
                    error.to_string()
                };
                return Some((Self::fail(state, reason), bytes.len().max(1)));
            }
        };

        match event {
            DecodeEvent::NeedMore => {
                state.framing = Some(Framing::Chunked { decoder, pending });
                (consumed != 0).then(|| (Head::silent(false), consumed))
            }
            DecodeEvent::Chunk(chunk) => {
                let retained_chunk = || {
                    let start = chunk.as_ptr() as usize - bytes.as_ptr() as usize;
                    buf.get(start..start + chunk.len())
                        .expect("chunk slice must belong to the response snapshot")
                };
                let (first, second) = if pending.is_empty() && chunk.len() >= STREAM_CHUNK_SIZE {
                    let data = retained_chunk();
                    (
                        Some(Emission {
                            bytes: data.len(),
                            event: Ok(ResponseEvent::Data(data)),
                        }),
                        None,
                    )
                } else if !pending.is_empty() && chunk.len() >= STREAM_CHUNK_SIZE {
                    let prefix = buffer::Shared::from(std::mem::take(&mut pending));
                    let data = retained_chunk();
                    (
                        Some(Emission {
                            bytes: prefix.len(),
                            event: Ok(ResponseEvent::Data(prefix)),
                        }),
                        Some(Emission {
                            bytes: data.len(),
                            event: Ok(ResponseEvent::Data(data)),
                        }),
                    )
                } else {
                    if pending.is_empty() {
                        pending.reserve_exact(STREAM_CHUNK_SIZE);
                    }
                    pending.extend_from_slice(chunk);
                    let emission = if pending.len() >= STREAM_CHUNK_SIZE {
                        let data = buffer::Shared::from(std::mem::take(&mut pending));
                        Some(Emission {
                            bytes: data.len(),
                            event: Ok(ResponseEvent::Data(data)),
                        })
                    } else {
                        None
                    };
                    (emission, None)
                };
                state.framing = Some(Framing::Chunked { decoder, pending });
                Some((
                    Head {
                        first,
                        second,
                        complete: false,
                    },
                    consumed,
                ))
            }
            DecodeEvent::Done(trailers) => {
                state.framing = None;
                let mut trailer_fields = HeaderList::new();
                trailer_fields.extend_trailers(trailers);
                let trailer_bytes = trailer_fields.wire_len();
                let data = (!pending.is_empty()).then(|| {
                    let data = buffer::Shared::from(pending);
                    Emission {
                        bytes: data.len(),
                        event: Ok(ResponseEvent::Data(data)),
                    }
                });
                let trailer = (!trailer_fields.is_empty()).then_some(Emission {
                    bytes: trailer_bytes,
                    event: Ok(ResponseEvent::Trailers(trailer_fields)),
                });
                let (first, second) = match (data, trailer) {
                    (Some(data), trailer) => (Some(data), trailer),
                    (None, trailer) => (trailer, None),
                };
                Some((
                    Head {
                        first,
                        second,
                        complete: true,
                    },
                    consumed,
                ))
            }
        }
    }
}

impl connector::codec::Codec for Codec {
    type Head = Head;
    type ParseState = ParseState;

    fn parse(&self, state: &mut ParseState, buf: &buffer::Shared) -> Option<(Head, usize)> {
        if state.failed {
            return None;
        }
        let Some(framing) = state.framing.take() else {
            return self.parse_head(state, buf.as_ref());
        };
        match framing {
            Framing::Sized { remaining } => self.parse_sized(state, buf, remaining),
            Framing::Chunked { decoder, pending } => {
                self.parse_chunked(state, buf, decoder, pending)
            }
            Framing::UntilEof { received } => self.parse_until_eof(state, buf, received),
        }
    }

    fn finish(&self, state: &mut ParseState, remaining: buffer::Shared) -> Option<Head> {
        if state.failed {
            return None;
        }
        match state.framing.take() {
            Some(Framing::UntilEof { received }) => {
                if received
                    .checked_add(remaining.len())
                    .is_none_or(|total| total > self.max_response_body)
                {
                    return Some(Self::fail(state, "response body exceeds size limit"));
                }
                if remaining.is_empty() {
                    Some(Head::silent(true))
                } else {
                    let bytes = remaining.len();
                    Some(Head::event(ResponseEvent::Data(remaining), bytes, true))
                }
            }
            Some(Framing::Sized { .. }) => Some(Self::fail(state, "incomplete HTTP response body")),
            Some(Framing::Chunked { .. }) => Some(Self::fail(state, "incomplete chunked response")),
            None if remaining.is_empty() => None,
            None => Some(Self::fail(state, "incomplete HTTP response head")),
        }
    }
}

#[cfg(test)]
mod tests {
    use dope::manifold::connector::codec::Codec as _;
    use o3::buffer::Shared;

    use super::{Codec, ParseState, ResponseEvent, STREAM_CHUNK_SIZE};

    #[test]
    fn content_length_data_retains_the_ingress_owner() {
        let response = Shared::from(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello".to_vec());
        let codec = Codec::default();
        let mut state = ParseState::default();
        let (_, head_len) = codec.parse(&mut state, &response).expect("response head");
        let body = response.get(head_len..).expect("response body");
        let (event, consumed) = codec.parse(&mut state, &body).expect("body event");
        assert_eq!(consumed, 5);
        let data = match event.first.expect("data event").event.unwrap() {
            ResponseEvent::Data(data) => data,
            other => panic!("expected data event, got {other:?}"),
        };
        assert_eq!(data.as_slice(), b"hello");
        assert_eq!(data.as_slice().as_ptr(), body.as_slice().as_ptr());
    }

    #[test]
    fn large_chunked_data_retains_the_ingress_owner() {
        let size = format!("{STREAM_CHUNK_SIZE:x}\r\n");
        let mut wire = Vec::with_capacity(size.len() + STREAM_CHUNK_SIZE + 2);
        wire.extend_from_slice(size.as_bytes());
        wire.resize(wire.len() + STREAM_CHUNK_SIZE, b'x');
        wire.extend_from_slice(b"\r\n");
        let wire = Shared::from(wire);

        let codec = Codec::default();
        let mut state = ParseState::default();
        let head = Shared::from_static(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
        let _ = codec.parse(&mut state, &head).expect("response head");
        let (event, _) = codec.parse(&mut state, &wire).expect("chunk event");
        let data = match event.first.expect("data event").event.unwrap() {
            ResponseEvent::Data(data) => data,
            other => panic!("expected data event, got {other:?}"),
        };
        assert_eq!(data.len(), STREAM_CHUNK_SIZE);
        assert_eq!(
            data.as_slice().as_ptr(),
            wire.as_slice()[size.len()..].as_ptr()
        );
    }
}
