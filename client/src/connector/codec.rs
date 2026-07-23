use dope::manifold::connector;
use o3::buffer;
use sark_core::http::Response;
use sark_core::http::codec::chunked::{BodyDecoder, DecodeEvent};
use sark_core::http::codec::{BodyKind, DecodeMode, DecodedHead, ResponseDecoder};

const DEFAULT_MAX_RESPONSE_BODY: usize = 16 * 1024 * 1024;

pub struct Head {
    pub response: Result<Response, String>,
    pub buffered: usize,
}

enum Framing {
    Sized {
        head: DecodedHead,
        total: usize,
    },
    Chunked {
        head: DecodedHead,
        decoder: BodyDecoder,
        scanned: usize,
        body: Vec<u8>,
    },
}

#[derive(Default)]
pub struct ParseState {
    framing: Option<Framing>,
    pub error: bool,
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

impl connector::Codec for Codec {
    type Head = Head;
    type ParseState = ParseState;

    fn parse(&self, state: &mut ParseState, buf: &buffer::Shared) -> Option<(Head, usize)> {
        let bytes = buf.as_ref();
        if state.framing.is_none() {
            let head = match ResponseDecoder::new(DecodeMode::Response).head(bytes) {
                Ok(Some(h)) => h,
                Ok(None) => return None,
                Err(error) => {
                    state.error = true;
                    return Some((
                        Head {
                            response: Err(error.to_string()),
                            buffered: bytes.len(),
                        },
                        bytes.len(),
                    ));
                }
            };
            state.framing = Some(match head.body_kind {
                BodyKind::NoBody => Framing::Sized {
                    total: head.header_len,
                    head,
                },
                BodyKind::ContentLength(n) => {
                    let total = match head.header_len.checked_add(n) {
                        Some(total) => total,
                        None => {
                            state.error = true;
                            return Some((
                                Head {
                                    response: Err("response body size overflows usize".to_owned()),
                                    buffered: head.header_len,
                                },
                                bytes.len(),
                            ));
                        }
                    };
                    Framing::Sized { head, total }
                }
                BodyKind::Chunked => Framing::Chunked {
                    head,
                    decoder: BodyDecoder::with_limit(self.max_response_body),
                    scanned: 0,
                    body: Vec::new(),
                },
                BodyKind::UntilEof => {
                    state.error = true;
                    return Some((
                        Head {
                            response: Err("EOF-delimited response body is not supported".to_owned()),
                            buffered: head.header_len,
                        },
                        bytes.len(),
                    ));
                }
            });
        }

        match state.framing.as_mut()? {
            Framing::Sized { head, total } => {
                let head_len = head.header_len;
                let total = *total;
                let body_len = total.saturating_sub(head_len);
                if body_len > self.max_response_body {
                    state.framing = None;
                    state.error = true;
                    return Some((
                        Head {
                            response: Err("response body exceeds size limit".to_owned()),
                            buffered: head_len,
                        },
                        bytes.len(),
                    ));
                }
                if bytes.len() < total {
                    return None;
                }
                let Framing::Sized { head, .. } =
                    state.framing.take().expect("sized response framing")
                else {
                    unreachable!("sized response framing changed")
                };
                let body = buf.slice(head_len..total);
                Some((
                    Head {
                        response: Ok(head.into_response(body, [])),
                        buffered: total,
                    },
                    total,
                ))
            }
            Framing::Chunked {
                head,
                decoder,
                scanned,
                body,
            } => {
                let head_len = head.header_len;
                loop {
                    let input = &bytes[head_len + *scanned..];
                    let (consumed, event) = match decoder.decode(input) {
                        Ok(pair) => pair,
                        Err(error) => {
                            let reason =
                                if matches!(error, sark_core::error::Error::PayloadTooLarge(_)) {
                                    "response body exceeds size limit".to_owned()
                                } else {
                                    error.to_string()
                                };
                            state.framing = None;
                            state.error = true;
                            return Some((
                                Head {
                                    response: Err(reason),
                                    buffered: head_len,
                                },
                                bytes.len(),
                            ));
                        }
                    };
                    *scanned += consumed;
                    match event {
                        DecodeEvent::NeedMore => return None,
                        DecodeEvent::Chunk(chunk) => {
                            body.extend_from_slice(chunk);
                        }
                        DecodeEvent::Done(trailers) => {
                            let total = head_len + *scanned;
                            let Framing::Chunked { head, body, .. } =
                                state.framing.take().expect("chunked response framing")
                            else {
                                unreachable!("chunked response framing changed")
                            };
                            return Some((
                                Head {
                                    response: Ok(head.into_response(body, trailers)),
                                    buffered: total,
                                },
                                total,
                            ));
                        }
                    }
                }
            }
        }
    }
}
