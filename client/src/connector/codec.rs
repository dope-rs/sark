use dope::manifold::connector;
use o3::buffer;
use sark_core::http::codec::chunked::{BodyDecoder, DecodeEvent};
use sark_core::http::codec::{BodyKind, DecodeMode, ResponseDecoder};

const DEFAULT_MAX_RESPONSE_BODY: usize = 16 * 1024 * 1024;

pub struct Head {
    pub full: buffer::Shared,
    pub head_len: usize,
    pub error: Option<&'static str>,
}

enum Framing {
    Sized {
        head_len: usize,
        total: usize,
    },
    Chunked {
        head_len: usize,
        decoder: BodyDecoder,
        scanned: usize,
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
                Err(_) => return None,
            };
            state.framing = Some(match head.body_kind {
                BodyKind::NoBody => Framing::Sized {
                    head_len: head.header_len,
                    total: head.header_len,
                },
                BodyKind::ContentLength(n) => Framing::Sized {
                    head_len: head.header_len,
                    total: head.header_len + n,
                },
                BodyKind::Chunked => Framing::Chunked {
                    head_len: head.header_len,
                    decoder: BodyDecoder::new(),
                    scanned: 0,
                },
                BodyKind::UntilEof => {
                    state.error = true;
                    return Some((
                        Head {
                            full: buf.slice(0..head.header_len),
                            head_len: head.header_len,
                            error: Some("EOF-delimited response body is not supported"),
                        },
                        bytes.len(),
                    ));
                }
            });
        }

        match state.framing.as_mut()? {
            Framing::Sized { head_len, total } => {
                let head_len = *head_len;
                let total = *total;
                let body_len = total.saturating_sub(head_len);
                if body_len > self.max_response_body {
                    state.framing = None;
                    state.error = true;
                    return Some((
                        Head {
                            full: buf.slice(0..head_len),
                            head_len,
                            error: Some("response body exceeds size limit"),
                        },
                        bytes.len(),
                    ));
                }
                if bytes.len() < total {
                    return None;
                }
                state.framing = None;
                Some((
                    Head {
                        full: buf.slice(0..total),
                        head_len,
                        error: None,
                    },
                    total,
                ))
            }
            Framing::Chunked {
                head_len,
                decoder,
                scanned,
            } => {
                let head_len = *head_len;
                loop {
                    if *scanned > self.max_response_body {
                        state.framing = None;
                        state.error = true;
                        return Some((
                            Head {
                                full: buf.slice(0..head_len),
                                head_len,
                                error: Some("response body exceeds size limit"),
                            },
                            bytes.len(),
                        ));
                    }
                    let input = &bytes[head_len + *scanned..];
                    let (consumed, event) = match decoder.decode(input) {
                        Ok(pair) => pair,
                        Err(_) => return None,
                    };
                    *scanned += consumed;
                    match event {
                        DecodeEvent::NeedMore => return None,
                        DecodeEvent::Chunk(_) => continue,
                        DecodeEvent::Done(_) => {
                            let total = head_len + *scanned;
                            state.framing = None;
                            return Some((
                                Head {
                                    full: buf.slice(0..total),
                                    head_len,
                                    error: None,
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
