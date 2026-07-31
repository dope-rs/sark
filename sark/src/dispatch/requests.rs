use std::ops::Range;

use o3::buffer::Shared;
use sark_core::error::Error;
use sark_core::http::codec::chunked::BodyDecoder;
use sark_core::http::codec::{BodyFraming, RequestLine};

use super::conn_state::{ConnState, ConsumeOutcome, DispatchPermit, NeedMore};
use crate::request::RequestStorage;
use crate::service::{HeaderParse, RouteRequestImpl, RouteSpec};
use crate::{CANNED_400, CANNED_413};

const MAX_HEADER_COUNT: usize = 128;

pub struct Ctx<'a> {
    pub head: &'a RequestLine<'a>,
    pub target_off: usize,
    pub target_len: usize,
    pub query_range: Option<Range<usize>>,
    pub req_bytes: &'a [u8],
}

impl<'a> Ctx<'a> {
    pub fn routed(req_bytes: &'a [u8], parsed: &'a RequestLine<'a>, path_end: usize) -> Self {
        let target = parsed.target;
        debug_assert!(path_end <= target.len());
        let req_base = req_bytes.as_ptr() as usize;
        let target_off = target.as_ptr() as usize - req_base;
        let target_len = target.len();
        let query_range = if path_end < target_len {
            Some((target_off + path_end + 1)..(target_off + target_len))
        } else {
            None
        };
        Self {
            head: parsed,
            target_off,
            target_len,
            query_range,
            req_bytes,
        }
    }

    pub(super) fn assemble_domain<R: RouteSpec>(
        &self,
        raw_params: R::RawParams,
        conn: &mut ConnState,
    ) -> Result<RequestDomainInput<R>, RequestErr> {
        let Framing {
            mut raw_headers,
            head_len,
            total,
            conn_close,
            chunked_body,
            accept_gzip: _,
        } = Framing::<R>::from_ctx(self)?;
        let retain = if chunked_body.is_some() {
            head_len
        } else {
            total
        };
        let retained = Self::retain(conn.recv_view.as_ref(), self.req_bytes, retain);
        let req = retained.as_ref();
        if self.parse_query::<R>(&mut raw_headers, req).is_err() {
            return Err(RequestErr::Bad(CANNED_400));
        }
        Ok(RequestDomainInput {
            storage: RequestStorage::new(retained, chunked_body, head_len),
            raw_params,
            raw_headers,
            target: self.target_off..(self.target_off + self.target_len),
            total,
            conn_close,
        })
    }

    pub(super) fn parse_query<R: RouteSpec>(
        &self,
        raw_headers: &mut R::RawHeaders,
        request: &[u8],
    ) -> Result<(), ()> {
        if let Some(query) = self.query_range.clone() {
            R::Request::parse_query_raw(raw_headers, request, query).map_err(|_| ())?;
        }
        Ok(())
    }

    fn retain(view: Option<&Shared>, req_bytes: &[u8], len: usize) -> Shared {
        if let Some(view) = view {
            let base = view.as_slice().as_ptr() as usize;
            if let Some(offset) = (req_bytes.as_ptr() as usize).checked_sub(base)
                && let Some(end) = offset.checked_add(len)
                && end <= view.len()
                && let Some(retained) = view.get(offset..end)
            {
                return retained;
            }
        }
        Shared::copy_from_slice(&req_bytes[..len])
    }
}

pub struct Matched<R: RouteSpec> {
    pub raw_params: R::RawParams,
}

pub(super) struct RequestDomainInput<R: RouteSpec> {
    pub(super) storage: RequestStorage,
    pub(super) raw_params: R::RawParams,
    pub(super) raw_headers: R::RawHeaders,
    pub(super) target: Range<usize>,
    pub(super) total: usize,
    pub(super) conn_close: bool,
}

pub(super) enum RequestErr {
    NeedMore(NeedMore),
    Bad(&'static [u8]),
}

pub(super) fn assemble_matched<R: RouteSpec>(
    permit: DispatchPermit,
    matched: Matched<R>,
    ctx: &Ctx<'_>,
    conn: &mut ConnState,
) -> Result<RequestDomainInput<R>, ConsumeOutcome> {
    let Matched { raw_params } = matched;
    match ctx.assemble_domain::<R>(raw_params, conn) {
        Ok(request) => Ok(request),
        Err(RequestErr::NeedMore(state)) => Err(ConsumeOutcome::NeedMore { permit, state }),
        Err(RequestErr::Bad(reason)) => Err(ConsumeOutcome::Close(reason)),
    }
}

pub(super) struct Framing<R: RouteSpec> {
    pub(super) raw_headers: R::RawHeaders,
    pub(super) head_len: usize,
    pub(super) total: usize,
    pub(super) conn_close: bool,
    pub(super) chunked_body: Option<Shared>,
    pub(super) accept_gzip: bool,
}

pub(super) struct DiscardFraming<R: RouteSpec> {
    pub(super) raw_headers: R::RawHeaders,
    pub(super) head_len: usize,
    pub(super) body_total: usize,
    pub(super) conn_close: bool,
    pub(super) accept_gzip: bool,
}

struct FramingBase<R: RouteSpec> {
    raw_headers: R::RawHeaders,
    head_len: usize,
    conn_close: bool,
    accept_gzip: bool,
    body_framing: BodyFraming,
    is_bodyless_method: bool,
}

impl<R: RouteSpec> FramingBase<R> {
    fn from_ctx(ctx: &Ctx<'_>) -> Result<Self, RequestErr> {
        let head = ctx.head;
        let (raw_headers, head_len, body_framing, flags, accept_gzip) =
            match R::parse_headers(ctx.req_bytes, head.headers_start, MAX_HEADER_COUNT) {
                HeaderParse::Ready {
                    headers,
                    head_len,
                    body_framing,
                    flags,
                    accept_gzip,
                } => (headers, head_len, body_framing, flags, accept_gzip),
                HeaderParse::NeedMore => return Err(RequestErr::NeedMore(NeedMore::Head)),
                HeaderParse::Bad => return Err(RequestErr::Bad(CANNED_400)),
            };
        Ok(Self {
            raw_headers,
            head_len,
            conn_close: flags.implies_close(head.version),
            accept_gzip,
            body_framing,
            is_bodyless_method: head.method == b"GET" || head.method == b"HEAD",
        })
    }

    fn checked_length(&self, length: usize) -> Result<(), RequestErr> {
        if length > R::MAX_BODY {
            return Err(RequestErr::Bad(CANNED_413));
        }
        if length > 0 && self.is_bodyless_method {
            return Err(RequestErr::Bad(CANNED_400));
        }
        Ok(())
    }
}

impl<R: RouteSpec> DiscardFraming<R> {
    pub(super) fn from_ctx(ctx: &Ctx<'_>) -> Result<Self, RequestErr> {
        let base = FramingBase::<R>::from_ctx(ctx)?;
        let body_total = match base.body_framing {
            BodyFraming::Length(length) => {
                base.checked_length(length)?;
                length
            }
            BodyFraming::Chunked => {
                return Err(RequestErr::Bad(CANNED_400));
            }
        };
        Ok(Self {
            raw_headers: base.raw_headers,
            head_len: base.head_len,
            body_total,
            conn_close: base.conn_close,
            accept_gzip: base.accept_gzip,
        })
    }
}

impl<R: RouteSpec> Framing<R> {
    pub(super) fn from_ctx(ctx: &Ctx<'_>) -> Result<Self, RequestErr> {
        let base = FramingBase::<R>::from_ctx(ctx)?;
        let head_len = base.head_len;
        let (total, chunked_body) = match base.body_framing {
            BodyFraming::Length(length) => {
                base.checked_length(length)?;
                let total = head_len.saturating_add(length);
                if ctx.req_bytes.len() < total {
                    return Err(RequestErr::NeedMore(NeedMore::FixedBody(total)));
                }
                (total, None)
            }
            BodyFraming::Chunked => {
                if base.is_bodyless_method {
                    return Err(RequestErr::Bad(CANNED_400));
                }
                let chunked = &ctx.req_bytes[head_len..];
                match BodyDecoder::body_consumed(chunked, R::MAX_BODY) {
                    Ok(None) => return Err(RequestErr::NeedMore(NeedMore::ChunkedBody)),
                    Ok(Some((consumed, decoded))) => {
                        (head_len.saturating_add(consumed), Some(decoded))
                    }
                    Err(Error::PayloadTooLarge(_)) => {
                        return Err(RequestErr::Bad(CANNED_413));
                    }
                    Err(_) => return Err(RequestErr::Bad(CANNED_400)),
                }
            }
        };
        Ok(Self {
            raw_headers: base.raw_headers,
            head_len,
            total,
            conn_close: base.conn_close,
            chunked_body,
            accept_gzip: base.accept_gzip,
        })
    }
}
