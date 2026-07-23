use std::ops::Range;

use o3::buffer::Shared;

use super::conn_state::{ConnState, ConsumeOutcome, DispatchPermit, NeedMore};
use crate::CANNED_400;
use crate::request;
use crate::service::{self, RouteRequestImpl, RouteSpec, SlicePath};

const MAX_HEADER_COUNT: usize = 128;

pub struct Ctx<'a> {
    pub head: &'a sark_core::http::codec::ParsedRequestHead<'a>,
    pub method_key: service::Key,
    pub slice_path: SlicePath<'a>,
    pub target_off: usize,
    pub target_len: usize,
    pub query_range: Option<Range<usize>>,
    pub req_bytes: &'a [u8],
}

impl<'a> Ctx<'a> {
    pub fn parse(
        req_bytes: &'a [u8],
        parsed: &'a sark_core::http::codec::ParsedRequestHead<'a>,
    ) -> Self {
        let method_key = service::Key::from_bytes(parsed.method);
        Self::parse_with_key(req_bytes, parsed, method_key)
    }

    pub fn parse_with_key(
        req_bytes: &'a [u8],
        parsed: &'a sark_core::http::codec::ParsedRequestHead<'a>,
        method_key: service::Key,
    ) -> Self {
        let target = parsed.target;
        let path_end = target
            .iter()
            .position(|&byte| byte == b'?')
            .unwrap_or(target.len());
        let slice_path = SlicePath::new(&target[..path_end]);
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
            method_key,
            slice_path,
            target_off,
            target_len,
            query_range,
            req_bytes,
        }
    }

    pub(super) fn http_method(&self) -> Result<http::Method, ()> {
        Ok(match self.method_key {
            service::Key::Get => http::Method::GET,
            service::Key::Post => http::Method::POST,
            service::Key::Put => http::Method::PUT,
            service::Key::Patch => http::Method::PATCH,
            service::Key::Delete => http::Method::DELETE,
            service::Key::Head => http::Method::HEAD,
            service::Key::Options => http::Method::OPTIONS,
            service::Key::Other => http::Method::from_bytes(self.head.method).map_err(|_| ())?,
        })
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
        if let Some(query) = self.query_range.clone()
            && R::Request::parse_query_raw(&mut raw_headers, req, query).is_err()
        {
            return Err(RequestErr::Bad(CANNED_400));
        }
        if self.http_method().is_err() {
            return Err(RequestErr::Bad(CANNED_400));
        }
        Ok(RequestDomainInput {
            storage: request::RequestStorage::new(retained, chunked_body, head_len),
            raw_params,
            raw_headers,
            target: self.target_off..(self.target_off + self.target_len),
            total,
            conn_close,
        })
    }

    fn retain(view: Option<&Shared>, req_bytes: &[u8], len: usize) -> Shared {
        if let Some(view) = view {
            let base = view.as_slice().as_ptr() as usize;
            if let Some(offset) = (req_bytes.as_ptr() as usize).checked_sub(base)
                && offset + len <= view.len()
            {
                return view.slice(offset..offset + len);
            }
        }
        Shared::copy_from_slice(&req_bytes[..len])
    }
}

pub struct Matched<R: RouteSpec> {
    pub raw_params: R::RawParams,
}

pub(super) struct RequestDomainInput<R: RouteSpec> {
    pub(super) storage: request::RequestStorage,
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

pub enum Framed<R: RouteSpec> {
    NeedMore,
    Bad,
    Ready {
        headers: R::RawHeaders,
        head_len: usize,
        body_framing: sark_core::http::codec::BodyFraming,
        flags: sark_core::http::head::Flags,
        accept_gzip: bool,
    },
}

impl<R: RouteSpec> Framed<R> {
    pub fn parse(req_bytes: &[u8], headers_start: usize) -> Self {
        let mut raw_headers = R::RawHeaders::default();
        let mut scan = sark_core::http::codec::HeaderScan::default();
        let mut flags = sark_core::http::head::Flags::default();
        let mut header_count = 0usize;
        let mut pos = headers_start;
        let head_len = loop {
            if pos + 2 > req_bytes.len() {
                return Framed::NeedMore;
            }
            let rest = &req_bytes[pos..];
            match R::Request::apply_header_contig(
                &mut raw_headers,
                req_bytes,
                rest,
                pos,
                &mut scan,
                &mut flags,
                &mut header_count,
                MAX_HEADER_COUNT,
            ) {
                Ok(Some(0)) => break pos + 2,
                Ok(Some(relative)) => pos += relative + 2,
                Ok(None) => return Framed::NeedMore,
                Err(_) => return Framed::Bad,
            }
        };
        let body_framing = match scan.validate_for_request() {
            Ok(framing) => framing,
            Err(_) => return Framed::Bad,
        };
        Framed::Ready {
            headers: raw_headers,
            head_len,
            body_framing,
            flags,
            accept_gzip: scan.accept_encoding_gzip,
        }
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
    body_framing: sark_core::http::codec::BodyFraming,
    is_bodyless_method: bool,
}

impl<R: RouteSpec> FramingBase<R> {
    fn from_ctx(ctx: &Ctx<'_>) -> Result<Self, RequestErr> {
        let head = ctx.head;
        let (raw_headers, head_len, body_framing, flags, accept_gzip) =
            match Framed::<R>::parse(ctx.req_bytes, head.headers_start) {
                Framed::Ready {
                    headers,
                    head_len,
                    body_framing,
                    flags,
                    accept_gzip,
                } => (headers, head_len, body_framing, flags, accept_gzip),
                Framed::NeedMore => return Err(RequestErr::NeedMore(NeedMore::Head)),
                Framed::Bad => return Err(RequestErr::Bad(CANNED_400)),
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
            return Err(RequestErr::Bad(crate::CANNED_413));
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
            sark_core::http::codec::BodyFraming::Length(length) => {
                base.checked_length(length)?;
                length
            }
            sark_core::http::codec::BodyFraming::Chunked => {
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
            sark_core::http::codec::BodyFraming::Length(length) => {
                base.checked_length(length)?;
                let total = head_len.saturating_add(length);
                if ctx.req_bytes.len() < total {
                    return Err(RequestErr::NeedMore(NeedMore::FixedBody(total)));
                }
                (total, None)
            }
            sark_core::http::codec::BodyFraming::Chunked => {
                if base.is_bodyless_method {
                    return Err(RequestErr::Bad(CANNED_400));
                }
                let chunked = &ctx.req_bytes[head_len..];
                match sark_core::http::codec::chunked::BodyDecoder::body_consumed(
                    chunked,
                    R::MAX_BODY,
                ) {
                    Ok(None) => return Err(RequestErr::NeedMore(NeedMore::ChunkedBody)),
                    Ok(Some((consumed, decoded))) => {
                        (head_len.saturating_add(consumed), Some(decoded))
                    }
                    Err(sark_core::error::Error::PayloadTooLarge(_)) => {
                        return Err(RequestErr::Bad(crate::CANNED_413));
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
