pub mod conn_state;
pub mod pipeline;
pub mod preser;
pub mod routing;

use std::future::Future;
use std::mem;
use std::ops::Range;
use std::task::{Context, Poll};

pub use conn_state::{ConsumeOutcome, Outcome};
use dope::Driver;
use dope::manifold::listener;
use dope::transport::link;
use dope::transport::wire::Wire;
use o3::buffer::Shared;
pub use pipeline::Pipeline;
use preser::Slot;
pub use routing::Routing;
use sark_core::http::{CHUNK_TERMINATOR, FixedResponseInner, Shape};

use crate::service::{self, RouteRequestImpl, RouteSpec, SlicePath, manifold};
use crate::{CANNED_400, CANNED_500, Request, request};

impl service::Key {
    pub fn miss_tag(maybe: Option<Self>, path_hit: bool) -> u64 {
        let method_tag = match maybe {
            None => 0u64,
            Some(service::Key::Other) => 1u64,
            Some(service::Key::Get) => 2u64,
            Some(service::Key::Post) => 3u64,
            Some(service::Key::Put) => 4u64,
            Some(service::Key::Patch) => 5u64,
            Some(service::Key::Delete) => 6u64,
            Some(service::Key::Head) => 7u64,
            Some(service::Key::Options) => 8u64,
        };
        let path_tag = if path_hit { 1u64 } else { 0u64 };
        (path_tag << 8) | method_tag
    }
}

pub struct Ctx<'a> {
    pub head: &'a crate::parser::framer::ParsedHead<'a>,
    pub method_key: crate::service::Key,
    pub slice_path: SlicePath<'a>,
    pub target_off: usize,
    pub target_len: usize,
    pub query_range: Option<Range<usize>>,
    pub req_bytes: &'a [u8],
}

impl<'a> Ctx<'a> {
    pub fn parse(req_bytes: &'a [u8], parsed: &'a crate::parser::framer::ParsedHead<'a>) -> Self {
        let method_key = crate::service::Key::from_bytes(parsed.method);
        Self::parse_with_key(req_bytes, parsed, method_key)
    }

    pub fn parse_with_key(
        req_bytes: &'a [u8],
        parsed: &'a crate::parser::framer::ParsedHead<'a>,
        method_key: crate::service::Key,
    ) -> Self {
        let target = parsed.target;
        let path_end = target
            .iter()
            .position(|&b| b == b'?')
            .unwrap_or(target.len());
        let path_bytes = &target[..path_end];
        let slice_path = SlicePath::new(path_bytes);
        let req_base = req_bytes.as_ptr() as usize;
        let target_off = target.as_ptr() as usize - req_base;
        let target_len = target.len();
        let query_range: Option<Range<usize>> = if path_end < target_len {
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
}

pub trait ResponseEncoder {
    fn emit(&mut self, status: http::StatusCode, headers_wire: &[u8], body: &[u8]);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decoded {
    Emitted,
    NotFound,
    Bad,
    Unsupported,
}

pub trait Decode {
    fn dispatch_decoded<E: ResponseEncoder>(
        &self,
        method: http::Method,
        path: &[u8],
        headers: &[(&[u8], Range<usize>)],
        head_bytes: &[u8],
        body_bytes: &[u8],
        encoder: &mut E,
    ) -> Decoded;
}

struct StaticRequest<R: RouteSpec> {
    request: Request,
    params: <R as RouteSpec>::Params<'static>,
    headers: <R as RouteSpec>::Headers<'static>,
    body: <R as RouteSpec>::ParsedBody<'static>,
    total: usize,
    conn_close: bool,
}

enum RequestErr {
    NeedMore(Option<usize>),
    Bad(&'static [u8]),
}

impl Ctx<'_> {
    fn http_method(&self) -> Result<http::Method, ()> {
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

    fn assemble_static<R: RouteSpec>(
        &self,
        raw_params: <R as RouteSpec>::RawParams,
        conn: &mut conn_state::ConnState,
    ) -> Result<StaticRequest<R>, RequestErr> {
        let Framing {
            mut raw_headers,
            head_len,
            total,
            conn_close,
            chunked_body,
            accept_gzip: _,
        } = Framing::<R>::from_ctx(self)?;
        if let Some(decoded) = chunked_body {
            conn.chunked_body = Some(decoded);
        }
        let req = &self.req_bytes[..total];
        if let Some(qrange) = self.query_range.clone()
            && <<R as RouteSpec>::Request as RouteRequestImpl>::parse_query_raw(
                &mut raw_headers,
                req,
                qrange,
            )
            .is_err()
        {
            return Err(RequestErr::Bad(CANNED_400));
        }
        let Ok(http_method) = self.http_method() else {
            return Err(RequestErr::Bad(CANNED_400));
        };
        let target_range = self.target_off..(self.target_off + self.target_len);
        let body_bytes: &[u8] = match &conn.chunked_body {
            Some(shared) => shared.as_ref(),
            None => &req[head_len..],
        };
        // SAFETY: borrows the conn recv buffer (frozen) for the head, and conn.chunked_body (alive until slab.release) for the body — 'static sound for fiber/stream lifetime.
        let request = unsafe {
            Request::from_borrowed_static(http_method, target_range, &req[..head_len], body_bytes)
        };
        let Some(params) =
            <<R as RouteSpec>::Request as RouteRequestImpl>::build_params(&request, raw_params)
        else {
            return Err(RequestErr::Bad(CANNED_400));
        };
        let headers = match <<R as RouteSpec>::Request as RouteRequestImpl>::build_headers(
            &request,
            raw_headers,
        ) {
            Ok(h) => h,
            Err(_) => return Err(RequestErr::Bad(CANNED_400)),
        };
        let body_borrowed = match <R as RouteSpec>::parse_body(body_bytes) {
            Ok(b) => b,
            Err(_) => return Err(RequestErr::Bad(CANNED_400)),
        };
        // SAFETY: parsed body borrows from the same frozen storage as `request`.
        let body = unsafe { Pipeline::lift_parsed_body_to_static::<R>(body_borrowed) };
        Ok(StaticRequest {
            request,
            params,
            headers,
            body,
            total,
            conn_close,
        })
    }
}

const MAX_HEADER_COUNT: usize = 128;

pub enum Framed<R: RouteSpec> {
    NeedMore,
    Bad,
    Ready {
        headers: <R as RouteSpec>::RawHeaders,
        head_len: usize,
        body_framing: sark_core::http::codec::BodyFraming,
        flags: sark_core::http::head::Flags,
        accept_gzip: bool,
    },
}

impl<R: RouteSpec> Framed<R> {
    pub fn parse(req_bytes: &[u8], headers_start: usize) -> Self {
        let mut raw_headers = <<R as RouteSpec>::RawHeaders as std::default::Default>::default();
        let mut scan = sark_core::http::codec::HeaderScan::default();
        let mut flags = sark_core::http::head::Flags::default();
        let mut header_count = 0usize;
        let mut pos = headers_start;
        let head_len = loop {
            if pos + 2 > req_bytes.len() {
                return Framed::NeedMore;
            }
            let rest = &req_bytes[pos..];
            match <<R as RouteSpec>::Request as RouteRequestImpl>::apply_header_contig(
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
                Ok(Some(rel)) => pos += rel + 2,
                Ok(None) => return Framed::NeedMore,
                Err(_) => return Framed::Bad,
            }
        };
        let body_framing = match scan.validate_for_request() {
            Ok(f) => f,
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

impl Outcome {
    pub fn into_consume(
        self,
        permit: conn_state::DispatchPermit,
        consumed: usize,
        conn_close: bool,
    ) -> conn_state::ConsumeOutcome {
        use conn_state::ConsumeOutcome;
        match self {
            Outcome::Send {
                written,
                close_after,
            } => ConsumeOutcome::Done {
                permit,
                consumed,
                written,
                close: close_after || conn_close,
            },
            Outcome::SendStatic {
                hdr_written,
                body,
                close_after,
            } => ConsumeOutcome::DoneStatic {
                permit,
                consumed,
                hdr_written,
                body,
                close: close_after || conn_close,
            },
            Outcome::SendSplit {
                hdr_written,
                body,
                close_after,
            } => ConsumeOutcome::DoneSplit {
                permit,
                consumed,
                hdr_written,
                body,
                close: close_after || conn_close,
            },
            Outcome::Park => ConsumeOutcome::Park {
                consumed,
                close: conn_close,
            },
            Outcome::Close(reason) => ConsumeOutcome::Close(reason),
        }
    }

    pub fn apply<W: Wire, C: Default + 'static>(
        self,
        slot: &mut link::Slot<W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) {
        let close_after = match &self {
            Outcome::Park => return,
            Outcome::Close(reason) => {
                slot.core.set_close_after();
                if !reason.is_empty() {
                    let buf = aux.write_buf_for(slot);
                    let ud = slot.token();
                    slot.submit_split_static(buf, 0, reason, ud, driver);
                }
                return;
            }
            Outcome::Send { close_after, .. }
            | Outcome::SendStatic { close_after, .. }
            | Outcome::SendSplit { close_after, .. } => *close_after,
        };
        if close_after {
            slot.core.set_close_after();
        }
        let buf = aux.write_buf_for(slot);
        let ud = slot.token();
        match self {
            Outcome::Send { written, .. } => slot.submit_buffered(buf, written, ud, driver),
            Outcome::SendStatic {
                hdr_written, body, ..
            } => slot.submit_split_static(buf, hdr_written, body, ud, driver),
            Outcome::SendSplit {
                hdr_written, body, ..
            } => slot.submit_split_shared(buf, hdr_written, body, ud, driver),
            Outcome::Park | Outcome::Close(_) => unreachable!(),
        }
    }

    fn write_response<R: RouteSpec>(
        resp: <R as RouteSpec>::Response<'_>,
        write: &mut [u8],
        date: &[u8; 29],
        close_after: bool,
    ) -> Self {
        match Shape::write_into_slice(&resp, write, date) {
            Some(written) => Outcome::Send {
                written,
                close_after,
            },
            None => match Shape::write_head_split(resp, write, date) {
                Some((hdr_written, body)) => Outcome::SendSplit {
                    hdr_written,
                    body,
                    close_after,
                },
                None => Outcome::Close(CANNED_500),
            },
        }
    }
}

pub struct Matched<'r, R: RouteSpec> {
    pub route: &'r R,
    pub raw_params: <R as RouteSpec>::RawParams,
}

struct FramingBase<R: RouteSpec> {
    raw_headers: <R as RouteSpec>::RawHeaders,
    head_len: usize,
    conn_close: bool,
    accept_gzip: bool,
    body_framing: sark_core::http::codec::BodyFraming,
    is_bodyless_method: bool,
}

struct Framing<R: RouteSpec> {
    raw_headers: <R as RouteSpec>::RawHeaders,
    head_len: usize,
    total: usize,
    conn_close: bool,
    chunked_body: Option<Shared>,
    accept_gzip: bool,
}

struct StreamFraming<R: RouteSpec> {
    raw_headers: <R as RouteSpec>::RawHeaders,
    head_len: usize,
    body_total: usize,
    conn_close: bool,
}

impl<R: RouteSpec> FramingBase<R> {
    fn from_ctx(ctx: &Ctx<'_>) -> Result<Self, RequestErr> {
        let head = ctx.head;
        let req_bytes = ctx.req_bytes;
        let (raw_headers, head_len, body_framing, flags, accept_gzip) =
            match Framed::<R>::parse(req_bytes, head.headers_start) {
                Framed::Ready {
                    headers,
                    head_len,
                    body_framing,
                    flags,
                    accept_gzip,
                } => (headers, head_len, body_framing, flags, accept_gzip),
                Framed::NeedMore => return Err(RequestErr::NeedMore(None)),
                Framed::Bad => return Err(RequestErr::Bad(CANNED_400)),
            };
        let is_bodyless_method = head.method == b"GET" || head.method == b"HEAD";
        let conn_close = flags.implies_close(head.version);
        Ok(FramingBase {
            raw_headers,
            head_len,
            conn_close,
            accept_gzip,
            body_framing,
            is_bodyless_method,
        })
    }

    fn checked_length(&self, n: usize) -> Result<(), RequestErr> {
        if n > <R as RouteSpec>::MAX_BODY {
            return Err(RequestErr::Bad(crate::CANNED_413));
        }
        if n > 0 && self.is_bodyless_method {
            return Err(RequestErr::Bad(CANNED_400));
        }
        Ok(())
    }
}

impl<R: RouteSpec> StreamFraming<R> {
    fn from_ctx(ctx: &Ctx<'_>) -> Result<Self, RequestErr> {
        use sark_core::http::codec::BodyFraming;
        let base = FramingBase::<R>::from_ctx(ctx)?;
        let body_total = match base.body_framing {
            BodyFraming::Length(n) => {
                base.checked_length(n)?;
                n
            }
            BodyFraming::Chunked => {
                return Err(RequestErr::Bad(CANNED_400));
            }
        };
        Ok(StreamFraming {
            raw_headers: base.raw_headers,
            head_len: base.head_len,
            body_total,
            conn_close: base.conn_close,
        })
    }
}

impl<R: RouteSpec> Framing<R> {
    fn from_ctx(ctx: &Ctx<'_>) -> Result<Self, RequestErr> {
        use sark_core::http::codec::{BodyFraming, Parse};
        let req_bytes = ctx.req_bytes;
        let base = FramingBase::<R>::from_ctx(ctx)?;
        let head_len = base.head_len;
        let (total, chunked_body): (usize, Option<Shared>) = match base.body_framing {
            BodyFraming::Length(n) => {
                base.checked_length(n)?;
                let total = head_len.saturating_add(n);
                if req_bytes.len() < total {
                    return Err(RequestErr::NeedMore(Some(total)));
                }
                (total, None)
            }
            BodyFraming::Chunked => {
                if base.is_bodyless_method {
                    return Err(RequestErr::Bad(CANNED_400));
                }
                let chunked_section = &req_bytes[head_len..];
                match Parse::chunked_body_consumed(chunked_section, <R as RouteSpec>::MAX_BODY) {
                    Ok(None) => return Err(RequestErr::NeedMore(None)),
                    Ok(Some((consumed, decoded))) => (
                        head_len.saturating_add(consumed),
                        Some(Shared::from(decoded)),
                    ),
                    Err(sark_core::error::Error::PayloadTooLarge(_)) => {
                        return Err(RequestErr::Bad(crate::CANNED_413));
                    }
                    Err(_) => return Err(RequestErr::Bad(CANNED_400)),
                }
            }
        };
        Ok(Framing {
            raw_headers: base.raw_headers,
            head_len,
            total,
            conn_close: base.conn_close,
            chunked_body,
            accept_gzip: base.accept_gzip,
        })
    }
}

impl Pipeline {
    unsafe fn lift_parsed_body_to_static<R: RouteSpec>(
        body: <R as RouteSpec>::ParsedBody<'_>,
    ) -> <R as RouteSpec>::ParsedBody<'static> {
        // SAFETY: ParsedBody differs only in lifetime → identical layout; transmute_copy is used because the compiler cannot prove size-equality for the GAT.
        unsafe {
            let md = mem::ManuallyDrop::new(body);
            mem::transmute_copy(&*md)
        }
    }

    fn try_static_response_hit<R: RouteSpec>(
        write: &mut [u8],
        date: &[u8; 29],
        cache: &Slot<'_>,
    ) -> Option<Outcome> {
        if !<R as RouteSpec>::STATIC_RESPONSE {
            return None;
        }
        Some(match cache.try_hit(write, date)? {
            preser::Hit::Fixed { written } => Outcome::Send {
                written,
                close_after: false,
            },
            preser::Hit::Static { hdr_written, body } => Outcome::SendStatic {
                hdr_written,
                body,
                close_after: false,
            },
        })
    }

    fn finish_sync<R: RouteSpec>(
        mut resp: <R as RouteSpec>::Response<'_>,
        write: &mut [u8],
        date: &[u8; 29],
        cache: Slot<'_>,
        accept_gzip: bool,
    ) -> Outcome {
        use sark_core::http::body_kind::ResponseKind;

        let static_body = matches!(<R as RouteSpec>::RESPONSE_BODY_KIND, ResponseKind::Static);
        if accept_gzip
            && !<R as RouteSpec>::STATIC_RESPONSE
            && matches!(<R as RouteSpec>::RESPONSE_BODY_KIND, ResponseKind::Inline)
            && let Some(plain) = Shape::body_for_gzip(&resp)
            && !plain.is_empty()
        {
            let compressed = sark_core::http::compress::Gzip::with_thread_local(|g| {
                let bytes = g.encode(plain);
                o3::buffer::Shared::from(bytes.to_vec())
            });
            Shape::apply_gzip_body(&mut resp, compressed);
        }
        if <R as RouteSpec>::STATIC_RESPONSE && static_body {
            return match Shape::preserialize_static(&resp) {
                Some((head_template, date_offset, body)) => {
                    let hdr = FixedResponseInner::write_preserialized(
                        write,
                        &head_template,
                        date_offset,
                        date,
                    );
                    cache.store_static(head_template, date_offset, body);
                    match hdr {
                        Some(hdr_written) => Outcome::SendStatic {
                            hdr_written,
                            body,
                            close_after: false,
                        },
                        None => Outcome::Close(CANNED_500),
                    }
                }
                None => Outcome::Close(CANNED_500),
            };
        }
        if <R as RouteSpec>::STATIC_RESPONSE {
            let (template, date_offset) = Shape::preserialize(&resp);
            let n = FixedResponseInner::write_preserialized(write, &template, date_offset, date);
            cache.store_fixed(template, date_offset);
            return match n {
                Some(n) => Outcome::Send {
                    written: n,
                    close_after: false,
                },
                None => Outcome::Close(CANNED_500),
            };
        }
        if static_body {
            return match Shape::write_head_only(&resp, write, date) {
                Some((hdr_written, body)) => Outcome::SendStatic {
                    hdr_written,
                    body,
                    close_after: false,
                },
                None => Outcome::Close(CANNED_500),
            };
        }
        if matches!(<R as RouteSpec>::RESPONSE_BODY_KIND, ResponseKind::Stream) {
            unreachable!(
                "finish_sync: Stream routes go through route_sync_stream, not finish_sync"
            );
        }
        Outcome::write_response::<R>(resp, write, date, false)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_and_invoke<'req, 'a, R, S>(
        route: &'a R,
        raw_params: <R as RouteSpec>::RawParams,
        raw_headers: <R as RouteSpec>::RawHeaders,
        http_method: http::Method,
        target_range: Range<usize>,
        head_bytes: &'req [u8],
        body_bytes: &'req [u8],
        state: &'a S,
    ) -> Result<<R as RouteSpec>::Response<'req>, &'static [u8]>
    where
        R: RouteSpec + manifold::Route<S> + 'static,
        'req: 'a,
    {
        let request_ref_bare =
            request::Ref::<'_, ()>::from_slice(http_method, target_range, head_bytes, body_bytes);
        let Some(params) = <<R as RouteSpec>::Request as RouteRequestImpl>::build_params_ref(
            &request_ref_bare,
            raw_params,
        ) else {
            return Err(CANNED_400);
        };
        let headers = match <<R as RouteSpec>::Request as RouteRequestImpl>::build_headers_ref(
            &request_ref_bare,
            raw_headers,
        ) {
            Ok(h) => h,
            Err(_) => return Err(CANNED_400),
        };
        let request_ref = request_ref_bare.with_headers_ready::<<R as RouteSpec>::Headers<'_>>();
        let parsed_body = match <R as RouteSpec>::parse_body(body_bytes) {
            Ok(b) => b,
            Err(_) => return Err(CANNED_400),
        };
        Ok(<R as manifold::Route<S>>::invoke(
            route,
            params,
            &request_ref,
            headers,
            parsed_body,
            state,
        ))
    }

    pub fn route_manifold<R, S>(
        permit: conn_state::DispatchPermit,
        matched: Matched<'_, R>,
        state: &S,
        ctx: &Ctx<'_>,
        date: &[u8; 29],
        cache: Slot<'_>,
        write: &mut [u8],
    ) -> conn_state::ConsumeOutcome
    where
        R: RouteSpec + manifold::Route<S> + 'static,
    {
        if <R as RouteSpec>::STREAMING_BODY {
            return Self::route_manifold_stream::<R, S>(permit, matched, state, ctx, date, write);
        }
        use conn_state::ConsumeOutcome;
        let Matched { route, raw_params } = matched;
        let Framing {
            mut raw_headers,
            head_len,
            total,
            conn_close,
            chunked_body,
            accept_gzip,
        } = match Framing::<R>::from_ctx(ctx) {
            Ok(x) => x,
            Err(RequestErr::NeedMore(content_length)) => {
                return ConsumeOutcome::NeedMore {
                    permit,
                    content_length,
                };
            }
            Err(RequestErr::Bad(reason)) => return ConsumeOutcome::Close(reason),
        };
        let req = &ctx.req_bytes[..total];
        if let Some(out) = Self::try_static_response_hit::<R>(write, date, &cache) {
            return out.into_consume(permit, total, conn_close);
        }
        if let Some(qrange) = ctx.query_range.clone()
            && <<R as RouteSpec>::Request as RouteRequestImpl>::parse_query_raw(
                &mut raw_headers,
                req,
                qrange,
            )
            .is_err()
        {
            return ConsumeOutcome::Close(CANNED_400);
        }
        let body_bytes: &[u8] = match chunked_body.as_ref() {
            Some(shared) => shared.as_ref(),
            None => &req[head_len..],
        };
        let http_method = match ctx.http_method() {
            Ok(m) => m,
            Err(_) => return ConsumeOutcome::Close(CANNED_400),
        };
        let target_range = ctx.target_off..(ctx.target_off + ctx.target_len);
        let out = match Self::build_and_invoke::<R, S>(
            route,
            raw_params,
            raw_headers,
            http_method,
            target_range,
            &req[..head_len],
            body_bytes,
            state,
        ) {
            Ok(resp) => Self::finish_sync::<R>(resp, write, date, cache, accept_gzip),
            Err(reason) => return ConsumeOutcome::Close(reason),
        };
        out.into_consume(permit, total, conn_close)
    }

    fn route_manifold_stream<R, S>(
        permit: conn_state::DispatchPermit,
        matched: Matched<'_, R>,
        state: &S,
        ctx: &Ctx<'_>,
        date: &[u8; 29],
        write: &mut [u8],
    ) -> conn_state::ConsumeOutcome
    where
        R: RouteSpec + manifold::Route<S> + 'static,
    {
        use conn_state::ConsumeOutcome;
        let Matched { route, raw_params } = matched;
        let StreamFraming {
            mut raw_headers,
            head_len,
            body_total,
            conn_close,
        } = match StreamFraming::<R>::from_ctx(ctx) {
            Ok(x) => x,
            Err(RequestErr::NeedMore(content_length)) => {
                return ConsumeOutcome::NeedMore {
                    permit,
                    content_length,
                };
            }
            Err(RequestErr::Bad(reason)) => return ConsumeOutcome::Close(reason),
        };
        let req_head = &ctx.req_bytes[..head_len];
        if let Some(qrange) = ctx.query_range.clone()
            && <<R as RouteSpec>::Request as RouteRequestImpl>::parse_query_raw(
                &mut raw_headers,
                req_head,
                qrange,
            )
            .is_err()
        {
            return ConsumeOutcome::Close(CANNED_400);
        }
        let body_bytes: &[u8] = &[];
        let http_method = match ctx.http_method() {
            Ok(m) => m,
            Err(_) => return ConsumeOutcome::Close(CANNED_400),
        };
        let target_range = ctx.target_off..(ctx.target_off + ctx.target_len);
        let mut request_ref_bare =
            request::Ref::<'_, ()>::from_slice(http_method, target_range, req_head, body_bytes);
        request_ref_bare.set_declared_body_len(body_total);
        let Some(params) = <<R as RouteSpec>::Request as RouteRequestImpl>::build_params_ref(
            &request_ref_bare,
            raw_params,
        ) else {
            return ConsumeOutcome::Close(CANNED_400);
        };
        let headers = match <<R as RouteSpec>::Request as RouteRequestImpl>::build_headers_ref(
            &request_ref_bare,
            raw_headers,
        ) {
            Ok(h) => h,
            Err(_) => return ConsumeOutcome::Close(CANNED_400),
        };
        let request_ref = request_ref_bare.with_headers_ready::<<R as RouteSpec>::Headers<'_>>();
        let parsed_body = match <R as RouteSpec>::parse_body(body_bytes) {
            Ok(b) => b,
            Err(_) => return ConsumeOutcome::Close(CANNED_400),
        };
        let resp = <R as manifold::Route<S>>::invoke(
            route,
            params,
            &request_ref,
            headers,
            parsed_body,
            state,
        );
        let written = match Shape::write_into_slice(&resp, write, date) {
            Some(n) => n,
            None => match Shape::write_head_split(resp, write, date) {
                Some((hdr_written, body)) => {
                    let body_end = hdr_written + body.len();
                    if body_end <= write.len() {
                        write[hdr_written..body_end].copy_from_slice(body.as_ref());
                        body_end
                    } else {
                        return ConsumeOutcome::Close(CANNED_500);
                    }
                }
                None => return ConsumeOutcome::Close(CANNED_500),
            },
        };
        ConsumeOutcome::StreamArmed {
            permit,
            head_consumed: head_len,
            body_total,
            written,
            close: conn_close,
        }
    }

    pub fn finish_pending<R: RouteSpec, W: Wire, C: Default + 'static>(
        resp: <R as RouteSpec>::Response<'static>,
        slot: &mut link::Slot<W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
        date: &[u8; 29],
        close: bool,
    ) {
        use sark_core::http::body_kind::ResponseKind;
        if matches!(<R as RouteSpec>::RESPONSE_BODY_KIND, ResponseKind::Stream) {
            unreachable!("finish_pending: Stream routes go through finish_pending_stream");
        }
        let outcome = {
            let write = aux.write_buf_for(slot);
            Outcome::write_response::<R>(resp, write, date, close)
        };
        outcome.apply(slot, aux, driver);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn route_sync_stream<'d, R, S, S2, const N: usize, const ROUTE_ID: u8>(
        permit: conn_state::DispatchPermit,
        matched: Matched<'d, R>,
        stream_slab: &mut dope::fiber::Slab<'d, S2, N>,
        state: &'d S,
        ctx: &Ctx<'_>,
        write: &mut [u8],
        date: &[u8; 29],
        conn: &mut conn_state::ConnState,
    ) -> conn_state::ConsumeOutcome
    where
        R: RouteSpec + manifold::StreamRoute<S> + 'static,
        for<'req> <R as RouteSpec>::Response<'req>: Shape<'req, StreamInner = S2>,
        S: 'd,
        S2: Future<Output = Option<Shared>> + 'd,
    {
        use conn_state::ConsumeOutcome;
        let Matched { route, raw_params } = matched;
        let StaticRequest {
            request,
            params,
            headers,
            body,
            total,
            conn_close,
        } = match ctx.assemble_static::<R>(raw_params, conn) {
            Ok(r) => r,
            Err(RequestErr::NeedMore(content_length)) => {
                return ConsumeOutcome::NeedMore {
                    permit,
                    content_length,
                };
            }
            Err(RequestErr::Bad(reason)) => return ConsumeOutcome::Close(reason),
        };
        let resp =
            <R as manifold::StreamRoute<S>>::invoke(route, params, request, headers, body, state);
        let Some((hdr_written, sark_stream)) = Shape::write_head_stream(resp, write, date) else {
            return ConsumeOutcome::Close(CANNED_500);
        };
        match stream_slab.alloc(dope::fiber::Fiber::new(sark_stream)) {
            Some(slot_idx) => {
                conn.async_state.stream_slot = Some((ROUTE_ID, slot_idx));
                conn.async_state.stream_phase = conn_state::StreamPhase::Streaming;
                conn.async_state.stream_pending = None;
                ConsumeOutcome::Streamed {
                    consumed: total,
                    written: hdr_written,
                    close: conn_close,
                }
            }
            None => ConsumeOutcome::Close(crate::CANNED_503),
        }
    }

    pub fn stream_coalesce<'d, S2, W, const N: usize>(
        slab: &mut dope::fiber::Slab<'d, S2, N>,
        slot: &mut link::Slot<W, listener::State<conn_state::ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) -> usize
    where
        S2: Future<Output = Option<Shared>> + 'd,
        W: Wire,
    {
        use conn_state::StreamPhase;
        let conn_ptr: *mut conn_state::ConnState = &mut slot.state.conn;
        // SAFETY: conn_ptr aliases slot.state.conn; below never re-borrows slot.state.conn via slot (only slot.core / wire / state.send), keeping &mut ConnState and &mut Slot disjoint.
        let conn: &mut conn_state::ConnState = unsafe { &mut *conn_ptr };
        let Some((stream_route_id, token)) = conn.async_state.stream_slot.take() else {
            return 0;
        };
        let waker = slot.make_waker(driver);
        let mut cx = Context::from_waker(&waker);
        let mut cursor = 0;
        loop {
            let (framed, is_terminator) = match conn.async_state.stream_pending.take() {
                Some(stashed) => (
                    stashed,
                    conn.async_state.stream_phase == StreamPhase::Terminating,
                ),
                None => match conn.async_state.stream_phase {
                    StreamPhase::Terminating => (Shared::from_static(CHUNK_TERMINATOR), true),
                    StreamPhase::Streaming => match slab.poll(&token, &mut cx) {
                        Poll::Ready(Some(raw)) => {
                            if raw.is_empty() {
                                continue;
                            }
                            (sark_core::http::codec::Wire::chunk_frame(raw), false)
                        }
                        Poll::Ready(None) => {
                            conn.async_state.stream_phase = StreamPhase::Terminating;
                            continue;
                        }
                        Poll::Pending => {
                            conn.async_state.stream_slot = Some((stream_route_id, token));
                            return cursor;
                        }
                    },
                },
            };
            let write_cap = aux.write_buf_for(slot).len();
            if write_cap.saturating_sub(cursor) < framed.len() {
                if framed.len() > write_cap {
                    let buf = aux.write_buf_for(slot);
                    let ud = slot.token();
                    slot.submit_split_shared(buf, cursor, framed, ud, driver);
                    if is_terminator {
                        slab.release(token);
                        conn.async_state.stream_phase = StreamPhase::Streaming;
                        conn.recv.unfreeze();
                        conn.chunked_body = None;
                        if conn.deferred_close {
                            slot.core.set_close_after();
                        }
                    } else {
                        conn.async_state.stream_slot = Some((stream_route_id, token));
                    }
                    return 0;
                }
                conn.async_state.stream_slot = Some((stream_route_id, token));
                conn.async_state.stream_pending = Some(framed);
                return cursor;
            }
            let flen = framed.len();
            aux.write_buf_for(slot)[cursor..cursor + flen].copy_from_slice(framed.as_ref());
            cursor += flen;
            if is_terminator {
                slab.release(token);
                conn.async_state.stream_phase = StreamPhase::Streaming;
                conn.recv.unfreeze();
                conn.chunked_body = None;
                if conn.deferred_close {
                    slot.core.set_close_after();
                }
                return cursor;
            }
        }
    }

    pub fn reborrow_write_buf<'a, W: Wire>(
        slot: &mut link::Slot<W, listener::State<conn_state::ConnState>>,
        aux: &'a mut listener::Aux,
    ) -> &'a mut [u8] {
        aux.write_buf_for(slot)
    }

    pub fn fiber_wake<'d, Fut, P, W, const N: usize>(
        slab: &mut dope::fiber::Slab<'d, Fut, N>,
        slot: &mut link::Slot<W, listener::State<conn_state::ConnState>>,
        driver: &mut Driver,
    ) -> Option<P>
    where
        Fut: Future<Output = P> + 'd,
        W: Wire,
    {
        let waker = slot.make_waker(driver);
        let mut cx = Context::from_waker(&waker);
        let conn_ptr: *mut conn_state::ConnState = &mut slot.state.conn;
        // SAFETY: conn_ptr aliases slot.state.conn; this fn never re-borrows slot.state.conn via slot (only slot.make_waker / slot is otherwise unused after this point), so the &mut ConnState and &mut Slot stay disjoint.
        let conn: &mut conn_state::ConnState = unsafe { &mut *conn_ptr };
        let token = conn.async_state.pending_wake.as_ref().map(|p| &p.1)?;
        match slab.poll(token, &mut cx) {
            Poll::Ready(resp) => {
                if let Some((_, owned)) = conn.async_state.pending_wake.take() {
                    slab.release(owned);
                }
                conn.chunked_body = None;
                Some(resp)
            }
            Poll::Pending => None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn route_fiber<'d, R, S, P, MP, Fut, const N: usize, const ROUTE_ID: u8>(
        permit: conn_state::DispatchPermit,
        matched: Matched<'d, R>,
        slab: &mut dope::fiber::Slab<'d, Fut, N>,
        state: &'d S,
        ctx: &Ctx<'_>,
        timer: crate::timer::Timer<'d>,
        conn: &mut conn_state::ConnState,
        make_fiber: MP,
    ) -> conn_state::ConsumeOutcome
    where
        R: RouteSpec + crate::fiber::Route<S> + 'static,
        S: 'd,
        P: 'd,
        Fut: Future<Output = P> + 'd,
        MP: FnOnce(
            &'d R,
            <R as RouteSpec>::Params<'static>,
            Request,
            <R as RouteSpec>::Headers<'static>,
            <R as RouteSpec>::ParsedBody<'static>,
            &'d S,
            crate::timer::Timer<'d>,
        ) -> dope::fiber::Fiber<'d, Fut>,
    {
        use conn_state::ConsumeOutcome;
        let Matched { route, raw_params } = matched;
        let StaticRequest {
            request,
            params,
            headers,
            body,
            total,
            conn_close,
        } = match ctx.assemble_static::<R>(raw_params, conn) {
            Ok(r) => r,
            Err(RequestErr::NeedMore(content_length)) => {
                return ConsumeOutcome::NeedMore {
                    permit,
                    content_length,
                };
            }
            Err(RequestErr::Bad(reason)) => return ConsumeOutcome::Close(reason),
        };
        let fiber = make_fiber(route, params, request, headers, body, state, timer);
        match slab.alloc(fiber) {
            Some(slot_idx) => {
                conn.async_state.pending_wake = Some((ROUTE_ID, slot_idx));
                ConsumeOutcome::Park {
                    consumed: total,
                    close: conn_close,
                }
            }
            None => ConsumeOutcome::Close(crate::CANNED_503),
        }
    }
}
