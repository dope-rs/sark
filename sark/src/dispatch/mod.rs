pub mod conn_state;
pub mod pipeline;
pub mod response_cache;
pub mod routing;

use std::ops::Range;
use std::pin::Pin;
use std::task::Poll;

pub use conn_state::{ConsumeOutcome, Outcome};
use dope::DriverContext;
use dope::manifold::listener;
use dope::manifold::listener::SlotEgress;
use dope_net::link;
use dope_net::wire::Wire;
use o3::buffer::Shared;
pub use pipeline::{Pipeline, identity_mut};
use response_cache::Cache;
pub use routing::Routing;
use sark_core::http::compress::Gzip;
use sark_core::http::{CHUNK_TERMINATOR, FixedResponseInner, OwnedShape, Shape};

use crate::service::{self, RouteRequestImpl, RouteSpec, SlicePath, manifold};
use crate::{CANNED_400, CANNED_500, request};

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
        _method: http::Method,
        path: &[u8],
        headers: &[(&[u8], Range<usize>)],
        head_bytes: &[u8],
        body_bytes: &[u8],
        encoder: &mut E,
    ) -> Decoded;
}

pub trait DecodeRoute<R: RouteSpec, S> {
    #[allow(clippy::too_many_arguments)]
    fn decode<E: ResponseEncoder>(
        route: &R,
        raw_params: R::RawParams,
        raw_headers: R::RawHeaders,
        _method: http::Method,
        head: &[u8],
        body: &[u8],
        state: &S,
        encoder: &mut E,
    ) -> Decoded;
}

impl<R, S> DecodeRoute<R, S> for manifold::Sync
where
    R: RouteSpec + manifold::Route<S> + 'static,
{
    fn decode<E: ResponseEncoder>(
        route: &R,
        raw_params: R::RawParams,
        raw_headers: R::RawHeaders,
        _method: http::Method,
        head: &[u8],
        body: &[u8],
        state: &S,
        encoder: &mut E,
    ) -> Decoded {
        match Pipeline::build_and_invoke::<R, S>(
            route,
            raw_params,
            raw_headers,
            0..0,
            head,
            body,
            body.len(),
            state,
        ) {
            Ok(response) => {
                let static_body = matches!(
                    R::RESPONSE_BODY_KIND,
                    sark_core::http::body_kind::ResponseKind::Static,
                )
                .then(|| Shape::preserialize_static(&response))
                .flatten()
                .map(|(_, _, body)| body);
                ResponseEncoder::emit(
                    encoder,
                    Shape::status(&response),
                    AsRef::as_ref(&Shape::headers_wire(&response)),
                    static_body.unwrap_or_else(|| Shape::body_bytes(&response)),
                );
                Decoded::Emitted
            }
            Err(_) => Decoded::Bad,
        }
    }
}

impl<R: RouteSpec, S> DecodeRoute<R, S> for manifold::NativeFiber {
    fn decode<E: ResponseEncoder>(
        _route: &R,
        _raw_params: R::RawParams,
        _raw_headers: R::RawHeaders,
        _method: http::Method,
        _head: &[u8],
        _body: &[u8],
        _state: &S,
        _encoder: &mut E,
    ) -> Decoded {
        Decoded::Unsupported
    }
}

impl<R: RouteSpec, S> DecodeRoute<R, S> for manifold::NativeStream {
    fn decode<E: ResponseEncoder>(
        _route: &R,
        _raw_params: R::RawParams,
        _raw_headers: R::RawHeaders,
        _method: http::Method,
        _head: &[u8],
        _body: &[u8],
        _state: &S,
        _encoder: &mut E,
    ) -> Decoded {
        Decoded::Unsupported
    }
}

struct RequestDomainInput<R: RouteSpec> {
    storage: request::RequestStorage,
    raw_params: <R as RouteSpec>::RawParams,
    raw_headers: <R as RouteSpec>::RawHeaders,
    target: Range<usize>,
    total: usize,
    conn_close: bool,
}

enum RequestErr {
    NeedMore(conn_state::NeedMore),
    Bad(&'static [u8]),
}

fn assemble_matched<'r, R: RouteSpec>(
    permit: conn_state::DispatchPermit,
    matched: Matched<'r, R>,
    ctx: &Ctx<'_>,
    conn: &mut conn_state::ConnState,
) -> Result<(&'r R, RequestDomainInput<R>), conn_state::ConsumeOutcome> {
    let Matched { route, raw_params } = matched;
    match ctx.assemble_domain::<R>(raw_params, conn) {
        Ok(request) => Ok((route, request)),
        Err(RequestErr::NeedMore(state)) => {
            Err(conn_state::ConsumeOutcome::NeedMore { permit, state })
        }
        Err(RequestErr::Bad(reason)) => Err(conn_state::ConsumeOutcome::Close(reason)),
    }
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

    fn retain_req(
        view: Option<&o3::buffer::Shared>,
        req_bytes: &[u8],
        len: usize,
    ) -> o3::buffer::Shared {
        if let Some(view) = view {
            let base = view.as_slice().as_ptr() as usize;
            if let Some(off) = (req_bytes.as_ptr() as usize).checked_sub(base)
                && off + len <= view.len()
            {
                return view.slice(off..off + len);
            }
        }
        o3::buffer::Shared::copy_from_slice(&req_bytes[..len])
    }

    fn assemble_domain<R: RouteSpec>(
        &self,
        raw_params: <R as RouteSpec>::RawParams,
        conn: &mut conn_state::ConnState,
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
        let retained = Self::retain_req(conn.recv_view.as_ref(), self.req_bytes, retain);
        let req: &[u8] = retained.as_ref();
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
        if self.http_method().is_err() {
            return Err(RequestErr::Bad(CANNED_400));
        }
        let target_range = self.target_off..(self.target_off + self.target_len);
        Ok(RequestDomainInput {
            storage: request::RequestStorage::new(retained, chunked_body, head_len),
            raw_params,
            raw_headers,
            target: target_range,
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
        consumption: conn_state::Consumption,
        conn_close: bool,
    ) -> conn_state::ConsumeOutcome {
        use conn_state::ConsumeOutcome;
        match self {
            response @ (Outcome::Send { .. }
            | Outcome::SendStatic { .. }
            | Outcome::SendSplit { .. }
            | Outcome::SendPooled { .. }) => ConsumeOutcome::Complete {
                permit,
                consumption,
                response,
                conn_close,
            },
            Outcome::Park => match consumption {
                conn_state::Consumption::Buffered(consumed) => ConsumeOutcome::Park {
                    consumed,
                    close: conn_close,
                },
                conn_state::Consumption::Discard { .. } => ConsumeOutcome::Close(CANNED_500),
            },
            Outcome::Close(reason) => ConsumeOutcome::Close(reason),
        }
    }

    pub fn apply<'d, W: Wire, C: Default + 'static>(
        self,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> bool {
        let close_after = match &self {
            Outcome::Park => return true,
            Outcome::Close(reason) => {
                slot.set_close_after();
                if !reason.is_empty() {
                    let buf = aux.write_buf_for(slot);
                    let ud = slot.token();
                    return slot.submit_split_static(buf, 0, reason, ud, driver);
                }
                return true;
            }
            Outcome::Send { close_after, .. }
            | Outcome::SendStatic { close_after, .. }
            | Outcome::SendSplit { close_after, .. }
            | Outcome::SendPooled { close_after, .. } => *close_after,
        };
        if close_after {
            slot.set_close_after();
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
            Outcome::SendPooled {
                hdr_written, body, ..
            } => slot.submit_split_pooled(buf, hdr_written, body, ud, driver),
            Outcome::Park | Outcome::Close(_) => unreachable!(),
        }
    }

    fn write_shape<'req, S: Shape<'req>>(
        resp: S,
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

pub enum TaskPoll {
    Complete,
    Stream(Option<Shared>),
}

pub trait Complete<'d, R, F>: service::manifold::Kind<'d, R, F>
where
    R: RouteSpec,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <Self as service::manifold::Kind<'d, R, F>>::Output,
        slot: &mut link::slot::Slot<'a, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'a>,
        date: &[u8; 29],
        close: bool,
    ) -> TaskPoll;
}

impl<'d, R: RouteSpec, F> Complete<'d, R, F> for service::manifold::Sync {
    fn complete<'a, W: Wire, C: Default + 'static>(
        _output: <Self as service::manifold::Kind<'d, R, F>>::Output,
        _slot: &mut link::slot::Slot<'a, W, listener::State<C>>,
        _aux: &mut listener::Aux,
        _driver: &mut DriverContext<'_, 'a>,
        _date: &[u8; 29],
        _close: bool,
    ) -> TaskPoll {
        unreachable!()
    }
}

impl<'d, R, F> Complete<'d, R, F> for service::manifold::NativeFiber
where
    R: RouteSpec,
    F: dope_fiber::Fiber<'d, Output = R::AsyncResponse> + 'd,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <Self as service::manifold::Kind<'d, R, F>>::Output,
        slot: &mut link::slot::Slot<'a, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'a>,
        date: &[u8; 29],
        close: bool,
    ) -> TaskPoll {
        Pipeline::finish_pending::<R, W, C>(output, slot, aux, driver, date, close);
        TaskPoll::Complete
    }
}

impl<'d, R, F> Complete<'d, R, F> for service::manifold::NativeStream
where
    R: RouteSpec,
    R::Stream: dope_fiber::Fiber<'d, Output = Option<Shared>> + 'd,
{
    fn complete<'a, W: Wire, C: Default + 'static>(
        output: <Self as service::manifold::Kind<'d, R, F>>::Output,
        _slot: &mut link::slot::Slot<'a, W, listener::State<C>>,
        _aux: &mut listener::Aux,
        _driver: &mut DriverContext<'_, 'a>,
        _date: &[u8; 29],
        _close: bool,
    ) -> TaskPoll {
        TaskPoll::Stream(output)
    }
}

pub trait Dispatch<'d, R, S, F>
where
    R: RouteSpec,
{
    #[allow(clippy::too_many_arguments)]
    fn dispatch<T, Tag, MK, Wrap, const N: usize>(
        permit: conn_state::DispatchPermit,
        matched: Matched<'d, R>,
        tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        state: &'d S,
        ctx: &Ctx<'_>,
        timer: &'d crate::Timer<'d>,
        conn: &mut conn_state::ConnState,
        date: &[u8; 29],
        cache: Cache<'_>,
        gzip: &mut Gzip,
        write: &mut [u8],
        make: MK,
        wrap: Wrap,
    ) -> conn_state::ConsumeOutcome
    where
        T: dope_fiber::Fiber<'d> + 'd,
        MK: FnOnce(
            &'d R,
            <R as RouteSpec>::Params<'d>,
            request::Ref<'d>,
            <R as RouteSpec>::Headers<'d>,
            R::ParsedBody<'d>,
            &'d S,
            &'d crate::Timer<'d>,
        ) -> F,
        Wrap: FnOnce(
            <Self as service::manifold::Kind<'d, R, F>>::Task,
            <Self as service::manifold::Kind<'d, R, F>>::Owner,
        ) -> T,
        Self: service::manifold::Kind<'d, R, F>;
}

impl<'d, R, S, F> Dispatch<'d, R, S, F> for service::manifold::Sync
where
    R: RouteSpec + manifold::Route<S> + 'static,
{
    fn dispatch<T, Tag, MK, Wrap, const N: usize>(
        permit: conn_state::DispatchPermit,
        matched: Matched<'d, R>,
        _tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        state: &'d S,
        ctx: &Ctx<'_>,
        _timer: &'d crate::Timer<'d>,
        _conn: &mut conn_state::ConnState,
        date: &[u8; 29],
        cache: Cache<'_>,
        gzip: &mut Gzip,
        write: &mut [u8],
        _make: MK,
        _wrap: Wrap,
    ) -> conn_state::ConsumeOutcome
    where
        T: dope_fiber::Fiber<'d> + 'd,
        MK: FnOnce(
            &'d R,
            <R as RouteSpec>::Params<'d>,
            request::Ref<'d>,
            <R as RouteSpec>::Headers<'d>,
            R::ParsedBody<'d>,
            &'d S,
            &'d crate::Timer<'d>,
        ) -> F,
        Wrap: FnOnce(
            <Self as service::manifold::Kind<'d, R, F>>::Task,
            <Self as service::manifold::Kind<'d, R, F>>::Owner,
        ) -> T,
        Self: service::manifold::Kind<'d, R, F>,
    {
        Pipeline::route_manifold(permit, matched, state, ctx, (date, cache, gzip, write))
    }
}

impl<'d, R, S, F> Dispatch<'d, R, S, F> for service::manifold::NativeFiber
where
    R: RouteSpec + 'static,
    S: 'd,
    F: dope_fiber::Fiber<'d, Output = R::AsyncResponse> + 'd,
    service::manifold::NativeFiber:
        service::manifold::Kind<'d, R, F, Task = F, Owner = request::RequestStorage>,
{
    fn dispatch<T, Tag, MK, Wrap, const N: usize>(
        permit: conn_state::DispatchPermit,
        matched: Matched<'d, R>,
        mut tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        state: &'d S,
        ctx: &Ctx<'_>,
        timer: &'d crate::Timer<'d>,
        conn: &mut conn_state::ConnState,
        _date: &[u8; 29],
        _cache: Cache<'_>,
        _gzip: &mut Gzip,
        _write: &mut [u8],
        make: MK,
        wrap: Wrap,
    ) -> conn_state::ConsumeOutcome
    where
        T: dope_fiber::Fiber<'d> + 'd,
        MK: FnOnce(
            &'d R,
            <R as RouteSpec>::Params<'d>,
            request::Ref<'d>,
            <R as RouteSpec>::Headers<'d>,
            R::ParsedBody<'d>,
            &'d S,
            &'d crate::Timer<'d>,
        ) -> F,
        Wrap: FnOnce(
            <Self as service::manifold::Kind<'d, R, F>>::Task,
            <Self as service::manifold::Kind<'d, R, F>>::Owner,
        ) -> T,
        Self: service::manifold::Kind<'d, R, F>,
    {
        let (
            route,
            RequestDomainInput {
                storage,
                raw_params,
                raw_headers,
                target,
                total,
                conn_close,
            },
        ) = match assemble_matched(permit, matched, ctx, conn) {
            Ok(request) => request,
            Err(outcome) => return outcome,
        };
        let (head, body) = unsafe { storage.task_views() };
        let request = request::Ref::<'d>::from_slice(target, head, body);
        let Some(params) =
            <<R as RouteSpec>::Request as RouteRequestImpl>::build_params(&request, raw_params)
        else {
            return conn_state::ConsumeOutcome::Close(CANNED_400);
        };
        let headers = match <<R as RouteSpec>::Request as RouteRequestImpl>::build_headers(
            &request,
            raw_headers,
        ) {
            Ok(headers) => headers,
            Err(_) => return conn_state::ConsumeOutcome::Close(CANNED_400),
        };
        let body = match <R as RouteSpec>::parse_body(body) {
            Ok(body) => body,
            Err(_) => return conn_state::ConsumeOutcome::Close(CANNED_400),
        };
        let Some(entry) = tasks.as_mut().vacant_entry() else {
            return conn_state::ConsumeOutcome::Close(crate::CANNED_503);
        };
        let task = wrap(
            make(route, params, request, headers, body, state, timer),
            storage,
        );
        let task = entry.insert(task);
        conn.async_state.task = Some(task.erase());
        conn.async_state.task_stream = false;
        conn_state::ConsumeOutcome::Park {
            consumed: total,
            close: conn_close,
        }
    }
}

impl<'d, R, S, F> Dispatch<'d, R, S, F> for service::manifold::NativeStream
where
    R: RouteSpec + manifold::Route<S> + 'static,
    for<'req> R::Response<'req>: Shape<'req, StreamInner = R::Stream>,
    R::Stream: dope_fiber::Fiber<'d, Output = Option<Shared>> + 'd,
    S: 'd,
    service::manifold::NativeStream:
        service::manifold::Kind<'d, R, F, Task = R::Stream, Owner = ()>,
{
    fn dispatch<T, Tag, MK, Wrap, const N: usize>(
        permit: conn_state::DispatchPermit,
        matched: Matched<'d, R>,
        tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        state: &'d S,
        ctx: &Ctx<'_>,
        _timer: &'d crate::Timer<'d>,
        conn: &mut conn_state::ConnState,
        date: &[u8; 29],
        _cache: Cache<'_>,
        _gzip: &mut Gzip,
        write: &mut [u8],
        _make: MK,
        wrap: Wrap,
    ) -> conn_state::ConsumeOutcome
    where
        T: dope_fiber::Fiber<'d> + 'd,
        MK: FnOnce(
            &'d R,
            <R as RouteSpec>::Params<'d>,
            request::Ref<'d>,
            <R as RouteSpec>::Headers<'d>,
            R::ParsedBody<'d>,
            &'d S,
            &'d crate::Timer<'d>,
        ) -> F,
        Wrap: FnOnce(
            <Self as service::manifold::Kind<'d, R, F>>::Task,
            <Self as service::manifold::Kind<'d, R, F>>::Owner,
        ) -> T,
        Self: service::manifold::Kind<'d, R, F>,
    {
        Pipeline::route_native_stream(
            permit,
            matched,
            tasks,
            state,
            ctx,
            write,
            date,
            conn,
            |task| wrap(task, ()),
        )
    }
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

struct DiscardFraming<R: RouteSpec> {
    raw_headers: <R as RouteSpec>::RawHeaders,
    head_len: usize,
    body_total: usize,
    conn_close: bool,
    accept_gzip: bool,
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
                Framed::NeedMore => return Err(RequestErr::NeedMore(conn_state::NeedMore::Head)),
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

impl<R: RouteSpec> DiscardFraming<R> {
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
        Ok(DiscardFraming {
            raw_headers: base.raw_headers,
            head_len: base.head_len,
            body_total,
            conn_close: base.conn_close,
            accept_gzip: base.accept_gzip,
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
                    return Err(RequestErr::NeedMore(conn_state::NeedMore::FixedBody(total)));
                }
                (total, None)
            }
            BodyFraming::Chunked => {
                if base.is_bodyless_method {
                    return Err(RequestErr::Bad(CANNED_400));
                }
                let chunked_section = &req_bytes[head_len..];
                match Parse::chunked_body_consumed(chunked_section, <R as RouteSpec>::MAX_BODY) {
                    Ok(None) => {
                        return Err(RequestErr::NeedMore(conn_state::NeedMore::ChunkedBody));
                    }
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
    fn try_static_response_hit<R: RouteSpec>(
        write: &mut [u8],
        date: &[u8; 29],
        cache: &Cache<'_>,
    ) -> Option<Outcome> {
        if !<R as RouteSpec>::STATIC_RESPONSE {
            return None;
        }
        Some(match cache.write(write, date)? {
            response_cache::Cached::Fixed { written } => Outcome::Send {
                written,
                close_after: false,
            },
            response_cache::Cached::Static { hdr_written, body } => Outcome::SendStatic {
                hdr_written,
                body,
                close_after: false,
            },
        })
    }

    fn finish_sync<R: RouteSpec>(
        resp: <R as RouteSpec>::Response<'_>,
        write: &mut [u8],
        date: &[u8; 29],
        cache: Cache<'_>,
        gzip: &mut Gzip,
        accept_gzip: bool,
    ) -> Outcome {
        use sark_core::http::body_kind::ResponseKind;

        let static_body = matches!(<R as RouteSpec>::RESPONSE_BODY_KIND, ResponseKind::Static);
        if accept_gzip
            && !<R as RouteSpec>::STATIC_RESPONSE
            && matches!(<R as RouteSpec>::RESPONSE_BODY_KIND, ResponseKind::Inline)
            && let Some(plain) = Shape::body_for_gzip(&resp)
            && !plain.is_empty()
            && let Some(compressed) = gzip.encode(plain)
        {
            let body_len = compressed.len();
            return match Shape::write_gzip_head(resp, write, date, body_len) {
                Some(hdr_written) => Outcome::SendPooled {
                    hdr_written,
                    body: compressed,
                    close_after: false,
                },
                None => Outcome::Close(CANNED_500),
            };
        }
        if <R as RouteSpec>::STATIC_RESPONSE && static_body {
            return match Shape::preserialize_static(&resp) {
                Some((mut head_template, date_offset, body)) => {
                    let date_offset = sark_core::http::apply_head_skip(
                        &mut head_template,
                        date_offset,
                        <R as RouteSpec>::EMIT_DATE,
                        <R as RouteSpec>::EMIT_SERVER,
                    );
                    let hdr = FixedResponseInner::write_preserialized(
                        write,
                        &head_template,
                        date_offset,
                        date,
                    );
                    cache.insert_static(head_template, date_offset, body);
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
            let (mut template, date_offset) = Shape::preserialize(&resp);
            let date_offset = sark_core::http::apply_head_skip(
                &mut template,
                date_offset,
                <R as RouteSpec>::EMIT_DATE,
                <R as RouteSpec>::EMIT_SERVER,
            );
            let n = FixedResponseInner::write_preserialized(write, &template, date_offset, date);
            cache.insert_fixed(template, date_offset);
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
        Outcome::write_shape(resp, write, date, false)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_and_invoke<'req, 'a, R, S>(
        route: &'a R,
        raw_params: <R as RouteSpec>::RawParams,
        raw_headers: <R as RouteSpec>::RawHeaders,
        target_range: Range<usize>,
        head_bytes: &'req [u8],
        body_bytes: &'req [u8],
        declared_body_len: usize,
        state: &'a S,
    ) -> Result<<R as RouteSpec>::Response<'req>, &'static [u8]>
    where
        R: RouteSpec + manifold::Route<S> + 'static,
        'req: 'a,
    {
        let mut request_ref_bare =
            request::Ref::<'_>::from_slice(target_range, head_bytes, body_bytes);
        request_ref_bare.set_declared_body_len(declared_body_len);
        let Some(params) = <<R as RouteSpec>::Request as RouteRequestImpl>::build_params(
            &request_ref_bare,
            raw_params,
        ) else {
            return Err(CANNED_400);
        };
        let headers = match <<R as RouteSpec>::Request as RouteRequestImpl>::build_headers(
            &request_ref_bare,
            raw_headers,
        ) {
            Ok(h) => h,
            Err(_) => return Err(CANNED_400),
        };
        let parsed_body = match <R as RouteSpec>::parse_body(body_bytes) {
            Ok(b) => b,
            Err(_) => return Err(CANNED_400),
        };
        Ok(<R as manifold::Route<S>>::invoke(
            route,
            params,
            &request_ref_bare,
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
        response: (&[u8; 29], Cache<'_>, &mut Gzip, &mut [u8]),
    ) -> conn_state::ConsumeOutcome
    where
        R: RouteSpec + manifold::Route<S> + 'static,
    {
        let (date, cache, gzip, write) = response;
        if matches!(
            <R as RouteSpec>::BODY_POLICY,
            service::BodyPolicy::Discarded
        ) {
            return Self::route_manifold_discard::<R, S>(
                permit,
                matched,
                state,
                ctx,
                (date, cache, gzip, write),
            );
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
            Err(RequestErr::NeedMore(state)) => {
                return ConsumeOutcome::NeedMore { permit, state };
            }
            Err(RequestErr::Bad(reason)) => return ConsumeOutcome::Close(reason),
        };
        let req = &ctx.req_bytes[..total];
        if let Some(out) = Self::try_static_response_hit::<R>(write, date, &cache) {
            return out.into_consume(permit, conn_state::Consumption::Buffered(total), conn_close);
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
        if ctx.http_method().is_err() {
            return ConsumeOutcome::Close(CANNED_400);
        }
        let target_range = ctx.target_off..(ctx.target_off + ctx.target_len);
        let out = match Self::build_and_invoke::<R, S>(
            route,
            raw_params,
            raw_headers,
            target_range,
            &req[..head_len],
            body_bytes,
            body_bytes.len(),
            state,
        ) {
            Ok(resp) => Self::finish_sync::<R>(resp, write, date, cache, gzip, accept_gzip),
            Err(reason) => return ConsumeOutcome::Close(reason),
        };
        out.into_consume(permit, conn_state::Consumption::Buffered(total), conn_close)
    }

    fn route_manifold_discard<R, S>(
        permit: conn_state::DispatchPermit,
        matched: Matched<'_, R>,
        state: &S,
        ctx: &Ctx<'_>,
        response: (&[u8; 29], Cache<'_>, &mut Gzip, &mut [u8]),
    ) -> conn_state::ConsumeOutcome
    where
        R: RouteSpec + manifold::Route<S> + 'static,
    {
        use conn_state::ConsumeOutcome;
        let (date, cache, gzip, write) = response;
        let Matched { route, raw_params } = matched;
        let DiscardFraming {
            mut raw_headers,
            head_len,
            body_total,
            conn_close,
            accept_gzip,
        } = match DiscardFraming::<R>::from_ctx(ctx) {
            Ok(x) => x,
            Err(RequestErr::NeedMore(state)) => {
                return ConsumeOutcome::NeedMore { permit, state };
            }
            Err(RequestErr::Bad(reason)) => return ConsumeOutcome::Close(reason),
        };
        let req_head = &ctx.req_bytes[..head_len];
        let consumption = conn_state::Consumption::Discard {
            head: head_len,
            body: body_total,
        };
        if let Some(out) = Self::try_static_response_hit::<R>(write, date, &cache) {
            return out.into_consume(permit, consumption, conn_close);
        }
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
        if ctx.http_method().is_err() {
            return ConsumeOutcome::Close(CANNED_400);
        }
        let target_range = ctx.target_off..(ctx.target_off + ctx.target_len);
        let out = match Self::build_and_invoke::<R, S>(
            route,
            raw_params,
            raw_headers,
            target_range,
            req_head,
            &[],
            body_total,
            state,
        ) {
            Ok(resp) => Self::finish_sync::<R>(resp, write, date, cache, gzip, accept_gzip),
            Err(reason) => return ConsumeOutcome::Close(reason),
        };
        out.into_consume(permit, consumption, conn_close)
    }

    pub fn finish_pending<'d, R: RouteSpec, W: Wire, C: Default + 'static>(
        resp: <R as RouteSpec>::AsyncResponse,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        date: &[u8; 29],
        close: bool,
    ) {
        use sark_core::http::body_kind::ResponseKind;
        if matches!(<R as RouteSpec>::RESPONSE_BODY_KIND, ResponseKind::Stream) {
            unreachable!("finish_pending: Stream routes go through finish_pending_stream");
        }
        let resp = resp.into_shape();
        let outcome = {
            let mut write = aux.write_buf_for(slot);
            Outcome::write_shape(resp, &mut write, date, close)
        };
        outcome.apply(slot, aux, driver);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn route_native_stream<'d, R, S, T, Tag, Wrap, const N: usize>(
        permit: conn_state::DispatchPermit,
        matched: Matched<'d, R>,
        mut tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        state: &'d S,
        ctx: &Ctx<'_>,
        write: &mut [u8],
        date: &[u8; 29],
        conn: &mut conn_state::ConnState,
        wrap: Wrap,
    ) -> conn_state::ConsumeOutcome
    where
        R: RouteSpec + manifold::Route<S> + 'static,
        for<'req> R::Response<'req>: Shape<'req, StreamInner = R::Stream>,
        R::Stream: dope_fiber::Fiber<'d, Output = Option<Shared>> + 'd,
        T: dope_fiber::Fiber<'d> + 'd,
        Wrap: FnOnce(R::Stream) -> T,
    {
        use conn_state::ConsumeOutcome;
        let Matched { route, raw_params } = matched;
        let Framing {
            mut raw_headers,
            head_len,
            total,
            conn_close,
            chunked_body,
            accept_gzip: _,
        } = match Framing::<R>::from_ctx(ctx) {
            Ok(framing) => framing,
            Err(RequestErr::NeedMore(state)) => {
                return ConsumeOutcome::NeedMore { permit, state };
            }
            Err(RequestErr::Bad(reason)) => return ConsumeOutcome::Close(reason),
        };
        let req = &ctx.req_bytes[..total];
        if let Some(query) = ctx.query_range.clone()
            && <<R as RouteSpec>::Request as RouteRequestImpl>::parse_query_raw(
                &mut raw_headers,
                req,
                query,
            )
            .is_err()
        {
            return ConsumeOutcome::Close(CANNED_400);
        }
        let body = match chunked_body.as_ref() {
            Some(shared) => shared.as_ref(),
            None => &req[head_len..],
        };
        if ctx.http_method().is_err() {
            return ConsumeOutcome::Close(CANNED_400);
        }
        let target = ctx.target_off..(ctx.target_off + ctx.target_len);
        let Some(entry) = tasks.as_mut().vacant_entry() else {
            return ConsumeOutcome::Close(crate::CANNED_503);
        };
        let response = match Self::build_and_invoke::<R, S>(
            route,
            raw_params,
            raw_headers,
            target,
            &req[..head_len],
            body,
            body.len(),
            state,
        ) {
            Ok(response) => response,
            Err(reason) => return ConsumeOutcome::Close(reason),
        };
        let Some((written, stream)) = Shape::write_head_stream(response, write, date) else {
            return ConsumeOutcome::Close(CANNED_500);
        };
        let task = entry.insert(wrap(stream));
        conn.async_state.task = Some(task.erase());
        conn.async_state.task_stream = true;
        conn.async_state.stream_phase = conn_state::StreamPhase::Streaming;
        conn.async_state.stream_pending = None;
        ConsumeOutcome::Streamed {
            consumed: total,
            written,
            close: conn_close,
        }
    }

    pub fn task_poll_proj<'d, T, Tag, W, C, PJ, Classify, const N: usize>(
        mut tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        project: PJ,
        date: &[u8; 29],
        mut classify: Classify,
    ) -> usize
    where
        T: dope_fiber::Fiber<'d> + 'd,
        W: Wire,
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState,
        Classify: FnMut(
            T::Output,
            &mut link::slot::Slot<'d, W, listener::State<C>>,
            &mut listener::Aux,
            &mut DriverContext<'_, 'd>,
            &[u8; 29],
            bool,
        ) -> TaskPoll,
    {
        use conn_state::StreamPhase;
        let conn_ptr: *mut conn_state::ConnState = project(&mut slot.state.conn);
        let conn = unsafe { &mut *conn_ptr };
        let Some(task) = conn.async_state.task.take() else {
            return 0;
        };
        let task = crate::fiber::TaskId::<Tag>::from_erased(task);
        let mut cursor = 0;
        loop {
            let (framed, terminating) = match conn.async_state.stream_pending.take() {
                Some(stashed) => (
                    stashed,
                    conn.async_state.stream_phase == StreamPhase::Terminating,
                ),
                None => match conn.async_state.stream_phase {
                    StreamPhase::Terminating => (Shared::from_static(CHUNK_TERMINATOR), true),
                    StreamPhase::Streaming => {
                        let poll = {
                            let mut context = std::pin::pin!(dope_fiber::Context::from_ready(
                                slot.driver(),
                                slot.ready_key(),
                                driver.reborrow(),
                            ));
                            tasks.as_mut().poll(&task, context.as_mut())
                        };
                        let Some(poll) = poll else {
                            debug_assert!(false, "live task must exist in fiber slab");
                            conn.async_state.task_stream = false;
                            conn.recv.unfreeze();
                            if conn.deferred_close {
                                slot.set_close_after();
                            }
                            return 0;
                        };
                        match poll {
                            Poll::Pending => {
                                conn.async_state.task = Some(task.erase());
                                return cursor;
                            }
                            Poll::Ready(output) => {
                                match classify(output, slot, aux, driver, date, conn.deferred_close)
                                {
                                    TaskPoll::Complete => {
                                        let removed = tasks.as_mut().remove(task);
                                        debug_assert!(removed, "live task must be removable");
                                        conn.async_state.task_stream = false;
                                        conn.recv.unfreeze();
                                        if conn.deferred_close {
                                            slot.set_close_after();
                                        }
                                        return 0;
                                    }
                                    TaskPoll::Stream(Some(raw)) => {
                                        if raw.is_empty() {
                                            continue;
                                        }
                                        (sark_core::http::codec::Wire::chunk_frame(raw), false)
                                    }
                                    TaskPoll::Stream(None) => {
                                        conn.async_state.stream_phase = StreamPhase::Terminating;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                },
            };
            let capacity = aux.write_buf_for(slot).len();
            if capacity.saturating_sub(cursor) < framed.len() {
                if framed.len() > capacity {
                    let buffer = aux.write_buf_for(slot);
                    let token = slot.token();
                    slot.submit_split_shared(buffer, cursor, framed, token, driver);
                    if terminating {
                        let removed = tasks.as_mut().remove(task);
                        debug_assert!(removed, "live task must be removable");
                        conn.async_state.task_stream = false;
                        conn.async_state.stream_phase = StreamPhase::Streaming;
                        conn.recv.unfreeze();
                        if conn.deferred_close {
                            slot.set_close_after();
                        }
                    } else {
                        conn.async_state.task = Some(task.erase());
                    }
                    return 0;
                }
                conn.async_state.task = Some(task.erase());
                conn.async_state.stream_pending = Some(framed);
                return cursor;
            }
            let end = cursor + framed.len();
            aux.write_buf_for(slot)[cursor..end].copy_from_slice(framed.as_ref());
            cursor = end;
            if terminating {
                let removed = tasks.as_mut().remove(task);
                debug_assert!(removed, "live task must be removable");
                conn.async_state.task_stream = false;
                conn.async_state.stream_phase = StreamPhase::Streaming;
                conn.recv.unfreeze();
                if conn.deferred_close {
                    slot.set_close_after();
                }
                return cursor;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reborrow_write_buf<'d, 'a, W: Wire, C: Default + 'static>(
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        aux: &'a mut listener::Aux,
    ) -> listener::WriteBuf<'a> {
        aux.write_buf_for(slot)
    }
}

pub trait H1Project<'d, W: Wire> {
    fn chunk_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        bytes: &[u8],
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
        project: PJ,
    ) -> bool
    where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;

    fn send_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        project: PJ,
        sent: usize,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;

    fn activate_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        project: PJ,
        aux: &mut listener::Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;

    fn close_proj<C, PJ>(
        self: Pin<&mut Self>,
        slot: &mut link::slot::Slot<'d, W, listener::State<C>>,
        project: PJ,
        aux: &mut listener::Aux,
    ) where
        C: Default + 'static,
        PJ: Fn(&mut C) -> &mut conn_state::ConnState;
}
