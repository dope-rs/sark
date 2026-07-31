use std::ops::Range;
use std::pin::Pin;

use dope_fiber::abi::Fiber;
use o3::buffer::Shared;
use sark_core::http::compress::Gzip;

use super::conn_state::{
    ConnState, ConsumeOutcome, Consumption, DispatchPermit, Outcome, StreamPhase,
};
use super::egress::ResponseEgress;
use super::requests::{Ctx, DiscardFraming, Framing, Matched, RequestErr};
use super::response_cache::Cache;
use crate::request;
use crate::service::{self, RouteRequestImpl, RouteSpec, manifold};
use crate::{CANNED_400, CANNED_500};

pub struct Invocation<'req> {
    target: Range<usize>,
    head: &'req [u8],
    body: &'req [u8],
    declared_body_len: usize,
}

impl<'req> Invocation<'req> {
    pub fn new(
        target: Range<usize>,
        head: &'req [u8],
        body: &'req [u8],
        declared_body_len: usize,
    ) -> Self {
        Self {
            target,
            head,
            body,
            declared_body_len,
        }
    }

    pub fn invoke<'a, R, S>(
        self,
        raw_params: R::RawParams,
        raw_headers: R::RawHeaders,
        state: &'a S,
    ) -> Result<R::Response<'req>, &'static [u8]>
    where
        R: RouteSpec + manifold::Route<S> + 'static,
        'req: 'a,
    {
        let mut request = request::Ref::from_slice(self.target, self.head, self.body);
        request.set_declared_body_len(self.declared_body_len);
        let Some(params) = R::Request::build_params(&request, raw_params) else {
            return Err(CANNED_400);
        };
        let headers = R::Request::build_headers(&request, raw_headers).map_err(|_| CANNED_400)?;
        let body = R::parse_body(self.body).map_err(|_| CANNED_400)?;
        Ok(R::invoke(params, &request, headers, body, state))
    }
}

macro_rules! framing_or_return {
    ($permit:ident, $result:expr) => {
        match $result {
            Ok(framing) => framing,
            Err(RequestErr::NeedMore(state)) => {
                return ConsumeOutcome::NeedMore {
                    permit: $permit,
                    state,
                };
            }
            Err(RequestErr::Bad(reason)) => return ConsumeOutcome::Close(reason),
        }
    };
}

macro_rules! parse_query_or_return {
    ($ctx:expr, $route:ty, $raw_headers:expr, $request:expr) => {
        if $ctx.parse_query::<$route>($raw_headers, $request).is_err() {
            return ConsumeOutcome::Close(CANNED_400);
        }
    };
}

macro_rules! buffered_body {
    ($chunked_body:expr, $request:expr, $head_len:expr) => {
        match $chunked_body.as_ref() {
            Some(shared) => shared.as_ref(),
            None => &$request[$head_len..],
        }
    };
}

macro_rules! invoke_route_or_return {
    (
        $ctx:expr, $route:ty, $state_ty:ty;
        $head:expr, $body:expr, $declared_body_len:expr;
        $raw_params:expr, $raw_headers:expr, $state:expr
    ) => {{
        let invocation = Invocation::new(
            $ctx.target_off..($ctx.target_off + $ctx.target_len),
            $head,
            $body,
            $declared_body_len,
        );
        match invocation.invoke::<$route, $state_ty>($raw_params, $raw_headers, $state) {
            Ok(response) => response,
            Err(reason) => return ConsumeOutcome::Close(reason),
        }
    }};
}

pub struct SyncRoute<'a, 'req, 'cache> {
    ctx: &'a Ctx<'req>,
    date: &'a [u8; 29],
    cache: Cache<'cache>,
    gzip: &'a mut Gzip,
    write: &'a mut [u8],
}

impl<'a, 'req, 'cache> SyncRoute<'a, 'req, 'cache> {
    pub fn new(
        ctx: &'a Ctx<'req>,
        date: &'a [u8; 29],
        cache: Cache<'cache>,
        gzip: &'a mut Gzip,
        write: &'a mut [u8],
    ) -> Self {
        Self {
            ctx,
            date,
            cache,
            gzip,
            write,
        }
    }

    pub fn dispatch<R, S>(
        self,
        permit: DispatchPermit,
        matched: Matched<R>,
        state: &S,
    ) -> ConsumeOutcome
    where
        R: RouteSpec<Kind = manifold::Sync> + manifold::Route<S> + 'static,
    {
        match R::BODY_POLICY {
            service::BodyPolicy::Buffered => self.buffered(permit, matched, state),
            service::BodyPolicy::Discarded => self.discard(permit, matched, state),
        }
    }

    fn buffered<R, S>(
        self,
        permit: DispatchPermit,
        matched: Matched<R>,
        state: &S,
    ) -> ConsumeOutcome
    where
        R: RouteSpec + manifold::Route<S> + 'static,
    {
        let Matched { raw_params } = matched;
        let Framing {
            mut raw_headers,
            head_len,
            total,
            conn_close,
            chunked_body,
            accept_gzip,
        } = framing_or_return!(permit, Framing::<R>::from_ctx(self.ctx));
        let req = &self.ctx.req_bytes[..total];
        if let Some(outcome) = ResponseEgress::new(self.write, self.date).cached::<R>(&self.cache) {
            return outcome.into_consume(permit, Consumption::Buffered(total), conn_close);
        }
        parse_query_or_return!(self.ctx, R, &mut raw_headers, req);
        let body = buffered_body!(chunked_body, req, head_len);
        let response = invoke_route_or_return!(
            self.ctx, R, S;
            &req[..head_len], body, body.len();
            raw_params, raw_headers, state
        );
        ResponseEgress::new(self.write, self.date)
            .route::<R>(response, self.cache, self.gzip, accept_gzip)
            .into_consume(permit, Consumption::Buffered(total), conn_close)
    }

    fn discard<R, S>(self, permit: DispatchPermit, matched: Matched<R>, state: &S) -> ConsumeOutcome
    where
        R: RouteSpec + manifold::Route<S> + 'static,
    {
        let Matched { raw_params } = matched;
        let DiscardFraming {
            mut raw_headers,
            head_len,
            body_total,
            conn_close,
            accept_gzip,
        } = framing_or_return!(permit, DiscardFraming::<R>::from_ctx(self.ctx));
        let head = &self.ctx.req_bytes[..head_len];
        let consumption = Consumption::Discard {
            head: head_len,
            body: body_total,
        };
        if let Some(outcome) = ResponseEgress::new(self.write, self.date).cached::<R>(&self.cache) {
            return outcome.into_consume(permit, consumption, conn_close);
        }
        parse_query_or_return!(self.ctx, R, &mut raw_headers, head);
        let response = invoke_route_or_return!(
            self.ctx, R, S;
            head, &[], body_total;
            raw_params, raw_headers, state
        );
        ResponseEgress::new(self.write, self.date)
            .route::<R>(response, self.cache, self.gzip, accept_gzip)
            .into_consume(permit, consumption, conn_close)
    }
}

pub(super) fn stream_response<'req, S: sark_core::http::Shape<'req>>(
    response: S,
    write: &mut [u8],
    date: &[u8; 29],
) -> Result<(usize, sark_core::http::ShapeStream<'req, S>), Outcome> {
    ResponseEgress::new(write, date)
        .stream(response)
        .ok_or(Outcome::Close(CANNED_500))
}

pub struct StreamRoute<'a, 'req> {
    ctx: &'a Ctx<'req>,
    write: &'a mut [u8],
    date: &'a [u8; 29],
    conn: &'a mut ConnState,
}

impl<'a, 'req> StreamRoute<'a, 'req> {
    pub fn new(
        ctx: &'a Ctx<'req>,
        write: &'a mut [u8],
        date: &'a [u8; 29],
        conn: &'a mut ConnState,
    ) -> Self {
        Self {
            ctx,
            write,
            date,
            conn,
        }
    }

    pub fn dispatch<'env, 'd, R, S, Tag, const N: usize>(
        self,
        permit: DispatchPermit,
        matched: Matched<R>,
        mut tasks: Pin<&mut crate::fiber::FixedSlab<'d, R::Stream, N, Tag>>,
        state: &'env S,
    ) -> ConsumeOutcome
    where
        R: RouteSpec<Kind = manifold::NativeStream> + manifold::Route<S> + 'static,
        for<'request> <R::Response<'request> as sark_core::http::Shape<'request>>::Metadata:
            sark_core::http::ShapeMetadata<Stream = R::Stream>,
        R::Stream: Fiber<'d, Output = Option<Shared>>,
    {
        let Matched { raw_params } = matched;
        let Framing {
            mut raw_headers,
            head_len,
            total,
            conn_close,
            chunked_body,
            accept_gzip: _,
        } = framing_or_return!(permit, Framing::<R>::from_ctx(self.ctx));
        let request = &self.ctx.req_bytes[..total];
        parse_query_or_return!(self.ctx, R, &mut raw_headers, request);
        let body = buffered_body!(chunked_body, request, head_len);
        let Some(entry) = tasks.as_mut().vacant_entry() else {
            return ConsumeOutcome::Close(crate::CANNED_503);
        };
        let response = invoke_route_or_return!(
            self.ctx, R, S;
            &request[..head_len], body, body.len();
            raw_params, raw_headers, state
        );
        let (written, stream) = match stream_response(response, self.write, self.date) {
            Ok(stream) => stream,
            Err(outcome) => {
                return outcome.into_consume(permit, Consumption::Buffered(total), conn_close);
            }
        };
        let task = entry.insert(stream);
        self.conn.async_state.task = Some(task.erase());
        self.conn.async_state.task_stream = true;
        self.conn.async_state.stream_phase = StreamPhase::Streaming;
        self.conn.async_state.stream_pending = None;
        ConsumeOutcome::Streamed {
            consumed: total,
            written,
            close: conn_close,
        }
    }
}
