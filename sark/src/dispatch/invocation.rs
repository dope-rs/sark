use std::ops::Range;
use std::pin::Pin;

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
        R: RouteSpec + manifold::Route<S> + 'static,
    {
        if matches!(R::BODY_POLICY, service::BodyPolicy::Discarded) {
            self.discard(permit, matched, state)
        } else {
            self.buffered(permit, matched, state)
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
        } = match Framing::<R>::from_ctx(self.ctx) {
            Ok(framing) => framing,
            Err(RequestErr::NeedMore(state)) => {
                return ConsumeOutcome::NeedMore { permit, state };
            }
            Err(RequestErr::Bad(reason)) => return ConsumeOutcome::Close(reason),
        };
        let req = &self.ctx.req_bytes[..total];
        if let Some(outcome) = ResponseEgress::new(self.write, self.date).cached::<R>(&self.cache) {
            return outcome.into_consume(permit, Consumption::Buffered(total), conn_close);
        }
        if self.parse_query::<R>(&mut raw_headers, req).is_err() {
            return ConsumeOutcome::Close(CANNED_400);
        }
        let body = match chunked_body.as_ref() {
            Some(shared) => shared.as_ref(),
            None => &req[head_len..],
        };
        let invocation = Invocation::new(
            self.ctx.target_off..(self.ctx.target_off + self.ctx.target_len),
            &req[..head_len],
            body,
            body.len(),
        );
        let response = match invocation.invoke::<R, S>(raw_params, raw_headers, state) {
            Ok(response) => response,
            Err(reason) => return ConsumeOutcome::Close(reason),
        };
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
        } = match DiscardFraming::<R>::from_ctx(self.ctx) {
            Ok(framing) => framing,
            Err(RequestErr::NeedMore(state)) => {
                return ConsumeOutcome::NeedMore { permit, state };
            }
            Err(RequestErr::Bad(reason)) => return ConsumeOutcome::Close(reason),
        };
        let head = &self.ctx.req_bytes[..head_len];
        let consumption = Consumption::Discard {
            head: head_len,
            body: body_total,
        };
        if let Some(outcome) = ResponseEgress::new(self.write, self.date).cached::<R>(&self.cache) {
            return outcome.into_consume(permit, consumption, conn_close);
        }
        if self.parse_query::<R>(&mut raw_headers, head).is_err() {
            return ConsumeOutcome::Close(CANNED_400);
        }
        let invocation = Invocation::new(
            self.ctx.target_off..(self.ctx.target_off + self.ctx.target_len),
            head,
            &[],
            body_total,
        );
        let response = match invocation.invoke::<R, S>(raw_params, raw_headers, state) {
            Ok(response) => response,
            Err(reason) => return ConsumeOutcome::Close(reason),
        };
        ResponseEgress::new(self.write, self.date)
            .route::<R>(response, self.cache, self.gzip, accept_gzip)
            .into_consume(permit, consumption, conn_close)
    }

    fn parse_query<R: RouteSpec>(
        &self,
        raw_headers: &mut R::RawHeaders,
        request: &[u8],
    ) -> Result<(), ()> {
        if let Some(query) = self.ctx.query_range.clone() {
            R::Request::parse_query_raw(raw_headers, request, query).map_err(|_| ())?;
        }
        self.ctx.http_method()?;
        Ok(())
    }
}

pub(super) fn stream_response<'req, S: sark_core::http::Shape<'req>>(
    response: S,
    write: &mut [u8],
    date: &[u8; 29],
) -> Result<(usize, S::StreamInner), Outcome> {
    ResponseEgress::new(write, date)
        .stream(response)
        .ok_or(Outcome::Close(CANNED_500))
}

pub(super) struct StreamRoute<'a, 'req> {
    ctx: &'a Ctx<'req>,
    write: &'a mut [u8],
    date: &'a [u8; 29],
    conn: &'a mut ConnState,
}

impl<'a, 'req> StreamRoute<'a, 'req> {
    pub(super) fn new(
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn dispatch<'env, 'd, R, S, T, Tag, Wrap, const N: usize>(
        self,
        permit: DispatchPermit,
        matched: Matched<R>,
        mut tasks: Pin<&mut crate::fiber::FixedSlab<'d, T, N, Tag>>,
        state: &'env S,
        wrap: Wrap,
    ) -> ConsumeOutcome
    where
        R: RouteSpec + manifold::Route<S> + 'static,
        for<'request> R::Response<'request>:
            sark_core::http::Shape<'request, StreamInner = R::Stream>,
        R::Stream: dope_fiber::Fiber<'d, Output = Option<Shared>>,
        T: dope_fiber::Fiber<'d>,
        Wrap: FnOnce(R::Stream) -> T,
    {
        let Matched { raw_params } = matched;
        let Framing {
            mut raw_headers,
            head_len,
            total,
            conn_close,
            chunked_body,
            accept_gzip: _,
        } = match Framing::<R>::from_ctx(self.ctx) {
            Ok(framing) => framing,
            Err(RequestErr::NeedMore(state)) => {
                return ConsumeOutcome::NeedMore { permit, state };
            }
            Err(RequestErr::Bad(reason)) => return ConsumeOutcome::Close(reason),
        };
        let request = &self.ctx.req_bytes[..total];
        if self.parse_query::<R>(&mut raw_headers, request).is_err() {
            return ConsumeOutcome::Close(CANNED_400);
        }
        let body = match chunked_body.as_ref() {
            Some(shared) => shared.as_ref(),
            None => &request[head_len..],
        };
        let Some(entry) = tasks.as_mut().vacant_entry() else {
            return ConsumeOutcome::Close(crate::CANNED_503);
        };
        let invocation = Invocation::new(
            self.ctx.target_off..(self.ctx.target_off + self.ctx.target_len),
            &request[..head_len],
            body,
            body.len(),
        );
        let response = match invocation.invoke::<R, S>(raw_params, raw_headers, state) {
            Ok(response) => response,
            Err(reason) => return ConsumeOutcome::Close(reason),
        };
        let (written, stream) = match stream_response(response, self.write, self.date) {
            Ok(stream) => stream,
            Err(outcome) => {
                return outcome.into_consume(permit, Consumption::Buffered(total), conn_close);
            }
        };
        let task = entry.insert(wrap(stream));
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

    fn parse_query<R: RouteSpec>(
        &self,
        raw_headers: &mut R::RawHeaders,
        request: &[u8],
    ) -> Result<(), ()> {
        if let Some(query) = self.ctx.query_range.clone() {
            R::Request::parse_query_raw(raw_headers, request, query).map_err(|_| ())?;
        }
        self.ctx.http_method()?;
        Ok(())
    }
}
