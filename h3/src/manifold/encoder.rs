//! Fiber-free HTTP/3 response encoding over the shared request body model.

use std::{convert::Infallible, ops::ControlFlow};

use dope_quic::wire::stream::Id as StreamId;
use o3::buffer::{
    bytes::scope,
    hold,
    hold::evidence::{Mode, Off, On},
    resident,
    storage::Shared,
};
use sark_core::{decode, life, manifold::task::response, request, request::frame::proof};
use sark_http::{
    field::{Response, metadata::section::rules},
    owner::{evidence, evidence::Sink as _, handoff, handoff::stage},
    payload::{
        fixed,
        response::{Body, Content, Method, bounded, dispatch, exact, headers, status::Code},
    },
    request::Id,
};

use crate::{
    conn,
    conn::{io::replies, output},
    manifold::{owners, requests},
    stream,
};

fn write_payload<'d>(body: handoff::Ready<'d>, len: usize) -> Option<output::WritePayload<'d>> {
    if len == 0 {
        return None;
    }
    match body {
        handoff::Ready::Empty => None,
        handoff::Ready::Owned(body) => Some(output::WritePayload::Owned(body)),
        handoff::Ready::Copied(body) => Some(output::WritePayload::Shared(body.into_body())),
        handoff::Ready::Retained(body) => Some(output::WritePayload::Shared(body)),
        handoff::Ready::Segmented(body) | handoff::Ready::Linked(body) => {
            Some(output::WritePayload::from(body))
        }
        handoff::Ready::Fixed(body) => Some(output::WritePayload::Retained(
            resident::Shared::from(body.as_slice()),
        )),
    }
}

struct Head<'a, 'd, P: rules::Policy> {
    prepared: replies::Prepared<'a, 'd, P>,
    bytes: usize,
}

impl<'a, 'd, P: rules::Policy> Head<'a, 'd, P> {
    fn with_body<T>(self, body: Result<T, handoff::Error>) -> Result<(T, Self), conn::Error> {
        match body {
            Ok(body) => Ok((body, self)),
            Err(error) => {
                self.prepared.discard();
                Err(conn::Error::Handoff(error))
            }
        }
    }

    fn complete(self, body: handoff::Ready<'d>) -> Result<replies::Batch<'a, 'd, P>, conn::Error> {
        let actual = body.body_len();
        if actual != self.bytes {
            let expected = self.bytes;
            self.prepared.discard();
            return Err(conn::Error::ResponseLength { expected, actual });
        }
        match write_payload(body, actual) {
            Some(body) => match replies::Data::try_new(body) {
                Ok(body) => Ok(self.prepared.complete(body)),
                Err((error, _)) => {
                    self.prepared.discard();
                    Err(conn::Error::Reply(error))
                }
            },
            None => Ok(self.prepared.finish()),
        }
    }

    fn defer<M: owners::Handoff>(
        self,
        body: handoff::Ready<'d>,
        linked: M::Projection,
        copied: bool,
        state: response::State,
    ) -> Result<Pending<'d, M>, conn::Error> {
        let body_bytes = self.bytes;
        let reply = match state {
            response::State::Streaming => self.prepared.open(),
            response::State::Complete(_) => self.complete(body)?,
        }
        .defer()?;
        Ok(Pending {
            reply,
            body_bytes,
            linked,
            copied,
            state,
        })
    }
}

#[must_use = "an H3 response encoder must prepare exactly one response"]
pub struct Encoder<
    'a,
    'req,
    'd,
    P: rules::Policy = rules::RetainRaw,
    Q: scope::Source = (),
    I = Infallible,
> {
    conn: &'a mut conn::Connection<'d, P>,
    target: stream::Target,
    method: Method,
    body: handoff::Slot<'req, 'd, Q, I>,
}

#[must_use = "an H3 response is not visible until Prepared::finish succeeds"]
pub struct Prepared<
    'a,
    'req,
    'd,
    P: rules::Policy = rules::RetainRaw,
    S = Infallible,
    I = Infallible,
> {
    body: handoff::Plan<'req, 'd, S, I>,
    head: Head<'a, 'd, P>,
}

/// Response transaction after every request borrow has been structurally
/// removed. Only this state may receive the original request owner.
#[must_use = "a detached H3 response is not visible until finish succeeds"]
pub struct Detached<'a, 'd, P: rules::Policy = rules::RetainRaw, M: Mode = Off, D = Infallible> {
    body: handoff::Pending<'d, M, D>,
    head: Head<'a, 'd, P>,
}

#[repr(transparent)]
pub struct Initial<T>(pub(in crate::manifold) T);

/// One resolved response whose unchanged wire owners await queue admission.
#[must_use = "send or discard the pending response"]
pub struct Pending<'d, M: owners::Handoff = Off> {
    reply: replies::Pending<'d>,
    body_bytes: usize,
    linked: M::Projection,
    copied: bool,
    state: response::State,
}

impl<'d, M: owners::Handoff> Pending<'d, M> {
    pub(in crate::manifold) fn send<P: rules::Policy, C: Copy, O>(
        mut self,
        conn: &mut conn::Connection<'d, P>,
        id: requests::Id,
        life: &mut O,
        flow: life::exchange::Id<C, StreamId>,
    ) -> Result<ControlFlow<Self, response::State>, conn::Error>
    where
        O: life::Mode<C, StreamId, Retention = M>,
    {
        let sent = if O::RESPONSE_CHARGE {
            self.reply.send::<On, _, _>(conn.reply(), id.target())
        } else {
            self.reply
                .send::<Off, _, _>(conn.reply(), id.target())
                .map(|sent| sent.map_continue(|_| hold::Charge::default()))
        }?;
        match sent {
            ControlFlow::Break(reply) => {
                self.reply = reply;
                Ok(ControlFlow::Break(self))
            }
            ControlFlow::Continue(charge) => {
                let linked = M::charge(self.linked);
                if let Some(copy) = M::copied(self.copied, self.body_bytes) {
                    life::response::Sink::new(life, flow).copied(copy);
                }
                life.observe(life::Event::ResponseCommitted {
                    exchange: flow,
                    body_bytes: self.body_bytes,
                    charge,
                    linked,
                });
                Ok(ControlFlow::Continue(self.state))
            }
        }
    }

    pub(in crate::manifold) fn discard<P: rules::Policy>(
        self,
        conn: &mut conn::Connection<'d, P>,
        id: requests::Id,
    ) {
        self.reply.discard(conn.reply(), id.target());
    }
}

impl<'a, 'd, P: rules::Policy> Encoder<'a, 'd, 'd, P> {
    pub(crate) fn task(
        conn: &'a mut conn::Connection<'d, P>,
        target: stream::Target,
        method: Method,
        id: Id<'d>,
    ) -> Self {
        Self::from_slot(conn, target, method, handoff::Slot::ready(Some(id)))
    }
}

impl<'a, 'req, 'd: 'req, P: rules::Policy> Encoder<'a, 'req, 'd, P> {
    pub fn new(
        conn: &'a mut conn::Connection<'d, P>,
        target: stream::Target,
        method: Method,
    ) -> Self {
        Self::from_slot(conn, target, method, handoff::Slot::ready(None))
    }
}

impl<'a, 'req, 'd: 'req, P: rules::Policy, I: stage::Body<'req, 'd>>
    Encoder<'a, 'req, 'd, P, (), I>
{
    /// Carries the slot issued by the request's exact source projection.
    pub fn from_slot(
        conn: &'a mut conn::Connection<'d, P>,
        target: stream::Target,
        method: Method,
        body: handoff::Slot<'req, 'd, (), I>,
    ) -> Self {
        Self {
            conn,
            target,
            method,
            body,
        }
    }
}

impl<'a, 'req, 'd: 'req, P: rules::Policy, Q: scope::Source, I: stage::Body<'req, 'd>>
    Encoder<'a, 'req, 'd, P, Q, I>
{
    fn omit(&self, status: Code) -> Result<bool, (conn::Error, stream::Cancellation)> {
        if status.is_info() {
            return Err((
                conn::Error::Informational(status),
                stream::Cancellation::new(self.target),
            ));
        }
        Ok(matches!(
            Content::new(self.method, status.as_u16()),
            Content::Forbidden | Content::Suppressed | Content::Empty
        ))
    }

    fn empty<H: headers::Source>(
        self,
        status: Code,
        headers: H,
    ) -> Result<Prepared<'a, 'req, 'd, P, Q::Selection, I>, (conn::Error, stream::Cancellation)>
    {
        self.prepare(status, headers, fixed::Body::new(&[]), 0)
    }

    fn prepare<H, B>(
        self,
        status: Code,
        headers: H,
        value: B,
        bytes: usize,
    ) -> Result<Prepared<'a, 'req, 'd, P, Q::Selection, I>, (conn::Error, stream::Cancellation)>
    where
        H: headers::Source,
        B: Body<'d>,
    {
        let Self {
            conn, target, body, ..
        } = self;
        let plan = Body::try_resolve(value, &body).map_err(|error| {
            (
                conn::Error::Response(error),
                stream::Cancellation::new(target),
            )
        })?;
        let fields = Response::new(status, headers::Fields::new(headers));
        let prepared = conn
            .reply()
            .prepare(target, fields)
            .map_err(|error| (error, stream::Cancellation::new(target)))?;
        Ok(Prepared {
            body: plan,
            head: Head { prepared, bytes },
        })
    }
}

impl<'a, 'req, 'd: 'req, P: rules::Policy, I: stage::Body<'req, 'd>>
    Prepared<'a, 'req, 'd, P, Infallible, I>
{
    pub(in crate::manifold) fn defer(
        self,
        state: response::State,
    ) -> Result<Pending<'d, Off>, conn::Error> {
        let (body, head) = self.head.with_body(self.body.finish())?;
        head.defer::<Off>(body, Off, false, state)
    }

    pub(in crate::manifold) fn try_defer(
        self,
        state: response::State,
    ) -> Result<Pending<'d, On>, conn::Error> {
        let ((body, linked), head) = self.head.with_body(self.body.try_finish())?;
        let copied = matches!(&body, handoff::Ready::Copied(_));
        head.defer::<On>(body, linked, copied, state)
    }

    /// Ends the request borrow before an owning request body can be moved into
    /// the response transaction.
    pub fn detach(self) -> Detached<'a, 'd, P, Off, I::Deferred>
    where
        I::Deferred: stage::Deferred<'d>,
    {
        let Self { body, head } = self;
        Detached {
            body: body.detach(),
            head,
        }
    }

    /// Detaches while retaining the request-lineage proof.
    pub fn try_detach(self) -> Result<Detached<'a, 'd, P, On, I::Deferred>, conn::Error>
    where
        I::Deferred: stage::Deferred<'d>,
    {
        let (body, head) = self.head.with_body(self.body.try_detach())?;
        Ok(Detached { body, head })
    }

    /// Commits the complete response in one bounded-queue transaction.
    pub fn finish(self) -> Result<hold::Charge, conn::Error> {
        let (body, head) = self.head.with_body(self.body.finish())?;
        head.complete(body)?.commit()
    }
}

impl<'a, 'd, P: rules::Policy, D: stage::Deferred<'d>> Detached<'a, 'd, P, Off, D> {
    pub fn finish_with(self, request: D::Owner) -> Result<hold::Charge, conn::Error> {
        let Self { body, head } = self;
        let body = body.finish(request);
        head.complete(body)?.commit()
    }

    pub(in crate::manifold) fn defer_with(
        self,
        request: D::Owner,
        outcome: life::Outcome,
    ) -> Result<Pending<'d, Off>, conn::Error> {
        let Self { body, head } = self;
        head.defer::<Off>(
            body.finish(request),
            Off,
            false,
            response::State::Complete(outcome),
        )
    }
}

impl<'a, 'd, P: rules::Policy, D: stage::Deferred<'d>> Detached<'a, 'd, P, On, D> {
    pub fn finish_with(
        self,
        request: D::Owner,
    ) -> Result<(hold::Charge, hold::Charge, Option<evidence::Copy>), conn::Error> {
        let Self { body, head } = self;
        let (body, linked) = body.finish(request);
        let (body, copied) = body.into_copy();
        let charge = head.complete(body)?.commit()?;
        Ok((charge, linked, copied))
    }

    pub(in crate::manifold) fn defer_with(
        self,
        request: D::Owner,
        outcome: life::Outcome,
    ) -> Result<Pending<'d, On>, conn::Error> {
        let Self { body, head } = self;
        let (body, linked) = body.finish(request);
        let copied = matches!(&body, handoff::Ready::Copied(_));
        head.defer::<On>(body, linked, copied, response::State::Complete(outcome))
    }
}

impl<'a, 'req, 'd: 'req, P: rules::Policy, Q: scope::Source, I: stage::Body<'req, 'd>>
    dispatch::Accept<'d> for Encoder<'a, 'req, 'd, P, Q, I>
{
    type Output = Prepared<'a, 'req, 'd, P, Q::Selection, I>;
    type Error = (conn::Error, stream::Cancellation);

    fn fixed<H, B>(self, status: Code, headers: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: Body<'d>,
    {
        if self.omit(status)? {
            return self.empty(status, headers);
        }
        let bytes = body.len();
        self.prepare(status, headers, body, bytes)
    }

    fn exact<H, B>(self, status: Code, headers: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: exact::TryBody,
    {
        if self.omit(status)? {
            return self.empty(status, headers);
        }
        let plan = match exact::Plan::try_new(body) {
            Ok(plan) => plan,
            Err(rejected) => {
                return Err((
                    conn::Error::Size(rejected.discard()),
                    decode::Cancel::cancel(self),
                ));
            }
        };
        let body = match plan.into_shared() {
            Ok(body) => body,
            Err(error) => return Err((conn::Error::Build(error), decode::Cancel::cancel(self))),
        };
        let bytes = body.len();
        self.prepare(status, headers, body, bytes)
    }

    fn bounded<H, B>(self, status: Code, headers: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: headers::Source,
        B: bounded::Body,
    {
        if self.omit(status)? {
            return self.empty(status, headers);
        }
        let plan = bounded::Plan::new(body);
        let body = match plan.into_shared() {
            Ok(body) => body,
            Err(error) => return Err((conn::Error::Build(error), decode::Cancel::cancel(self))),
        };
        let bytes = body.len();
        self.prepare(status, headers, body, bytes)
    }
}

impl<'a, 'req, 'd: 'req, P: rules::Policy, I: stage::Body<'req, 'd>> decode::Encode<'req, 'd>
    for Encoder<'a, 'req, 'd, P, (), I>
{
    type Error = conn::Error;
    type Prepared = Prepared<'a, 'req, 'd, P, Infallible, I>;
    type Pending<'id> = Prepared<'a, 'req, 'd, P, scope::Range<'id>, I>;
    type Head<'h, 'id> = Encoder<'a, 'req, 'd, P, scope::View<'h, 'id>, I>;

    fn begin_head<'h, 'id>(self, head: scope::View<'h, 'id>) -> Self::Head<'h, 'id> {
        Encoder {
            conn: self.conn,
            target: self.target,
            method: self.method,
            body: self.body.with_head(head),
        }
    }
}

impl<'a, 'req, 'd: 'req, P: rules::Policy, I: stage::Body<'req, 'd>> decode::Streaming<'req, 'd>
    for Encoder<'a, 'req, 'd, P, (), I>
{
    fn split_stream<S>(output: Self::Stream<S>) -> (Self::Prepared, Option<S>) {
        output
    }
}

impl<'a, 'req, 'd: 'req, P: rules::Policy, Q: scope::Source, I: stage::Body<'req, 'd>>
    dispatch::Sink<'d> for Encoder<'a, 'req, 'd, P, Q, I>
{
    type Stream<S> = (Self::Output, Option<S>);

    fn stream<H, S>(
        self,
        status: Code,
        headers: H,
        stream: S,
    ) -> Result<Self::Stream<S>, Self::Error>
    where
        H: headers::Source,
    {
        let omit = self.omit(status)?;
        let prepared = self.empty(status, headers)?;
        Ok((prepared, if omit { None } else { Some(stream) }))
    }
}

impl<'a, 'req, 'd, P: rules::Policy, Q: scope::Source, I> decode::Cancel
    for Encoder<'a, 'req, 'd, P, Q, I>
{
    type Target = stream::Cancellation;

    fn cancel(self) -> Self::Target {
        stream::Cancellation::new(self.target)
    }
}

impl<'a, 'req, 'd: 'req, 'id, P: rules::Policy, I: stage::Body<'req, 'd>> decode::Lineage<'id>
    for Prepared<'a, 'req, 'd, P, scope::Range<'id>, I>
{
    type Output = Prepared<'a, 'req, 'd, P, Infallible, I>;

    fn resolve_head<O: Into<scope::Owner<'id, Shared>>, Q: proof::Proof>(
        self,
        head: request::Head<O, Q>,
    ) -> Self::Output {
        Prepared {
            body: self.body.resolve_head(head.into_owner()),
            head: self.head,
        }
    }
}
