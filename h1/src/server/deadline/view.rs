//! Deadline projection composed with one existing H1 lifecycle mode.

use std::fmt;

use dope::{
    core::driver::{route::Identity, schedule::clock},
    manifold::network::listener::conn,
};
use o3::buffer::hold;
use sark_core::{
    body::{credit, credit::Mode as _},
    life::{self, Observer, audit, exchange, sequence},
};
use sark_http::owner::cause;

use crate::server::deadline::{self, domain};

/// Couples exchange deadlines to the same connection identity as lifecycle evidence.
pub struct View<'d, M, ID: Identity> {
    mode: M,
    state: domain::Domain<'d, ID>,
}

impl<'d, M, ID: Identity> View<'d, M, ID> {
    pub(super) const fn new(mode: M, state: domain::Domain<'d, ID>) -> Self {
        Self { mode, state }
    }
}

impl<'d, ID: Identity, M> deadline::Mode<'d, ID> for View<'d, M, ID>
where
    M: sequence::Mode<conn::Id<'d, ID>>,
{
    fn begin(&mut self, now: clock::Moment<'d>) -> clock::Moment<'d> {
        self.state.begin_turn(now);
        now
    }

    fn finish(
        &mut self,
        conn: conn::Id<'d, ID>,
        now: clock::Moment<'d>,
        head: deadline::Head,
        body: deadline::Body,
    ) -> deadline::Root<clock::Deadline<'d>> {
        self.state.turn(conn, now, head, body)
    }

    fn timeout(
        &mut self,
        conn: conn::Id<'d, ID>,
        now: clock::Moment<'d>,
    ) -> deadline::Due<conn::Id<'d, ID>, clock::Deadline<'d>> {
        self.state.timeout(conn, now)
    }
}

#[doc(hidden)]
pub struct Body<'a, 'd, ID: Identity, B> {
    exchange: exchange::Id<conn::Id<'d, ID>, u64>,
    inner: B,
    state: &'a mut domain::Domain<'d, ID>,
}

impl<'d, ID: Identity, B> credit::Evidence for Body<'_, 'd, ID, B>
where
    B: credit::Evidence,
{
    type Change = credit::Mutation;

    fn record(&mut self, change: credit::Mutation) {
        if let credit::Mutation::Head = &change {
            self.state.admit_head(self.exchange);
        }
        if let Some(change) = B::Change::committed(change) {
            self.inner.record(change);
        }
    }
}

impl<'d, ID: Identity, M> life::Observer<conn::Id<'d, ID>, u64> for View<'d, M, ID>
where
    M: life::Mode<conn::Id<'d, ID>, u64>,
{
    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        if let Some(observer) = self.mode.observer() {
            Observer::diagnostic(observer, cause);
        }
    }

    fn observe(&mut self, event: life::Event<conn::Id<'d, ID>, u64>) {
        self.state.observe(event);
        if let Some(out) = self.mode.observer() {
            life::Observer::observe(out, event);
        }
    }

    fn audit(&mut self, conn: conn::Id<'d, ID>, state: audit::Snapshot) {
        if let Some(out) = self.mode.observer() {
            life::Observer::audit(out, conn, state);
        }
    }
}

impl<'d, ID: Identity, M> life::Mode<conn::Id<'d, ID>, u64> for View<'d, M, ID>
where
    M: life::Mode<conn::Id<'d, ID>, u64>,
{
    const ENABLED: bool = true;

    type Retention = M::Retention;
    type Observer = Self;

    type Body<'a>
        = Body<'a, 'd, ID, M::Body<'a>>
    where
        Self: 'a,
        'd: 'a;

    fn observer(&mut self) -> Option<&mut Self::Observer> {
        Some(self)
    }

    fn body<'a>(&'a mut self, id: exchange::Id<conn::Id<'d, ID>, u64>) -> Self::Body<'a>
    where
        'd: 'a,
    {
        Body {
            exchange: id,
            inner: self.mode.body(id),
            state: &mut self.state,
        }
    }
}

impl<'d, ID: Identity, M> sequence::Mode<conn::Id<'d, ID>> for View<'d, M, ID>
where
    M: sequence::Mode<conn::Id<'d, ID>>,
{
    type Session = M::Session;
    type TransportTag = M::TransportTag;
    type Trace = Self;

    fn trace(&self) -> Option<&Self::Trace> {
        Some(self)
    }

    fn trace_mut(&mut self) -> Option<&mut Self::Trace> {
        Some(self)
    }
}

impl<'d, ID: Identity, M> sequence::Trace<conn::Id<'d, ID>> for View<'d, M, ID>
where
    M: sequence::Mode<conn::Id<'d, ID>>,
{
    type Session = M::Session;
    type TransportTag = M::TransportTag;

    fn tag(
        &self,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        charge: hold::Charge,
    ) -> Self::TransportTag {
        match sequence::Mode::trace(&self.mode) {
            Some(trace) => sequence::Trace::tag(trace, id, charge),
            None => Self::TransportTag::default(),
        }
    }

    fn transport(
        &self,
        conn: conn::Id<'d, ID>,
        tag: Self::TransportTag,
    ) -> Option<(exchange::Id<conn::Id<'d, ID>, u64>, hold::Charge)> {
        let trace = sequence::Mode::trace(&self.mode)?;
        sequence::Trace::transport(trace, conn, tag)
    }

    fn open(
        &mut self,
        session: &mut Self::Session,
        conn: conn::Id<'d, ID>,
    ) -> exchange::Id<conn::Id<'d, ID>, u64> {
        match sequence::Mode::trace_mut(&mut self.mode) {
            Some(trace) => sequence::Trace::open(trace, session, conn),
            None => exchange::Id::new(conn, 0),
        }
    }

    fn current(
        &self,
        session: &Self::Session,
        conn: conn::Id<'d, ID>,
    ) -> Option<exchange::Id<conn::Id<'d, ID>, u64>> {
        match sequence::Mode::trace(&self.mode) {
            Some(trace) => sequence::Trace::current(trace, session, conn),
            None => Some(exchange::Id::new(conn, 0)),
        }
    }

    fn commit_link(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        bytes: usize,
        charge: hold::Charge,
        linked: hold::Charge,
    ) {
        self.state.observe(life::Event::ResponseCommitted {
            exchange: id,
            body_bytes: bytes,
            charge,
            linked,
        });
        if let Some(trace) = sequence::Mode::trace_mut(&mut self.mode) {
            sequence::Trace::commit_link(trace, session, id, bytes, charge, linked);
        }
    }

    fn stage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        charge: hold::Charge,
    ) {
        if let Some(trace) = sequence::Mode::trace_mut(&mut self.mode) {
            sequence::Trace::stage(trace, session, id, charge);
        }
    }

    fn unstage(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        charge: hold::Charge,
    ) {
        if let Some(trace) = sequence::Mode::trace_mut(&mut self.mode) {
            sequence::Trace::unstage(trace, session, id, charge);
        }
    }

    fn accept(
        &mut self,
        session: &mut Self::Session,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        charge: hold::Charge,
    ) {
        if let Some(trace) = sequence::Mode::trace_mut(&mut self.mode) {
            sequence::Trace::accept(trace, session, id, charge);
        }
    }

    fn release(
        &mut self,
        id: exchange::Id<conn::Id<'d, ID>, u64>,
        charge: hold::Charge,
        cause: cause::Transport,
    ) {
        if let Some(trace) = sequence::Mode::trace_mut(&mut self.mode) {
            sequence::Trace::release(trace, id, charge, cause);
        }
    }

    fn close(&mut self, session: &mut Self::Session, conn: conn::Id<'d, ID>) {
        if let Some(trace) = sequence::Mode::trace_mut(&mut self.mode) {
            sequence::Trace::close(trace, session, conn);
        }
    }
}
