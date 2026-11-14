//! Protocol-neutral evidence committed by lifecycle owners.

use std::{convert::Infallible, fmt};

use hold::evidence;
use o3::buffer::{hold, segments};
use sark_http::owner::{self, evidence::Cause};

use crate::{body::credit, deadline};

pub mod audit;
pub mod endpoint;
pub mod exchange;
pub mod mode;
pub mod owners;
pub mod response;
pub mod sequence;
pub mod session;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Outcome {
    Completed,
    Cancelled,
    Rejected,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Event<C, S> {
    SessionOpened(C),
    SessionEstablished(C),
    ExchangeOpened(exchange::Id<C, S>),
    HeadAdmitted(exchange::Id<C, S>),
    PreAdmissionReserved {
        exchange: exchange::Id<C, S>,
        bytes: usize,
        chunks: segments::Count,
    },
    PreAdmissionReleased {
        exchange: exchange::Id<C, S>,
        bytes: usize,
        chunks: segments::Count,
    },
    RequestOwnerAcquired {
        exchange: exchange::Id<C, S>,
        charge: owners::Request,
    },
    RequestOwnerScopeClosed(exchange::Id<C, S>),
    InputFinished(exchange::Id<C, S>),
    BuildReady(exchange::Id<C, S>),
    TaskAdmitted(exchange::Id<C, S>),
    TaskPollPending(exchange::Id<C, S>),
    TaskPollReady(exchange::Id<C, S>),
    TaskCancelled(exchange::Id<C, S>),
    ResponseCommitted {
        exchange: exchange::Id<C, S>,
        body_bytes: usize,
        charge: hold::Charge,
        linked: hold::Charge,
    },
    ResponseBodyCopied {
        exchange: exchange::Id<C, S>,
        read: usize,
        written: usize,
        cause: Cause,
    },
    ResponseOwnership(owner::Effect<exchange::Id<C, S>>),
    Terminal {
        exchange: exchange::Id<C, S>,
        outcome: Outcome,
    },
    ExchangeDeadlineExpired {
        exchange: exchange::Id<C, S>,
        phase: deadline::Phase,
    },
    ExchangeRetired(exchange::Id<C, S>),
    SessionClosing(C),
    SessionClosed(C),
}

pub trait Observer<C: Copy, S: Copy> {
    fn observe(&mut self, event: Event<C, S>);

    /// Observes a borrowed local cause without changing lifecycle decisions.
    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E);

    fn audit(&mut self, conn: C, state: audit::Snapshot);
}

impl<C: Copy, S: Copy> Observer<C, S> for Infallible {
    fn diagnostic<E: fmt::Debug>(&mut self, _: &E) {
        match *self {}
    }

    fn observe(&mut self, _: Event<C, S>) {
        match *self {}
    }

    fn audit(&mut self, _: C, _: audit::Snapshot) {
        match *self {}
    }
}

pub trait Mode<C: Copy, S: Copy> {
    const ENABLED: bool;

    /// Whether response commits need exact retained-owner charges.
    const RESPONSE_CHARGE: bool = true;

    /// Whether stream-chunk acquisition needs exact retained-owner charges.
    const STREAM_CHARGE: bool = true;

    type Retention: evidence::Mode;
    type Observer: Observer<C, S>;

    type Body<'a>: credit::Evidence
    where
        Self: 'a,
        C: 'a,
        S: 'a;

    fn observer(&mut self) -> Option<&mut Self::Observer>;

    fn observe(&mut self, event: Event<C, S>) {
        if let Some(out) = self.observer() {
            out.observe(event);
        }
    }

    /// Observes a borrowed local cause without changing lifecycle decisions.
    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        if let Some(out) = self.observer() {
            out.diagnostic(cause);
        }
    }

    fn audit<F>(&mut self, conn: C, get: F)
    where
        F: FnOnce() -> audit::Snapshot,
    {
        if let Some(out) = self.observer() {
            out.audit(conn, get());
        }
    }

    fn body<'a>(&'a mut self, id: exchange::Id<C, S>) -> Self::Body<'a>
    where
        C: 'a,
        S: 'a;

    fn close<F>(&mut self, conn: C, get: F)
    where
        F: FnOnce() -> audit::Snapshot,
    {
        let out = match self.observer() {
            Some(out) => out,
            None => return,
        };
        out.audit(conn, get());
        out.observe(Event::SessionClosed(conn));
    }
}

pub struct BodyScope<'a, C, S, O> {
    pub(crate) id: exchange::Id<C, S>,
    pub(crate) out: &'a mut O,
}

impl<C: Copy, S: Copy, O: Observer<C, S>> credit::Evidence for BodyScope<'_, C, S, O> {
    type Change = credit::Mutation;

    fn record(&mut self, change: credit::Mutation) {
        match change {
            credit::Mutation::Head => {
                self.out.observe(Event::HeadAdmitted(self.id));
            }
            credit::Mutation::Reserved(credit) => {
                self.out.observe(Event::PreAdmissionReserved {
                    exchange: self.id,
                    bytes: credit.bytes(),
                    chunks: credit.chunks(),
                });
            }
            credit::Mutation::Released {
                credit,
                disposition: kind,
            } => {
                self.out.observe(Event::PreAdmissionReleased {
                    exchange: self.id,
                    bytes: credit.bytes(),
                    chunks: credit.chunks(),
                });
                if kind == credit::Disposition::Admitted {
                    self.out.observe(Event::RequestOwnerAcquired {
                        exchange: self.id,
                        charge: owners::Request::new(credit.bytes(), credit.chunks()),
                    });
                }
            }
            credit::Mutation::BuildReady => self.out.observe(Event::BuildReady(self.id)),
        }
    }
}

#[must_use = "finish protocol cleanup, then consume the expiry"]
pub struct Expiry<C: Copy, S: Copy> {
    exchange: exchange::Id<C, S>,
    phase: deadline::Phase,
}

impl<C: Copy, S: Copy> Expiry<C, S> {
    pub(crate) const fn new(id: exchange::Id<C, S>, phase: deadline::Phase) -> Self {
        Self {
            exchange: id,
            phase,
        }
    }

    pub const fn exchange(&self) -> exchange::Id<C, S> {
        self.exchange
    }

    pub const fn phase(&self) -> deadline::Phase {
        self.phase
    }

    pub fn finish<M: Mode<C, S>>(self, mode: &mut M) {
        self.exchange.retire(mode, Outcome::Cancelled);
    }
}
