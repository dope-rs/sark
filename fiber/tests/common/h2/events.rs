use std::{cell::Cell, fmt};

use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use dope_test::harness::signals::Progress;
use o3::buffer::{hold, pool};
use sark_core::life::{self, audit};
use sark_h2::stream;
#[derive(Clone, Copy)]
pub struct Log<'a, 'name, F>(pub &'a Cell<Vec<(&'name str, u64)>>, pub F);

pub struct Count<'a, 'name> {
    pub facts: &'a Cell<Vec<(&'name str, u64)>>,
    pub event: &'a str,
}

impl Progress for Count<'_, '_> {
    fn hits(&self) -> u32 {
        let facts = self.facts.take();
        let hits = facts.iter().filter(|event| event.0 == self.event).count() as u32;
        self.facts.set(facts);
        hits
    }
}

#[derive(Default)]
pub struct Credit {
    pub reserved: (usize, u32),
    pub released: (usize, u32),
    pub closing: Option<(usize, u32)>,
    pub audit: Option<audit::Snapshot>,
    pub invalid: bool,
}

pub struct CreditLog<'a, 'name, F> {
    pub log: Log<'a, 'name, F>,
    pub state: &'a Cell<Credit>,
}

pub struct PoolLog<'a, O> {
    pub log: O,
    pub pool: &'a pool::Pool,
    pub released: &'a Cell<Option<(hold::Charge, usize)>>,
}

impl<C: Copy, S: Copy, O: life::Observer<C, S>> life::Observer<C, S> for PoolLog<'_, O> {
    fn observe(&mut self, event: life::Event<C, S>) {
        use sark_http::owner::{Change, cause};

        if let life::Event::ResponseOwnership(effect) = &event
            && effect.change() == Change::StagingReleased(cause::Staging::Closed)
        {
            self.released
                .set(Some((effect.charge(), self.pool.available())));
        }
        self.log.observe(event);
    }

    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        self.log.diagnostic(cause);
    }

    fn audit(&mut self, conn: C, state: audit::Snapshot) {
        self.log.audit(conn, state);
    }
}

impl<'d, ID: Identity, F: FnMut(&dyn fmt::Debug)> life::Observer<conn::Id<'d, ID>, stream::Key>
    for CreditLog<'_, '_, F>
{
    fn observe(&mut self, event: life::Event<conn::Id<'d, ID>, stream::Key>) {
        {
            let mut state = self.state.take();
            match &event {
                life::Event::PreAdmissionReserved { bytes, chunks, .. } => {
                    let (bytes, bcarry) = state.reserved.0.overflowing_add(*bytes);
                    let (chunks, ccarry) = state.reserved.1.overflowing_add(chunks.into_raw());
                    state.reserved = (bytes, chunks);
                    state.invalid |= bcarry || ccarry;
                }
                life::Event::PreAdmissionReleased { bytes, chunks, .. } => {
                    let (bytes, bcarry) = state.released.0.overflowing_add(*bytes);
                    let (chunks, ccarry) = state.released.1.overflowing_add(chunks.into_raw());
                    state.released = (bytes, chunks);
                    state.invalid |= bcarry || ccarry;
                }
                life::Event::SessionClosing(_) => {
                    let (bytes, bborrow) = state.reserved.0.overflowing_sub(state.released.0);
                    let (chunks, cborrow) = state.reserved.1.overflowing_sub(state.released.1);
                    state.closing = Some((bytes, chunks));
                    state.invalid |= bborrow || cborrow;
                }
                _ => {}
            }
            self.state.set(state);
        }
        self.log.observe(event);
    }

    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        <Log<'_, '_, F> as life::Observer<conn::Id<'d, ID>, stream::Key>>::diagnostic(
            &mut self.log,
            cause,
        );
    }

    fn audit(&mut self, conn: conn::Id<'d, ID>, snapshot: audit::Snapshot) {
        let mut state = self.state.take();
        state.audit = Some(snapshot);
        self.state.set(state);
        self.log.audit(conn, snapshot);
    }
}

impl<'d, ID: Identity, F: FnMut(&dyn fmt::Debug)> life::Observer<conn::Id<'d, ID>, stream::Key>
    for Log<'_, '_, F>
{
    fn observe(&mut self, event: life::Event<conn::Id<'d, ID>, stream::Key>) {
        use sark_core::life::{Event, Outcome};

        let fact = match event {
            Event::SessionOpened(_) => ("session-opened", u64::MAX),
            Event::SessionEstablished(_) => ("session-established", u64::MAX),
            Event::ExchangeOpened(exchange) => ("exchange-opened", exchange.stream().identity()),
            Event::HeadAdmitted(exchange) => ("head-admitted", exchange.stream().identity()),
            Event::PreAdmissionReserved { exchange, .. } => {
                ("pre-admission-reserved", exchange.stream().identity())
            }
            Event::PreAdmissionReleased { exchange, .. } => {
                ("pre-admission-released", exchange.stream().identity())
            }
            Event::RequestOwnerAcquired { exchange, .. } => {
                ("request-owner-acquired", exchange.stream().identity())
            }
            Event::RequestOwnerScopeClosed(exchange) => {
                ("request-owner-scope-closed", exchange.stream().identity())
            }
            Event::InputFinished(exchange) => ("input-finished", exchange.stream().identity()),
            Event::BuildReady(exchange) => ("build-ready", exchange.stream().identity()),
            Event::TaskAdmitted(exchange) => ("task-admitted", exchange.stream().identity()),
            Event::TaskPollPending(exchange) => ("task-poll-pending", exchange.stream().identity()),
            Event::TaskPollReady(exchange) => ("task-poll-ready", exchange.stream().identity()),
            Event::TaskCancelled(exchange) => ("task-cancelled", exchange.stream().identity()),
            Event::ExchangeDeadlineExpired { exchange, .. } => {
                ("exchange-deadline-expired", exchange.stream().identity())
            }
            Event::ResponseCommitted { exchange, .. } => {
                ("response-committed", exchange.stream().identity())
            }
            Event::ResponseBodyCopied { exchange, .. } => {
                ("response-body-copied", exchange.stream().identity())
            }
            Event::ResponseOwnership(effect) => {
                use sark_http::owner::{Change, cause};

                let exchange = *effect.key();
                let kind = match effect.change() {
                    Change::StagingAcquired(
                        cause::Acquire::Committed
                        | cause::Acquire::Split
                        | cause::Acquire::StreamChunk,
                    ) => "response-staging-acquired",
                    Change::Handoff => "response-handoff",
                    Change::StagingReleased(cause::Staging::Buffered) => {
                        "response-staging-buffered"
                    }
                    Change::StagingReleased(
                        cause::Staging::TransportAccepted
                        | cause::Staging::Reset
                        | cause::Staging::Closed
                        | cause::Staging::PeerRetired,
                    ) => "response-staging-released",
                    Change::TransportReleased(
                        cause::Transport::Sent
                        | cause::Transport::Acked
                        | cause::Transport::Stopped
                        | cause::Transport::Reset
                        | cause::Transport::Closed
                        | cause::Transport::PeerRetired,
                    ) => "response-transport-released",
                };
                (kind, exchange.stream().identity())
            }
            Event::Terminal { exchange, .. } => ("terminal", exchange.stream().identity()),
            Event::ExchangeRetired(exchange) => ("exchange-retired", exchange.stream().identity()),
            Event::SessionClosing(_) => ("session-closing", u64::MAX),
            Event::SessionClosed(_) => ("session-closed", u64::MAX),
        };
        let mut facts = self.0.take();
        if let Event::Terminal {
            outcome: Outcome::Cancelled,
            ..
        } = event
        {
            facts.push(("exchange-cancelled", fact.1));
        }
        facts.push(fact);
        self.0.set(facts);
    }

    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        (self.1)(cause);
    }

    fn audit(&mut self, conn: conn::Id<'d, ID>, _: audit::Snapshot) {
        let mut facts = self.0.take();
        facts.push(("audit", conn.index().into_usize() as u64));
        self.0.set(facts);
    }
}
