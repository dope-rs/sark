use std::{convert::Infallible, marker, mem, ops, pin};

use dope::{
    core::driver::{route::Identity, schedule::app},
    manifold::network::listener::conn::Id,
};
use o3::{
    alloc,
    buffer::resident,
    depot::{capacity, map::direct, queue::indexed::keyed},
    mem::Rejected,
};
use sark_core::{
    body::{self, credit},
    decode::{request, state},
    duplex::{self, input},
    life::{self, audit, exchange},
    manifold::{
        schedule::{self, typed},
        server::{Connection as _, Event, drain},
        stream::Boundary,
        task::app::Runtime,
    },
};
use sark_http::{field::metadata::section::rules, owner::cause};

use crate::{
    conn::{self, event, machine},
    frame::numbers,
    server::{
        self, payload,
        response::Handoff,
        state::Transitions,
        task::{
            ingress::{self, entry},
            response::{self, input::Error},
            *,
        },
    },
    stream,
};

pub(super) struct Book<'d, P, G, F, B, X, D>
where
    P: request::Prepared,
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
{
    pub(super) incoming: direct::Table<stream::Key, entry::Entry<'d, P, G, B>>,
    pub(super) responses: typed::Runtime<
        stream::Key,
        response::Response<'d, P, X, D>,
        response::Data<'d, D>,
        Infallible,
        conn::DataBlock,
    >,
    pub(super) build_ready: keyed::Fifo<stream::Key, entry::Build>,
    pub(super) admission_ready: keyed::Fifo<stream::Key, entry::Admission>,
    pub(super) body_credit: credit::raw::Account,
    policy: marker::PhantomData<fn() -> F>,
}
impl<'d, P: request::Prepared, G, F, B, X, D> Book<'d, P, G, F, B, X, D>
where
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
{
    pub(super) fn try_new(domain: capacity::Count) -> Result<Self, alloc::Error> {
        Ok(Self {
            incoming: direct::Table::try_new(domain)?,
            responses: typed::Runtime::try_bounded(domain, domain)?,
            build_ready: keyed::Fifo::new(),
            admission_ready: keyed::Fifo::new(),
            body_credit: credit::raw::Account::new(),
            policy: marker::PhantomData,
        })
    }
}

pub(super) struct Turn<'a, 'd, P, G, F, B, X, D>
where
    P: request::Prepared,
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
{
    pub(super) connection: &'a mut machine::Connection<crate::ServerRole, F, resident::Shared<'d>>,
    book: &'a mut Book<'d, P, G, F, B, X, D>,
}

impl<'a, 'd, P, G, F, B, X, D> Turn<'a, 'd, P, G, F, B, X, D>
where
    P: request::Prepared,
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
{
    pub(super) fn new(
        conn: &'a mut machine::Connection<crate::ServerRole, F, resident::Shared<'d>>,
        book: &'a mut Book<'d, P, G, F, B, X, D>,
    ) -> Self {
        const { assert!(mem::size_of::<Self>() == 2 * mem::size_of::<usize>()) };
        Self {
            connection: conn,
            book,
        }
    }
}

impl<'d, P, G, F, B, X, D> ops::Deref for Turn<'_, 'd, P, G, F, B, X, D>
where
    P: request::Prepared,
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
{
    type Target = Book<'d, P, G, F, B, X, D>;

    fn deref(&self) -> &Self::Target {
        self.book
    }
}

impl<'d, P, G, F, B, X, D> ops::DerefMut for Turn<'_, 'd, P, G, F, B, X, D>
where
    P: request::Prepared,
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.book
    }
}

impl<'d, P: request::Prepared, G, F, B, X, D> Turn<'_, 'd, P, G, F, B, X, D>
where
    G: state::Family<P>,
    F: rules::Policy,
    B: body::Build<'d>,
    D: duplex::Session<'d>,
{
    pub(super) fn activation(&self, limits: server::Config) -> Option<conn::DataBlock> {
        let blocked = self.connection.output().len() >= limits.max_outbound_bytes;
        blocked.then_some(conn::DataBlock::Egress)
    }

    pub(super) fn can_admit(&mut self) -> bool {
        !self.admission_ready.is_empty() && self.connection.streams().headers().is_ready()
    }

    pub(super) fn has_events(&mut self) -> bool {
        self.connection.ingress().has_event()
    }

    pub(super) fn ready(
        &mut self,
    ) -> Option<machine::Ready<'_, F, resident::Shared<'d>, event::Retirement>> {
        let ready = self.connection.ready()?;
        if let Some(key) = ready.data_stream()
            && matches!(
                self.book.incoming.get(key),
                Some(entry::Entry::Streaming { link, .. }) if link.is_linked(key)
            )
        {
            return None;
        }
        Some(ready)
    }

    /// Includes decoder-retained work without changing queued-only wake checks.
    pub(super) fn has_input(&mut self) -> bool {
        self.has_events() || self.connection.ingress().is_blocked()
    }

    pub(super) fn vacant(&mut self, stream: stream::Key) -> Option<slot::Slot<'_, 'd, P, F, X, D>> {
        let vacant = self.book.responses.vacant(stream)?;
        Some(slot::Slot {
            connection: self.connection,
            response: vacant,
            body_credit: &mut self.book.body_credit,
        })
    }

    /// Transfers one DATA event into its exact duplex owner. A non-duplex or
    /// absent stream returns the original event for the regular ingress path.
    pub(super) fn try_push(
        &mut self,
        stream: stream::Key,
        data: event::DataPayload<resident::Shared<'d>>,
        boundary: Boundary,
    ) -> Result<
        ops::ControlFlow<(), event::DataPayload<resident::Shared<'d>>>,
        input::PushError<event::DataPayload<resident::Shared<'d>>, payload::Error>,
    > {
        let Some(entry) = self.responses.entry(stream) else {
            return Ok(ops::ControlFlow::Continue(data));
        };
        let result = match entry {
            typed::Entry::Control(entry) => entry.change(|value| value.push(data, boundary)).1,
            typed::Entry::Data(entry) => {
                let (entry, result) = entry.change(|value| value.push(data, boundary));
                if matches!(result, response::Duplex::Applied(_)) {
                    entry.activate(None);
                }
                result
            }
            typed::Entry::Idle(entry) => match *entry.get() {},
        };
        match result {
            response::Duplex::Applied(result) => result.map(|()| ops::ControlFlow::Break(())),
            response::Duplex::Other(input) => Ok(ops::ControlFlow::Continue(input)),
            response::Duplex::Closed(error, input) => {
                Err(input::PushError::Buffer(Rejected::new(input, error)))
            }
        }
    }

    pub(super) fn try_finish(&mut self, stream: stream::Key) -> Option<Result<(), Error>> {
        let result = match self.responses.entry(stream)? {
            typed::Entry::Control(entry) => entry.change(response::Response::finish).1,
            typed::Entry::Data(entry) => {
                let (entry, result) = entry.change(response::Data::finish);
                if matches!(result, response::Duplex::Applied(_)) {
                    entry.activate(None);
                }
                result
            }
            typed::Entry::Idle(entry) => match *entry.get() {},
        };
        match result {
            response::Duplex::Applied(result) => Some(result),
            response::Duplex::Other(()) => None,
            response::Duplex::Closed(error, ()) => Some(Err(Error::Input(error))),
        }
    }

    pub(super) fn data(&mut self) -> ingress::Data<'_, 'd, P, G, B> {
        ingress::Data::from_book(self.book)
    }

    pub(super) fn reset<ID: Identity, E: credit::Evidence>(
        &mut self,
        exchange: exchange::Id<Id<'d, ID>, stream::Key>,
        error: numbers::ErrorCode,
        proof: E,
    ) -> (
        Option<response::Stored<'d, P, X, D>>,
        Option<event::Retirement>,
    ) {
        let (exchange, error) = match self.data().discard(exchange, proof) {
            ingress::Discard::Missing(exchange) | ingress::Discard::Removed(exchange) => {
                (exchange, error)
            }
            ingress::Discard::Failed {
                exchange,
                error: discard,
            } => (exchange, discard.code()),
        };
        let stream = exchange.stream();
        let value = self.book.responses.remove(stream);
        let write = self.connection.streams().retire(stream, error);
        (value, write)
    }

    pub(super) fn pump<const ROUND: bool, ID: Identity + 'd, R, M>(
        &mut self,
        router: pin::Pin<&mut R>,
        id: Id<'d, ID>,
        limits: server::Config,
        egress: schedule::Egress,
        work: app::Work<'_, 'd>,
        mode: &mut M,
    ) -> schedule::Report
    where
        R: Runtime<
                'd,
                Prepared = P,
                Routes = G,
                Plan = F,
                Build = B,
                Completion = X,
                Duplex = D,
                Target = Target<'d, ID>,
            >,
        M: Transitions<Id<'d, ID>>,
        M::Retention: Handoff,
    {
        let conn = &mut *self.connection;
        let store = &mut self.book.responses;
        store.resume(|| conn.flow().poll_wake(), egress);
        let mut driver = response::Driver {
            router,
            connection_id: id,
            connection: conn,
            limits,
            work,
            mode,
        };
        match ROUND {
            true => store.drive_round(&mut driver),
            false => store.drive(&mut driver),
        }
    }

    pub(super) fn close<ID: Identity, M: life::Mode<Id<'d, ID>, stream::Key>>(
        &mut self,
        conn: Id<'d, ID>,
        mode: &mut M,
    ) where
        D: duplex::Session<'d>,
    {
        while let Some(ready) = self.connection.ready() {
            if let Some(cause) = ready.rejection() {
                life::Mode::diagnostic(mode, cause);
            }
            let event = drain::Ready::take(ready);
            let write = match event {
                Event::Retired(write) => Some(write),
                Event::Reset { retired: write, .. } => write,
                _ => None,
            };
            if let Some(write) = write {
                release_retired(mode, conn, write, cause::Staging::PeerRetired);
            }
        }
        if !self.book.incoming.is_empty() {
            for index in 0..self.book.incoming.capacity() {
                let Some(stream) = self.book.incoming.key_at(index) else {
                    continue;
                };
                let exchange = identify(conn, stream);
                if let ingress::Discard::Failed { error, .. } =
                    self.data().discard(exchange, mode.body(exchange))
                {
                    life::Mode::diagnostic(mode, &error);
                }
                exchange.retire(mode, life::Outcome::Cancelled);
            }
        }
        let state = &mut *self.connection;
        if !self.book.responses.is_empty() {
            for index in 0..self.book.responses.capacity() {
                let Some(stream) = self.book.responses.key_at(index) else {
                    continue;
                };
                let Some(value) = self.book.responses.remove(stream) else {
                    continue;
                };
                match value {
                    Value::Data(response::Data::Body { pending, .. }) => {
                        let exchange = identify(conn, stream);
                        release_pending(mode, exchange, pending, cause::Staging::Closed);
                        exchange.retire(mode, life::Outcome::Cancelled);
                    }
                    Value::Control(response::Response::Encoded(wire)) => {
                        identify(conn, stream).retire(mode, life::Outcome::Cancelled);
                        drop(wire);
                    }
                    Value::Control(
                        response::Response::Head { head, .. }
                        | response::Response::DuplexHead { head, .. },
                    ) => {
                        head.discard(state);
                        identify(conn, stream).retire(mode, life::Outcome::Cancelled);
                    }
                    Value::Data(response::Data::Duplex { .. })
                    | Value::Control(
                        response::Response::Status(_)
                        | response::Response::Sync(_)
                        | response::Response::Completion(_)
                        | response::Response::DuplexOpen { .. },
                    ) => {
                        identify(conn, stream).retire(mode, life::Outcome::Cancelled);
                    }
                    Value::Idle(never) => match never {},
                }
            }
        }
        state.transfer().discard(|write| {
            release_retired(mode, conn, write, cause::Staging::Closed);
        });
    }

    /// Projects final gauges from their stores, certifying inferred zeroes only
    /// when every canonical root is empty.
    pub(super) fn audit(&self, tasks: usize, empty: bool) -> audit::Snapshot {
        let held = self.book.body_credit.retained();
        let req_ok =
            self.book.incoming.is_empty() && self.book.responses.is_empty() && tasks == 0 && empty;
        let res_ok = self.book.responses.is_empty() && self.connection.output().is_empty();

        audit::Snapshot::default()
            .request(
                self.book.incoming.len(),
                held.bytes(),
                held.chunks().into_raw() as usize,
                usize::from(!self.book.build_ready.is_empty()),
            )
            .owner_bytes(usize::from(!req_ok))
            .owners(usize::from(!req_ok))
            .task(tasks, tasks, 0)
            .streams(0, 0, usize::from(!empty))
            .wire_bytes(0)
            .stage_bytes(usize::from(!res_ok))
    }
}
mod slot;
