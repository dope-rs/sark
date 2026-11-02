use sark_http::payload::compress::dynamic::hash;

use crate::manifold::{
    Phase,
    requests::exchange::Error,
    server::{deadline::state::State, failure::Build},
};
pub(super) mod connect;
pub(super) mod egress;
pub(super) mod finish;
pub(super) mod input;
mod retire;
pub(super) mod status;
use std::pin;

use dope::core::driver::schedule::clock;
use dope_quic::{
    conn::{
        id, scope,
        stream::resident::{Handle, Local},
    },
    errors, stream,
    wire::stream::Id,
};
use flow::report::Report;
use io::ingress::schedule;
use o3::buffer::resident;
use owners::Projection;
use requests::{
    book,
    exchange::{Drained, Reset, Stop, Termination},
};
use retire::Retire;
use sark_core::{
    body::credit,
    deadline::Poll,
    life::{self, audit, exchange, session},
    manifold::task::app,
};

use crate::{
    conn::{event, io, io::write::Gate},
    frame,
    link::Flush,
    manifold,
    manifold::{
        flow, owners, requests,
        server::{self, deadline, rotation},
    },
};

/// Request-side roots that must read zero before the request owner retires.
const REQUEST_ROOTS: audit::Capability = audit::Capability::REQUEST_BOOK_ENTRIES
    .union(audit::Capability::PRE_ADMISSION_BYTES)
    .union(audit::Capability::PRE_ADMISSION_CHUNKS)
    .union(audit::Capability::REQUEST_READY_NODES);

pub(super) enum Drive {
    Idle,
    Progress,
}

pub struct Session<
    'd,
    A: app::Runtime<'d>,
    L: rotation::Policy<'d> = session::Disabled,
    E = deadline::Disabled,
> {
    pub(super) h3: manifold::Session<'d, A::Plan>,
    pub(super) requests: book::Book<'d, A::Prepared, A::Build, A::Task, A::Duplex>,
    pub(super) rotation: L::Drain,
    pub(super) deadline: E,
}

impl<'d, A: app::Runtime<'d>, L: rotation::Policy<'d>, E: State<'d>> Session<'d, A, L, E> {
    pub(super) fn try_new<P: deadline::Policy<'d, State = E>>(
        config: &server::Setup,
        policy: &P,
        seed: hash::Seed,
    ) -> Result<Self, Build> {
        let mut h3 =
            manifold::Session::from_seed(event::Role::Server, config.protocol.clone(), seed)
                .map_err(Build::Protocol)?;
        let requests = book::Book::try_new(config.requests).map_err(Build::Requests)?;
        let deadline = policy
            .try_state(config.requests.capacity())
            .map_err(Build::Deadlines)?;
        schedule::Credit::new(h3.h3_mut()).hold();
        Ok(Self {
            h3,
            requests,
            rotation: L::drain(),
            deadline,
        })
    }

    /// Reads the request phase under its connection's resident identity proof.
    pub fn phase<const ID: u8, R: stream::Recv, const RECONCILE: bool>(
        &self,
        quic: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
        id: Id,
    ) -> Result<Phase, manifold::Error> {
        let Some(origin) = self.h3.h3().origin() else {
            return Ok(Phase::Missing);
        };
        let mut streams = quic.streams();
        let slots = streams.resident();
        slots.owner(origin)?;
        let Some(handle) = slots.handle(id) else {
            return Ok(Phase::Missing);
        };
        let Some(owner) = requests::Identity::new(handle.local()) else {
            return Ok(Phase::Missing);
        };
        let Some(index) = streams.binding().try_slot(handle)? else {
            return Ok(Phase::Missing);
        };
        Ok(self.requests.phase(owner, index))
    }

    pub(super) fn expire_due<
        const ID: u8,
        O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
        R,
        const RECONCILE: bool,
    >(
        &mut self,
        mut app: pin::Pin<&mut A>,
        life: &mut O,
        conn: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
        handle: id::Handle<id::Driver<'d>, ID>,
        now: clock::Moment<'d>,
    ) -> Result<Drive, deadline::Expire>
    where
        O::Retention: owners::Handoff,
        A: app::Runtime<'d, Target = server::Handle<'d, ID>>,
        R: stream::Retain<'d>,
    {
        let due = match E::poll_due(&mut self.deadline, now) {
            Poll::Idle => return Ok(Drive::Idle),
            Poll::Renewed => return Ok(Drive::Progress),
            Poll::Expired(due) => due,
        };
        let Some(item) = self.requests.resolve_due(due) else {
            return Ok(Drive::Progress);
        };
        let id = item.id();
        let flow = exchange::Id::new(handle, id.stream_id());
        let expiry = flow.begin_expiry(life, item.phase());
        let resident = self
            .bind(conn, id.local())
            .map_err(deadline::Expire::Target)?;
        let held = self.requests.resume_head(id);
        self.h3
            .ingress()
            .reject(conn, id.stream_id())
            .map_err(deadline::Expire::Credit)?;
        let code = frame::ErrorCode::RequestCancelled;
        let reason = errors::Code::from_u16(code.wire_code());
        conn.streams()
            .try_stop(resident, reason)
            .map_err(|error| deadline::Expire::Stop(error.into()))?;
        Projection::new(life, handle).reset_bound(conn, resident, code);
        if let Some(claim) = held {
            claim.discard();
        } else {
            let proof = life.body(flow);
            let stop = self
                .requests
                .stop(id, proof)
                .map_err(deadline::Expire::Request)?;
            match stop {
                Stop::Task(task) => {
                    app::Runtime::cancel(app.as_mut(), task);
                    life.observe(life::Event::TaskCancelled(flow));
                }
                Stop::Exchange(_) | Stop::Ignored => {}
            }
        }
        let retired = finish::release(self, conn, id, now).map_err(|error| match error {
            Retire::Target(error) => deadline::Expire::Target(error),
            Retire::Schedule(error) => deadline::Expire::Schedule(error),
        });
        expiry.finish(life);
        retired?;
        Ok(Drive::Progress)
    }

    pub(in crate::manifold::server) fn bind<'scope, const ID: u8, R, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'scope, ID, R, RECONCILE, resident::Shared<'d>>,
        local: Local,
    ) -> Result<Handle<'scope>, manifold::Error>
    where
        R: stream::Recv,
    {
        self.h3.bind(quic, local)
    }

    pub(in crate::manifold::server) fn finish<const ID: u8, R, O, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
        id: requests::Id,
        life: &mut O,
        flow: exchange::Id<id::Handle<id::Driver<'d>, ID>, Id>,
        outcome: life::Outcome,
        now: clock::Moment<'d>,
    ) -> Result<(), Retire>
    where
        R: stream::Recv,
        O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    {
        let retired = finish::release(self, quic, id, now);
        flow.retire(life, outcome);
        retired
    }

    pub(in crate::manifold::server) fn flush_h3<
        const ID: u8,
        O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
        R,
        const RECONCILE: bool,
    >(
        &mut self,
        life: &mut O,
        quic: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
        handle: id::Handle<id::Driver<'d>, ID>,
    ) -> Result<Flush, egress::Error>
    where
        R: stream::Retain<'d>,
    {
        let report = self
            .h3
            .egress()
            .flush_with(quic, life, handle)
            .map_err(egress::Error::Protocol)?;
        let flush = report.flush();
        self.account(report).map_err(egress::Error::Request)?;
        Ok(flush)
    }

    /// Retires this session's roots; the runtime-wide task budget is not observable here.
    pub(in crate::manifold::server) fn cancel_owned<const ID: u8, O>(
        self,
        mut app: pin::Pin<&mut A>,
        life: &mut O,
        handle: id::Handle<id::Driver<'d>, ID>,
    ) -> audit::Snapshot
    where
        A: app::Runtime<'d, Target = server::Handle<'d, ID>>,
        O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    {
        let Self {
            h3,
            requests,
            rotation,
            deadline,
        } = self;
        let closed = owners::Projection::new(life, handle).close(h3);
        let snapshot = requests.drain_owned(|id, drained, ledger| {
            let flow = exchange::Id::new(handle, id.stream_id());
            match drained {
                Drained::Task(task) => {
                    app::Runtime::cancel(app.as_mut(), task);
                    life.observe(life::Event::TaskCancelled(flow));
                }
                Drained::Exchange { pending } => {
                    if let Some(pending) = pending
                        && let credit::Discard::Rejected { release, error } =
                            pending.try_discard(ledger, life.body(flow))
                    {
                        life.diagnostic(&error);
                        release.discard(life.body(flow));
                    }
                }
            }
            flow.retire(life, life::Outcome::Cancelled);
        });
        let owners = usize::from(!snapshot.is_zero(REQUEST_ROOTS));
        let snapshot = closed.apply(
            snapshot
                .owner_bytes(owners)
                .owners(owners)
                .live_tasks(0)
                .task_slots(0),
        );
        drop((rotation, deadline));
        snapshot
    }

    pub(in crate::manifold::server) fn has_work(&self) -> bool {
        schedule::Status::new(self.h3.h3()).has_work()
            || self.requests.has_ready()
            || self.egress_work().is_some()
    }

    pub(super) fn egress_work(&self) -> Option<egress::Work> {
        let h3 = self.h3.h3();
        match (
            h3.queues.has_write(&h3.qpack.feedback),
            h3.session.write_gate(),
            self.requests.has_blocked(),
        ) {
            (true, Gate::Open, _) => Some(egress::Work::Flush),
            (false, _, true) => Some(egress::Work::Resume),
            _ => None,
        }
    }

    pub(super) fn account(&mut self, report: Report) -> Result<(), requests::Error> {
        if report.has_release() || report.flush() == Flush::Drained {
            self.requests.resume()?;
        }
        Ok(())
    }

    pub(in crate::manifold::server) fn finish_input<
        const ID: u8,
        O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
        R: stream::Recv,
        const RECONCILE: bool,
    >(
        &mut self,
        app: &A,
        life: &mut O,
        quic: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
        handle: id::Handle<id::Driver<'d>, ID>,
        id: requests::Id,
        now: clock::Moment<'d>,
    ) where
        O::Retention: owners::Handoff,
    {
        let stream = id.stream_id();
        let finished = self.requests.try_finish(id, |phase| {
            if phase != Phase::Stopped {
                life.observe(life::Event::InputFinished(exchange::Id::new(
                    handle, stream,
                )));
            }
            life.body(exchange::Id::new(handle, stream))
        });
        match finished {
            Ok(Termination::Completed(done)) => finish::accept(
                &mut self.h3,
                now.at(&mut self.deadline),
                app,
                owners::Projection::new(life, handle),
                quic,
                id,
                done,
            ),
            Ok(Termination::InputFinished | Termination::Deferred | Termination::Retained) => {}
            Ok(Termination::Cancelled) => {
                let retired =
                    finish::removed(&mut self.h3, now.at(&mut self.deadline), quic, id, false);
                exchange::Id::new(handle, stream).retire(life, life::Outcome::Cancelled);
                if let Err(error) = retired {
                    manifold::close_connection_scoped(quic, error.code());
                }
            }
            Ok(Termination::Removed) => {
                if let Err(error) =
                    finish::removed(&mut self.h3, now.at(&mut self.deadline), quic, id, false)
                {
                    manifold::close_connection_scoped(quic, error.code());
                }
            }
            Err(Error::Request(requests::Error::Lookup(error))) => life.diagnostic(&error),
            Err(Error::Request(requests::Error::Identity)) => {}
            Err(Error::Request(error)) => {
                let code = server::request_error_code(error);
                self.reject(life, quic, handle, id, code, now)
            }
            Err(Error::Release { body, release }) => {
                life.diagnostic(&body);
                life.diagnostic(&release);
                manifold::close_connection_scoped(quic, frame::ErrorCode::Internal)
            }
        }
    }
    pub(in crate::manifold::server) fn reject<
        const ID: u8,
        O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
        R: stream::Recv,
        const RECONCILE: bool,
    >(
        &mut self,
        life: &mut O,
        quic: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
        handle: id::Handle<id::Driver<'d>, ID>,
        id: requests::Id,
        code: frame::ErrorCode,
        now: clock::Moment<'d>,
    ) where
        O::Retention: owners::Handoff,
    {
        let stream = id.stream_id();
        let resident = match self.bind(quic, id.local()) {
            Ok(resident) => resident,
            Err(error) => {
                manifold::close_connection_scoped(quic, error.code());
                return;
            }
        };
        if let Err(error) = self.h3.ingress().reject(quic, stream) {
            life.diagnostic(&error);
            manifold::close_connection_scoped(quic, error.code());
            return;
        }
        let app = errors::Code::from_u16(code.wire_code());
        if let Err(error) = quic.streams().try_stop(resident, app) {
            let error: manifold::Error = error.into();
            manifold::close_connection_scoped(quic, error.code());
            return;
        }
        let proof = life.body(exchange::Id::new(handle, stream));
        match self.requests.reset(id, proof) {
            Ok(Reset::Cancelled | Reset::Missing) => {}
            Err(error) => {
                manifold::close_connection_scoped(quic, server::request_error_code(error));
                return;
            }
        }
        Projection::new(life, handle).reset_bound(quic, resident, code);
        if let Err(error) = self.finish(
            quic,
            id,
            life,
            exchange::Id::new(handle, stream),
            life::Outcome::Rejected,
            now,
        ) {
            manifold::close_connection_scoped(quic, error.code());
        }
    }

    pub(in crate::manifold::server) fn reject_fresh<
        'scope,
        const ID: u8,
        O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
        R: stream::Recv,
        const RECONCILE: bool,
    >(
        &mut self,
        life: &mut O,
        quic: &mut scope::Access<'scope, ID, R, RECONCILE, resident::Shared<'d>>,
        handle: id::Handle<id::Driver<'d>, ID>,
        resident: Handle<'scope>,
        code: frame::ErrorCode,
    ) where
        O::Retention: owners::Handoff,
    {
        let stream = resident.id();
        let flow = exchange::Id::new(handle, stream);
        life.observe(life::Event::ExchangeOpened(flow));
        let rejected = (|| -> Result<(), manifold::Error> {
            self.h3.ingress().reject(quic, stream)?;
            let app = errors::Code::from_u16(code.wire_code());
            quic.streams().try_stop(resident, app)?;
            Projection::new(life, handle).reset_bound(quic, resident, code);
            Ok(())
        })();
        flow.retire(life, life::Outcome::Rejected);
        if let Err(error) = rejected {
            manifold::close_connection_scoped(quic, error.code());
        }
    }
}
