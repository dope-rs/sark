use std::pin;

use conn::{
    scope,
    stream::{access::bindings, resident::Handle},
};
use dope::core::driver::schedule::clock;
use dope_quic::{
    conn,
    conn::id,
    errors, stream,
    wire::stream::{Direction, Id},
};
use o3::{buffer::resident, depot::arena::hash::errors::Create};
use requests::{
    entries,
    exchange::{Reset, Stop},
};
use sark_core::{
    deadline::timeline,
    life::{self, Observer as _, exchange},
    manifold::{stream::Section, task::app::Runtime},
};
use sark_http::field::metadata::{
    block,
    section::{
        self,
        state::{RetainedHead, Selected},
    },
};
use session::{finish, input};

use crate::{
    conn::{event, io},
    frame,
    manifold::{
        self, Diagnostic, owners, requests,
        server::{
            self, deadline, deadline::state::State, rotation, session, task::ingress::dispatch,
        },
    },
};

enum Slot {
    Bound(u32),
    Vacant,
}

enum Presence<'scope> {
    None,
    Active {
        handle: Handle<'scope>,
        identity: requests::Identity,
        slot: Slot,
        finished: bool,
    },
    Other {
        handle: Handle<'scope>,
    },
}

enum Resolve {
    NotRequest,
    Unbound(requests::Identity),
    Stale(requests::Identity, u32),
}

enum Target<'scope> {
    Request {
        handle: Handle<'scope>,
        id: requests::Id,
    },
    Other(Handle<'scope>),
    Retired,
    None,
}

enum StopFailure<'scope> {
    Create {
        handle: Handle<'scope>,
        error: Create<entries::Tag>,
    },
    Binding(manifold::Error),
}

impl<'scope> Presence<'scope> {
    fn active(handle: Handle<'scope>, slot: Slot, finished: bool) -> Self {
        let Some(identity) = requests::Identity::new(handle.local()) else {
            return Self::Other { handle };
        };
        Self::Active {
            handle,
            identity,
            slot,
            finished,
        }
    }
}

pub(in crate::manifold::server) struct Item<
    'a,
    'scope,
    'd,
    R,
    O,
    L,
    E,
    B,
    const ID: u8,
    const RECONCILE: bool,
> where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    L: rotation::Policy<'d>,
    E: State<'d>,
    B: stream::Recv,
{
    pub(in crate::manifold::server) state: &'a mut server::Session<'d, R, L, E>,
    pub(in crate::manifold::server) app: pin::Pin<&'a mut R>,
    pub(in crate::manifold::server) life: &'a mut O,
    pub(in crate::manifold::server) conn:
        &'a mut scope::Access<'scope, ID, B, RECONCILE, resident::Shared<'d>>,
    pub(in crate::manifold::server) handle: id::Handle<id::Driver<'d>, ID>,
    pub(in crate::manifold::server) now: clock::Moment<'d>,
}

impl<'a, 'scope, 'd, R, O, L, E, B, const ID: u8, const RECONCILE: bool>
    Item<'a, 'scope, 'd, R, O, L, E, B, ID, RECONCILE>
where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    O::Retention: owners::Handoff,
    L: rotation::Policy<'d>,
    E: State<'d>,
    B: stream::Recv,
{
    pub(in crate::manifold::server) const fn new(
        state: &'a mut server::Session<'d, R, L, E>,
        app: pin::Pin<&'a mut R>,
        life: &'a mut O,
        conn: &'a mut scope::Access<'scope, ID, B, RECONCILE, resident::Shared<'d>>,
        handle: id::Handle<id::Driver<'d>, ID>,
        now: clock::Moment<'d>,
    ) -> Self {
        Self {
            state,
            app,
            life,
            conn,
            handle,
            now,
        }
    }

    pub(in crate::manifold::server::task) fn step(self)
    where
        B: stream::Retain<'d>,
    {
        if self.conn.view().lifecycle().is_closing() {
            return;
        }
        let step = match self
            .state
            .h3
            .ingress()
            .poll(self.conn, self.life, self.handle)
        {
            Ok(step) => step,
            Err(error) => {
                self.life.diagnostic(&error);
                manifold::close_connection_scoped(self.conn, error.code());
                return;
            }
        };
        match step {
            io::Step::Item(event) => self.run(event),
            io::Step::More | io::Step::Idle => {}
        }
    }

    fn rejected(mut self, resident: Presence<'scope>, error: block::ContentError) {
        let (resident, id) = match self.resolve(&resident) {
            Ok(bound) => bound,
            Err(error) => {
                self.reject(error);
                return;
            }
        };
        let stream = id.stream_id();
        self.life.diagnostic(&crate::conn::Error::Content {
            stream_id: stream.get(),
            error,
        });
        if let Err(error) = self.state.h3.ingress().reject(self.conn, stream) {
            manifold::close_connection_scoped(self.conn, error.code());
            return;
        }
        let (retired, failure) = match self.state.h3.h3_mut().queues.discard(stream) {
            Ok(retired) => (retired, None),
            Err((error, retired)) => (retired, Some(error)),
        };
        owners::Projection::new(self.life, self.handle).discarded(
            retired,
            stream,
            stream::send::admit::Cause::Reset,
        );
        if let Some(error) = failure {
            manifold::close_connection_scoped(self.conn, error.error_code());
            return;
        }
        let code = frame::ErrorCode::Message;
        if let Err(error) = self
            .conn
            .streams()
            .try_stop(resident, errors::Code::from_u16(code.wire_code()))
        {
            let error: manifold::Error = error.into();
            manifold::close_connection_scoped(self.conn, error.code());
            return;
        }
        owners::Projection::new(self.life, self.handle).reset_bound(self.conn, resident, code);
        self.retire(id, life::Outcome::Rejected);
    }

    fn run(self, item: manifold::Delivery<'scope, 'd, R::Plan>)
    where
        B: stream::Retain<'d>,
    {
        let error = 'dispatch: {
            let manifold::Delivery { event, resident } = item;
            let resident = match resident {
                manifold::Resident::Active { handle, slot } => {
                    Presence::active(handle, Slot::Bound(slot), false)
                }
                manifold::Resident::Unbound(handle) => {
                    Presence::active(handle, Slot::Vacant, false)
                }
                manifold::Resident::Ending(handle) => {
                    let slot = match self.conn.streams().binding().try_slot(handle) {
                        Ok(Some(slot)) => Slot::Bound(slot),
                        Ok(None) => Slot::Vacant,
                        Err(fault) => {
                            let error: manifold::Error = fault.into();
                            break 'dispatch error.code();
                        }
                    };
                    Presence::active(handle, slot, true)
                }
                manifold::Resident::None => Presence::None,
            };
            if let Presence::Active {
                handle,
                slot: Slot::Vacant,
                finished: true,
                ..
            }
            | Presence::Other { handle } = &resident
                && (matches!(
                    &event,
                    event::Event::Finished { .. } | event::Event::Reset { .. }
                ) || matches!(&event, event::Event::Stopped { .. })
                    && !io::ingress::schedule::Status::new(self.state.h3.h3())
                        .retains(handle.local()))
            {
                if let Err(error) = self.conn.streams().binding().try_release(*handle) {
                    let error: manifold::Error = error.into();
                    break 'dispatch error.code();
                }
                return;
            }
            match event {
                event::Event::Headers {
                    message,
                    head,
                    section,
                } => return self.headers(resident, message, head, section),
                event::Event::Data { message, data } => {
                    io::ingress::schedule::Credit::new(self.state.h3.h3_mut())
                        .delivered(message.stream_id());
                    return self.data(resident, message, data);
                }
                event::Event::Finished { .. } => return self.finish(resident),
                event::Event::Reset { .. } => return self.reset(resident),
                event::Event::Rejected { error, .. } => return self.rejected(resident, error),
                event::Event::Stopped {
                    stream_id: stream, ..
                } => return self.stopped(resident, stream),
                event::Event::CancelPush { .. } => frame::ErrorCode::Id,
                event::Event::PushData { .. }
                | event::Event::PushPromise { .. }
                | event::Event::PushHeaders { .. } => frame::ErrorCode::FrameUnexpected,
                event::Event::QpackCancelled { .. } => unreachable!("cancel"),
                event::Event::Settings(_) | event::Event::GoAway { .. } => return,
            }
        };
        manifold::close_connection_scoped(self.conn, error);
    }

    fn headers(
        mut self,
        resident: Presence<'scope>,
        message: event::Message,
        head: RetainedHead<
            <R::Plan as section::Policy>::Block,
            Selected<R::Plan>,
            Option<block::Plan>,
        >,
        section: Section,
    ) {
        let id = match self.resolve(&resident) {
            Ok((_, id)) => id,
            Err(error) => {
                self.reject(error);
                return;
            }
        };
        dispatch::accept(self, id, message, head, section)
    }

    fn data(mut self, resident: Presence<'scope>, message: event::Message, data: crate::Payload<'d>)
    where
        B: stream::Retain<'d>,
    {
        let error = match (self.resolve(&resident), data) {
            (Ok((_, id)), data) => return input::push(self, id, data, message.boundary()),
            (Err(error), _) => error,
        };
        self.reject(error);
    }

    fn finish(mut self, resident: Presence<'scope>) {
        let id = match self.resolve(&resident) {
            Ok((_, id)) => id,
            Err(error) => {
                self.reject(error);
                return;
            }
        };
        if let Err(error) = E::transition(
            &mut self.state.deadline,
            id,
            timeline::Transition::InputFinished,
            self.now,
        ) {
            manifold::close_connection_scoped(self.conn, deadline::code(error));
            return;
        }
        self.state.finish_input(
            self.app.into_ref().get_ref(),
            self.life,
            self.conn,
            self.handle,
            id,
            self.now,
        );
    }

    fn reset(mut self, resident: Presence<'scope>) {
        let id = match self.resolve(&resident) {
            Ok((_, id)) => id,
            Err(Resolve::NotRequest | Resolve::Unbound(_) | Resolve::Stale(..)) => return,
        };
        self.retire(id, life::Outcome::Cancelled);
    }

    fn retire(mut self, id: requests::Id, outcome: life::Outcome) {
        let stream = id.stream_id();
        let task = match self.state.requests.take_task(id) {
            Ok(task) => task,
            Err(error) => {
                manifold::close_connection_scoped(self.conn, server::request_error_code(error));
                return;
            }
        };
        let span = exchange::Id::new(self.handle, stream);
        match task {
            Some(task) => {
                Runtime::cancel(self.app.as_mut(), task);
                self.life.observe(life::Event::TaskCancelled(span));
            }
            None => {
                let proof = self.life.body(span);
                match self.state.requests.reset(id, proof) {
                    Ok(Reset::Cancelled) => {}
                    Ok(Reset::Missing) => {
                        if let Err(error) = finish::release(self.state, self.conn, id, self.now) {
                            manifold::close_connection_scoped(self.conn, error.code());
                        }
                        return;
                    }
                    Err(error) => {
                        manifold::close_connection_scoped(
                            self.conn,
                            server::request_error_code(error),
                        );
                        return;
                    }
                }
            }
        }
        if let Err(error) = self
            .state
            .finish(self.conn, id, self.life, span, outcome, self.now)
        {
            manifold::close_connection_scoped(self.conn, error.code());
        }
    }

    fn stopped(mut self, resident: Presence<'scope>, stream: Id)
    where
        B: stream::Retain<'d>,
    {
        if io::ingress::schedule::Status::new(self.state.h3.h3()).rejected(stream) {
            return;
        }
        let target = match self.target(resident) {
            Ok(target) => target,
            Err(StopFailure::Create { handle, error }) => {
                finish::reject_new(self.state, self.life, self.conn, self.handle, handle, error);
                return;
            }
            Err(StopFailure::Binding(error)) => {
                manifold::close_connection_scoped(self.conn, error.code());
                return;
            }
        };
        match target {
            Target::Request { handle, id } => {
                let span = exchange::Id::new(self.handle, stream);
                let proof = self.life.body(span);
                let stop = match self.state.requests.stop(id, proof) {
                    Ok(stop) => stop,
                    Err(error) => {
                        manifold::close_connection_scoped(
                            self.conn,
                            server::request_error_code(error),
                        );
                        return;
                    }
                };
                self.stop_stream(handle, stream);
                match stop {
                    Stop::Task(task) => {
                        Runtime::cancel(self.app.as_mut(), task);
                        self.life.observe(life::Event::TaskCancelled(span));
                    }
                    Stop::Exchange(bytes) => {
                        if let Err(error) =
                            self.state.h3.ingress().release(self.conn, stream, bytes)
                        {
                            manifold::close_connection_scoped(self.conn, error.code());
                            return;
                        }
                    }
                    Stop::Ignored => return,
                }
                if io::ingress::schedule::Status::new(self.state.h3.h3()).data_held(stream)
                    && let Err(error) = self.state.h3.ingress().resume(self.conn, stream)
                {
                    manifold::close_connection_scoped(self.conn, error.code());
                    return;
                }
                if let Err(error) = self.state.finish(
                    self.conn,
                    id,
                    self.life,
                    span,
                    life::Outcome::Cancelled,
                    self.now,
                ) {
                    manifold::close_connection_scoped(self.conn, error.code());
                }
            }
            Target::Other(handle) => self.stop_stream(handle, stream),
            Target::Retired => {}
            Target::None => {
                if matches!(stream.direction(), Direction::Bidi) {
                    manifold::close_connection_scoped(self.conn, frame::ErrorCode::Internal);
                }
            }
        }
    }

    fn target(
        &mut self,
        resident: Presence<'scope>,
    ) -> Result<Target<'scope>, StopFailure<'scope>> {
        match resident {
            Presence::None => Ok(Target::None),
            Presence::Other { handle } => Ok(Target::Other(handle)),
            Presence::Active {
                handle,
                identity,
                slot,
                ..
            } => match slot {
                Slot::Bound(slot) => match self.state.requests.resolve(identity, slot, false) {
                    Some(id) => Ok(Target::Request { handle, id }),
                    None => self.stop_new(handle, identity),
                },
                Slot::Vacant => self.stop_new(handle, identity),
            },
        }
    }

    fn stop_new(
        &mut self,
        handle: Handle<'scope>,
        identity: requests::Identity,
    ) -> Result<Target<'scope>, StopFailure<'scope>> {
        if self
            .conn
            .streams()
            .try_recv_eof(handle)
            .map_err(|error| StopFailure::Binding(error.into()))?
        {
            self.state
                .h3
                .ingress()
                .discard(self.conn, handle.id())
                .map_err(StopFailure::Binding)?;
            self.conn
                .streams()
                .binding()
                .try_release(handle)
                .map_err(|error| StopFailure::Binding(error.into()))?;
            return Ok(Target::Retired);
        }
        let id = match self.state.requests.insert_stopped(identity) {
            Ok(id) => id,
            Err(error) => return Err(StopFailure::Create { handle, error }),
        };
        match self.conn.streams().binding().try_bind(handle, id.slot()) {
            Ok(bindings::Bind::Added | bindings::Bind::Present) => {
                Ok(Target::Request { handle, id })
            }
            Err(fault) => Err(StopFailure::Binding(fault.into())),
        }
    }

    fn stop_stream(&mut self, resident: Handle<'scope>, stream: Id) {
        if !matches!(stream.direction(), Direction::Bidi) {
            return;
        }
        let code = frame::ErrorCode::RequestCancelled;
        let app = errors::Code::from_u16(code.wire_code());
        if let Err(error) = self.conn.streams().try_stop(resident, app) {
            let error: manifold::Error = error.into();
            manifold::close_connection_scoped(self.conn, error.code());
        }
    }

    fn reject(&mut self, error: Resolve) {
        if let Some(observer) = self.life.observer() {
            let cause = match error {
                Resolve::NotRequest => Diagnostic::ResolveNone,
                Resolve::Unbound(identity) => Diagnostic::Unbound {
                    stream: identity.stream_id().get(),
                    resident: identity.local().key().raw(),
                },
                Resolve::Stale(identity, slot) => Diagnostic::Stale {
                    stream: identity.stream_id().get(),
                    resident: identity.local().key().raw(),
                    slot,
                },
            };
            observer.diagnostic(&cause);
        }
        manifold::close_connection_scoped(self.conn, frame::ErrorCode::Internal);
    }

    fn resolve(
        &mut self,
        resident: &Presence<'scope>,
    ) -> Result<(Handle<'scope>, requests::Id), Resolve> {
        match resident {
            Presence::Active {
                handle,
                identity,
                slot: Slot::Bound(slot),
                finished,
                ..
            } => self
                .state
                .requests
                .resolve(*identity, *slot, *finished)
                .map(|id| (*handle, id))
                .ok_or(Resolve::Stale(*identity, *slot)),
            Presence::Active {
                identity,
                slot: Slot::Vacant,
                ..
            } => Err(Resolve::Unbound(*identity)),
            Presence::None | Presence::Other { .. } => Err(Resolve::NotRequest),
        }
    }
}
