use dope_quic::conn::id;

use crate::manifold::server::deadline::state::State;
pub(in crate::manifold::server) mod ingress;
pub(super) mod receive;
use std::pin;

use conn::scope;
use dope::core::driver::schedule::{
    clock,
    ready::{self, task::owner},
};
use dope_quic::{conn, stream, wire::stream::Id};
use o3::buffer::resident;
use owners::Projection;
use requests::task::{self, Close};
use sark_core::{
    deadline::timeline,
    life::{self, exchange},
    manifold::task::{app, response::Completion},
    route,
};

use crate::{
    frame,
    manifold::{
        self, encoder, owners, requests,
        server::{self, deadline, rotation},
    },
};

enum Dispatch {
    Idle,
    Progress,
}

/// Borrowed session task dispatch and completion.
#[repr(transparent)]
pub(in crate::manifold::server) struct Ready<
    'a,
    'd,
    A: app::Runtime<'d>,
    L: rotation::Policy<'d>,
    E: State<'d>,
> {
    pub(super) state: &'a mut server::Session<'d, A, L, E>,
}

impl<'a, 'd, A: app::Runtime<'d>, L: rotation::Policy<'d>, E: State<'d>> Ready<'a, 'd, A, L, E> {
    pub(super) fn deadline(&mut self) -> Option<clock::Deadline<'d>> {
        let drain = rotation::State::deadline(&self.state.rotation);
        let entry = E::deadline(&self.state.deadline);
        match (drain, entry) {
            (Some(drain), Some(entry)) => Some(drain.min(entry)),
            (Some(drain), None) => Some(drain),
            (None, Some(entry)) => Some(entry),
            (None, None) => None,
        }
    }

    fn dispatch<
        const ID: u8,
        O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
        B: stream::Recv,
        const RECONCILE: bool,
    >(
        &mut self,
        mut router: pin::Pin<&mut A>,
        life: &mut O,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        handle: id::Handle<id::Driver<'d>, ID>,
        parent: ready::Target<'d>,
        now: clock::Moment<'d>,
    ) -> Dispatch
    where
        O::Retention: owners::Handoff,
        A: app::Runtime<'d, Target = server::Handle<'d, ID>>,
    {
        let owner = owner::Owner::from(handle);
        let done = self.state.requests.dispatch_one(|id, input| {
            let target = server::Handle::from_request(handle, id);
            match input {
                task::Input::Built { output } => {
                    app::Runtime::try_built(router.as_mut(), output, target, owner, parent)
                }
                task::Input::Buffered(req) => {
                    app::Runtime::try_dispatch(router.as_mut(), req, target, owner, parent)
                }
            }
        });
        let done = match done {
            Ok(done) => done,
            Err(error) => {
                manifold::close_connection_scoped(quic, server::request_error_code(error));
                return Dispatch::Progress;
            }
        };
        match done {
            task::Dispatch::Idle => Dispatch::Idle,
            task::Dispatch::Active { key: id } => {
                life.observe(life::Event::TaskAdmitted(exchange::Id::new(
                    handle,
                    id.stream_id(),
                )));
                if let Err(error) = E::transition(
                    &mut self.state.deadline,
                    id,
                    timeline::Transition::HandlerStarted,
                    now,
                ) {
                    manifold::close_connection_scoped(quic, deadline::code(error));
                    return Dispatch::Progress;
                }
                Dispatch::Progress
            }
            task::Dispatch::Rejected { key: id, error } => {
                let code = match A::rejection(&error) {
                    route::Reject::Unavailable => frame::ErrorCode::ExcessiveLoad,
                    route::Reject::BadRequest => frame::ErrorCode::Message,
                    route::Reject::Internal => frame::ErrorCode::Internal,
                };
                let stream = id.stream_id();
                Projection::new(life, handle).reset(quic, id.target(), code);
                if let Err(error) = self.state.finish(
                    quic,
                    id,
                    life,
                    exchange::Id::new(handle, stream),
                    life::Outcome::Rejected,
                    now,
                ) {
                    manifold::close_connection_scoped(quic, error.code());
                }
                Dispatch::Progress
            }
        }
    }

    pub(in crate::manifold) fn finish_task<
        const ID: u8,
        R,
        O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
        B,
        const RECONCILE: bool,
    >(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
        handle: id::Handle<id::Driver<'d>, ID>,
        id: requests::Id,
        output: R,
        life: &mut O,
        now: clock::Moment<'d>,
    ) where
        R: Completion<'d>,
        B: stream::Retain<'d>,
        O::Retention: owners::Handoff,
    {
        let stream = id.stream_id();
        let method = match self.state.requests.close_task(id) {
            Ok(Close::Closed(method)) => method,
            Ok(Close::Missing) => return,
            Err(error) => {
                manifold::close_connection_scoped(quic, server::request_error_code(error));
                return;
            }
        };
        if let Err(error) = E::transition(
            &mut self.state.deadline,
            id,
            timeline::Transition::ResponseReady,
            now,
        ) {
            let retired = self.state.finish(
                quic,
                id,
                life,
                exchange::Id::new(handle, stream),
                life::Outcome::Rejected,
                now,
            );
            manifold::close_connection_scoped(quic, deadline::code(error));
            if let Err(error) = retired {
                manifold::close_connection_scoped(quic, error.code());
            }
            return;
        }
        let target = id.target();
        let flow = exchange::Id::new(handle, stream);
        let req_id = output.id();
        let enc = encoder::Encoder::task(self.state.h3.h3_mut(), target, method, req_id);
        let sent = output.emit(enc);
        use owners::Commit;
        let commit = match sent {
            Err((cause, target)) => Err((Commit::code(cause), target.into_target())),
            Ok((enc, outcome)) => <O::Retention as owners::Handoff>::resolve(enc)
                .map(|done| (done, outcome))
                .map_err(|code| (code, target)),
        };
        let (commit, outcome) = match commit {
            Ok(done) => done,
            Err((code, target)) => {
                Projection::new(life, handle).reset(quic, target, code);
                if let Err(error) =
                    self.state
                        .finish(quic, id, life, flow, life::Outcome::Rejected, now)
                {
                    manifold::close_connection_scoped(quic, error.code());
                }
                return;
            }
        };
        commit.observe(life, flow);
        if let Err(error) = self.state.finish(quic, id, life, flow, outcome, now) {
            manifold::close_connection_scoped(quic, error.code());
            return;
        }
        if let Err(error) = self.state.flush_h3(life, quic, handle) {
            manifold::close_connection_scoped(quic, error.code());
        }
    }
}
