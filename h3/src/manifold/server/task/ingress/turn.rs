use dope::core::driver::schedule::ready;
use dope_quic::{conn::id, stream, wire::stream::Id};
use sark_core::{
    body, life,
    life::{Observer as _, exchange},
    manifold::task::app::Runtime,
};

use crate::{
    conn::io::ingress::schedule,
    frame,
    manifold::{
        self, owners,
        requests::entries::Failure,
        server::{
            self, deadline,
            deadline::state::State,
            rotation,
            session::{self, egress::Work, status},
            task::{Dispatch, Ready, ingress::item::Item},
        },
    },
};

pub(in crate::manifold::server) fn drive<
    'scope,
    'd,
    R,
    O,
    L,
    E,
    B,
    const ID: u8,
    const RECONCILE: bool,
>(
    item: Item<'_, 'scope, 'd, R, O, L, E, B, ID, RECONCILE>,
    lifetime: &L,
    parent: ready::Target<'d>,
) where
    R: Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    O::Retention: owners::Handoff,
    L: rotation::Policy<'d>,
    E: State<'d>,
    B: stream::Retain<'d>,
{
    let Item {
        state,
        app: mut router,
        life,
        conn,
        handle,
        now,
    } = item;
    let mut ready = Ready { state };
    let rotation = {
        let server::Session { h3, rotation, .. } = &mut ready.state;
        rotation::State::expire(rotation, lifetime, h3, now)
    };
    match rotation {
        Ok(None) => {}
        Ok(Some(rotation::Expired::Draining)) => {
            if let Err(error) = ready.state.flush_h3(life, conn, handle) {
                manifold::close_connection_scoped(conn, error.code());
            }
            return;
        }
        Ok(Some(rotation::Expired::Closing)) => {
            manifold::close_connection_scoped(conn, frame::ErrorCode::NoError);
            return;
        }
        Err(rotation::Failure::Deadline { .. }) => {
            manifold::close_connection_scoped(conn, frame::ErrorCode::Internal);
            return;
        }
        Err(rotation::Failure::GoAway(error)) => {
            manifold::close_connection_scoped(conn, error.error_code());
            return;
        }
    }
    match ready
        .state
        .expire_due(router.as_mut(), life, conn, handle, now)
    {
        Ok(session::Drive::Idle) => {}
        Ok(session::Drive::Progress) => return,
        Err(
            deadline::Expire::Target(error)
            | deadline::Expire::Credit(error)
            | deadline::Expire::Stop(error),
        ) => {
            manifold::close_connection_scoped(conn, error.code());
            return;
        }
        Err(deadline::Expire::Request(error)) => {
            manifold::close_connection_scoped(conn, server::request_error_code(error));
            return;
        }
        Err(deadline::Expire::Schedule(error)) => {
            manifold::close_connection_scoped(conn, deadline::code(error));
            return;
        }
    }
    match ready.state.egress_work() {
        Some(Work::Flush) => {
            if let Err(error) = ready.state.flush_h3(life, conn, handle) {
                manifold::close_connection_scoped(conn, error.code());
            }
            return;
        }
        Some(Work::Resume) => {
            if let Err(error) = ready.state.requests.resume() {
                manifold::close_connection_scoped(conn, server::request_error_code(error));
            }
            return;
        }
        None => {}
    }
    if matches!(
        session::connect::drive(ready.state, life, conn, handle, now),
        session::Drive::Progress
    ) {
        return;
    }
    if matches!(
        status::drive(ready.state, life, conn, handle, now),
        session::Drive::Progress
    ) {
        return;
    }
    if matches!(
        ready.dispatch(router.as_mut(), life, conn, handle, parent, now),
        Dispatch::Progress
    ) {
        return;
    }
    let (done, input) = match ready
        .state
        .requests
        .drive(body::DEFAULT_BUILD_STEP_BUDGET, |id| {
            life.body(exchange::Id::new(handle, id.stream_id()))
        }) {
        Ok(done) => done,
        Err(error) => {
            manifold::close_connection_scoped(conn, server::request_error_code(error));
            return;
        }
    };
    if let Some(id) = input
        && schedule::Status::new(ready.state.h3.h3()).data_held(id.stream_id())
        && let Err(error) = ready.state.h3.ingress().resume(conn, id.stream_id())
    {
        manifold::close_connection_scoped(conn, error.code());
        return;
    }
    match done {
        body::BuildOutcome::Idle | body::BuildOutcome::Pending => {}
        body::BuildOutcome::Ready { build, .. } => match build {},
        body::BuildOutcome::Failed { key: id, error } => {
            if let Some(observer) = life.observer() {
                match &error {
                    Failure::Step(cause) => observer.diagnostic(cause),
                    Failure::Admit(cause) => observer.diagnostic(cause),
                    Failure::StepRelease { error, release } => {
                        observer.diagnostic(&(error, release));
                    }
                    Failure::AdmitRelease { error, release } => {
                        observer.diagnostic(&(error, release));
                    }
                }
            }
            let code = server::build_error_code::<R>(error);
            ready.state.reject(life, conn, handle, id, code, now);
            return;
        }
    }
    if schedule::Status::new(ready.state.h3.h3()).has_work() {
        Item::new(ready.state, router.as_mut(), life, conn, handle, now).step();
    }
}
