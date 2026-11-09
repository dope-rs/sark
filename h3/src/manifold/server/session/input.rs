use book::input;
use dope_quic::{conn::id, stream, wire::stream::Id};
use requests::book;
use sark_core::{
    deadline::timeline,
    life::{self, exchange},
    manifold::{stream::Boundary, task::app},
};

use crate::{
    frame,
    manifold::{
        self, Phase, owners, requests,
        server::{
            self, deadline, deadline::state::State, failure, rotation, task::ingress::item::Item,
        },
    },
};

pub(in crate::manifold::server) fn push<'d, A, L, E, const ID: u8, O, R, const RECONCILE: bool>(
    item: Item<'_, '_, 'd, A, O, L, E, R, ID, RECONCILE>,
    id: requests::Id,
    data: crate::Payload<'d>,
    boundary: Boundary,
) where
    A: app::Runtime<'d, Target = server::Handle<'d, ID>>,
    L: rotation::Policy<'d>,
    E: State<'d>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>
        + failure::Sink<id::Handle<id::Driver<'d>, ID>>,
    O::Retention: owners::Handoff,
    R: stream::Retain<'d>,
{
    let Item {
        state,
        app,
        life,
        conn: quic,
        handle,
        now,
    } = item;
    let app = app.into_ref().get_ref();
    let bytes = data.len();
    let progress = bytes != 0;
    let span = exchange::Id::new(handle, id.stream_id());
    let proof = life.body(span);
    let pushed = state.requests.push(id, data, boundary, proof);
    let done = match pushed {
        input::Push::Accepted { completion: done } => {
            // A building request retains the stream's existing DATA barrier
            // until its next NeedInput transition. Other streams stay runnable.
            let released = if matches!(
                state.requests.phase(id.identity(), id.slot()),
                Phase::Building
            ) {
                state.h3.ingress().release(quic, id.stream_id(), bytes)
            } else {
                state.h3.ingress().consumed(quic, id.stream_id(), bytes)
            };
            if let Err(error) = released {
                manifold::close_connection_scoped(quic, error.code());
                return;
            }
            done
        }
        input::Push::Queued => Boundary::More,
        input::Push::Inactive {
            phase: Phase::Stopped,
            ..
        } => {
            if let Err(error) = state.h3.ingress().consumed(quic, id.stream_id(), bytes) {
                manifold::close_connection_scoped(quic, error.code());
                return;
            }
            if matches!(boundary, Boundary::End) {
                state.finish_input(app, life, quic, handle, id, now);
            }
            return;
        }
        input::Push::Inactive {
            phase: Phase::Reserved,
            ..
        } => {
            state.reject(life, quic, handle, id, frame::ErrorCode::Message, now);
            return;
        }
        input::Push::Inactive { .. } => {
            manifold::close_connection_scoped(quic, frame::ErrorCode::Internal);
            return;
        }
        input::Push::Rejected(error) => {
            manifold::close_connection_scoped(quic, server::request_error_code(error));
            return;
        }
        input::Push::Failed(error) => {
            let code = server::input_error_code::<A>(error);
            state.reject(life, quic, handle, id, code, now);
            return;
        }
    };
    match boundary {
        Boundary::More => {
            if progress
                && let Err(error) = E::transition(
                    &mut state.deadline,
                    id,
                    timeline::Transition::BodyProgress,
                    now,
                )
            {
                manifold::close_connection_scoped(quic, deadline::code(error));
            }
            return;
        }
        Boundary::End => {}
    }
    if let Err(error) = E::transition(
        &mut state.deadline,
        id,
        timeline::Transition::InputFinished,
        now,
    ) {
        manifold::close_connection_scoped(quic, deadline::code(error));
        return;
    }
    if matches!(done, Boundary::End) {
        state.finish_input(app, life, quic, handle, id, now);
    } else {
        life.observe(life::Event::InputFinished(span));
    }
}
