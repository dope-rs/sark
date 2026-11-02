use conn::scope;
use dope::core::driver::schedule::clock;
use dope_quic::{conn, conn::id, stream, wire::stream::Id};
use o3::buffer::resident;
use sark_core::{
    life::{self, exchange},
    manifold::task::app,
};
use sark_http::payload::response::{Response, dispatch};

use crate::manifold::{
    self, encoder, owners,
    requests::entries::status,
    server::{self, deadline::state::State, rotation, session::finish},
};

enum Outcome<T> {
    Sent(T),
    Blocked,
    Failed(crate::conn::Error),
}

pub(in crate::manifold::server) fn drive<'d, A, L, E, const ID: u8, O, R, const RECONCILE: bool>(
    state: &mut server::Session<'d, A, L, E>,
    sink: &mut O,
    quic: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
    handle: id::Handle<id::Driver<'d>, ID>,
    now: clock::Moment<'d>,
) -> super::Drive
where
    A: app::Runtime<'d>,
    L: rotation::Policy<'d>,
    E: State<'d>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    O::Retention: owners::Handoff,
    R: stream::Retain<'d>,
{
    let next = {
        let (reqs, h3) = (&mut state.requests, &mut state.h3);
        reqs.with_status(|id, method, code| {
            let enc = encoder::Encoder::new(h3.h3_mut(), id.target(), method);
            let output = dispatch::IntoOutput::into_output(Response::new(code));
            let sent = match dispatch::Immediate::emit(output, enc) {
                Ok(enc) => enc.finish(),
                Err((error, _)) => Err(error),
            };
            match sent {
                Ok(charge) => status::Next::Finished((id, Outcome::Sent(charge))),
                Err(error) if h3.h3().queues.is_pressure(&error) => {
                    status::Next::Blocked((id, Outcome::Blocked))
                }
                Err(error) => status::Next::Finished((id, Outcome::Failed(error))),
            }
        })
    };
    let ((id, done), finished) = match next {
        Ok(Some(next)) => next,
        Ok(None) => return super::Drive::Idle,
        Err(error) => {
            manifold::close_connection_scoped(quic, server::request_error_code(error));
            return super::Drive::Progress;
        }
    };
    let stream = id.stream_id();
    let flow = exchange::Id::new(handle, stream);
    let flush = match done {
        Outcome::Sent(charge) => {
            owners::Commit::new(0, charge, Default::default()).observe(sink, flow);
            true
        }
        Outcome::Blocked => {
            if let Err(error) = state.flush_h3(sink, quic, handle) {
                manifold::close_connection_scoped(quic, error.code());
            }
            return super::Drive::Progress;
        }
        Outcome::Failed(error) => {
            use owners::Projection;

            Projection::new(sink, handle).reset(quic, id.target(), error.error_code());
            false
        }
    };
    let retired = finish::removed(
        &mut state.h3,
        now.at(&mut state.deadline),
        quic,
        id,
        !finished,
    );
    flow.retire(sink, life::Outcome::Rejected);
    if let Err(error) = retired {
        manifold::close_connection_scoped(quic, error.code());
        return super::Drive::Progress;
    }
    if flush && let Err(error) = state.flush_h3(sink, quic, handle) {
        manifold::close_connection_scoped(quic, error.code());
    }
    super::Drive::Progress
}
