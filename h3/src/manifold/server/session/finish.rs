use conn::scope;
use dope::core::driver::schedule::{clock, sequence};
use dope_quic::{
    conn,
    conn::{id, stream::resident::Handle},
    stream,
    wire::stream::Id,
};
use o3::{buffer::resident, depot::arena::hash::errors::Create};
use requests::{entries, task};
use sark_core::{
    deadline::timeline,
    decode::dispatch,
    life::{self, exchange},
    manifold::task::app,
};
use sark_http::field::metadata::section::rules;

use crate::{
    frame, manifold,
    manifold::{
        Phase, encoder, owners, requests,
        server::{self, deadline, deadline::state::State, rotation, session::retire::Retire},
    },
};

pub(in crate::manifold::server) fn release<'d, A, L, E, const ID: u8, R, const RECONCILE: bool>(
    state: &mut server::Session<'d, A, L, E>,
    quic: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
    id: requests::Id,
    now: clock::Moment<'d>,
) -> Result<(), Retire>
where
    A: app::Runtime<'d>,
    L: rotation::Policy<'d>,
    E: State<'d>,
    R: stream::Recv,
{
    retained(&mut state.deadline, id, now)?;
    if state.requests.request_phase(id) == Phase::Missing {
        retire_slot(&mut state.h3, quic, id, false)?;
    }
    Ok(())
}

pub(in crate::manifold::server) fn removed<'d, P, E, const ID: u8, R, const RECONCILE: bool>(
    h3: &mut manifold::Session<'d, P>,
    mut deadline: clock::At<'d, &mut E>,
    quic: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
    id: requests::Id,
    discard_input: bool,
) -> Result<(), Retire>
where
    P: rules::Policy,
    E: State<'d>,
    R: stream::Recv,
{
    deadline.apply_mut(|deadline, now| retained(deadline, id, now))?;
    retire_slot(h3, quic, id, discard_input)
}

fn retained<'d, E: State<'d>>(
    deadline: &mut E,
    id: requests::Id,
    now: clock::Moment<'d>,
) -> Result<(), Retire> {
    E::transition(deadline, id, timeline::Transition::Retired, now).map_err(Retire::Schedule)
}

fn retire_slot<'d, P: rules::Policy, const ID: u8, R, const RECONCILE: bool>(
    h3: &mut manifold::Session<'d, P>,
    quic: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
    id: requests::Id,
    discard_input: bool,
) -> Result<(), Retire>
where
    R: stream::Recv,
{
    let resident = h3.bind(quic, id.local()).map_err(Retire::Target)?;
    if discard_input {
        let eof = quic
            .streams()
            .try_recv_eof(resident)
            .map_err(|error| Retire::Target(error.into()))?;
        h3.ingress()
            .reject(quic, id.stream_id())
            .map_err(Retire::Target)?;
        if !eof {
            let code =
                dope_quic::errors::Code::from_u16(frame::ErrorCode::RequestCancelled.wire_code());
            quic.streams()
                .try_stop(resident, code)
                .map_err(|error| Retire::Target(error.into()))?;
        }
    }
    let rejected =
        crate::conn::io::ingress::schedule::Status::new(h3.h3()).rejected(id.stream_id());
    let mut streams = quic.streams();
    let mut binding = streams.binding();
    let result = if rejected {
        binding.try_unbind(resident, id.slot())
    } else {
        binding.try_retire(resident, id.slot())
    };
    result.map_err(|error| Retire::Target(error.into()))
}

pub(in crate::manifold::server) fn accept<'d, E, const ID: u8, A, O, R, const RECONCILE: bool>(
    h3: &mut manifold::Session<'d, A::Plan>,
    mut deadline: clock::At<'d, &mut E>,
    app: &A,
    effects: owners::Projection<'_, O, id::Handle<id::Driver<'d>, ID>>,
    quic: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
    id: requests::Id,
    done: task::Buffered<'d, A::Prepared>,
) where
    A: app::Runtime<'d>,
    E: State<'d>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    O::Retention: owners::Handoff,
    R: stream::Recv,
{
    use owners::{Commit, Projection};

    let (life, handle) = effects.into_parts();

    if let Err(error) = deadline.apply_mut(|deadline, now| {
        E::transition(deadline, id, timeline::Transition::ResponseReady, now)
    }) {
        let retired = removed(h3, deadline, quic, id, false);
        exchange::Id::new(handle, id.stream_id()).retire(life, life::Outcome::Rejected);
        manifold::close_connection_scoped(quic, deadline::code(error));
        if let Err(error) = retired {
            manifold::close_connection_scoped(quic, error.code());
        }
        return;
    }
    let target = id.target();
    let stream = target.stream_id();
    let task::Buffered {
        request: req,
        method,
    } = done;
    let flow = exchange::Id::new(handle, stream);
    let done = req.with_input(|prepared, input| {
        let sent = app.try_project(prepared, input, |scoped| {
            let (req, slot) = scoped.into_parts();
            let enc = encoder::Encoder::from_slot(h3.h3_mut(), target, method, slot);
            match app.try_dispatch(req, enc) {
                Err((error, target)) => {
                    let code = match error {
                        dispatch::Error::Busy | dispatch::Error::Sequence(sequence::Exhausted) => {
                            frame::ErrorCode::ExcessiveLoad
                        }
                        dispatch::Error::Encode(cause) => Commit::code(cause),
                        dispatch::Error::Body(never) => match never {},
                        dispatch::Error::Pending
                        | dispatch::Error::Moved
                        | dispatch::Error::Unsupported
                        | dispatch::Error::Invalid(_) => frame::ErrorCode::Internal,
                    };
                    Err((code, target.into_target()))
                }
                Ok(enc) => <O::Retention as owners::Handoff>::try_detach(enc)
                    .map_err(|code| (code, target)),
            }
        });
        let (enc, owner) = match sent {
            Ok(ready) => ready,
            Err(fail) => {
                fail.discard();
                return Err((frame::ErrorCode::Internal, target));
            }
        };
        <O::Retention as owners::Handoff>::finish(enc?, owner).map_err(|code| (code, target))
    });
    match done {
        Ok(commit) => {
            commit.observe(life, flow);
            retire(h3, deadline, life, quic, id, flow, life::Outcome::Completed);
        }
        Err((code, target)) => {
            Projection::new(life, handle).reset(quic, target, code);
            retire(h3, deadline, life, quic, id, flow, life::Outcome::Rejected);
        }
    }
}

fn retire<'d, P, E, const ID: u8, O, R, const RECONCILE: bool>(
    h3: &mut manifold::Session<'d, P>,
    deadline: clock::At<'d, &mut E>,
    life: &mut O,
    quic: &mut scope::Access<'_, ID, R, RECONCILE, resident::Shared<'d>>,
    id: requests::Id,
    flow: exchange::Id<id::Handle<id::Driver<'d>, ID>, Id>,
    end: life::Outcome,
) where
    P: rules::Policy,
    E: State<'d>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    R: stream::Recv,
{
    let retired = removed(h3, deadline, quic, id, false);
    flow.retire(life, end);
    if let Err(error) = retired {
        manifold::close_connection_scoped(quic, error.code());
    }
}

pub(in crate::manifold::server) fn reject_new<
    'd,
    'scope,
    A,
    L,
    E,
    const ID: u8,
    O,
    R,
    const RECONCILE: bool,
>(
    state: &mut server::Session<'d, A, L, E>,
    life: &mut O,
    quic: &mut scope::Access<'scope, ID, R, RECONCILE, resident::Shared<'d>>,
    handle: id::Handle<id::Driver<'d>, ID>,
    resident: Handle<'scope>,
    failure: Create<entries::Tag>,
) where
    A: app::Runtime<'d>,
    L: rotation::Policy<'d>,
    E: State<'d>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>,
    O::Retention: owners::Handoff,
    R: stream::Recv,
{
    let code = match failure {
        Create::Full { .. } => frame::ErrorCode::ExcessiveLoad,
        Create::Occupied { .. } => {
            manifold::close_connection_scoped(quic, frame::ErrorCode::Internal);
            return;
        }
    };
    state.reject_fresh(life, quic, handle, resident, code);
}
