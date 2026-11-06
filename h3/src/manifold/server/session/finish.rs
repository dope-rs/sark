use conn::scope;
use dope::core::driver::schedule::clock;
use dope_quic::{
    conn,
    conn::{id, stream::resident::Handle},
    stream,
    wire::stream::Id,
};
use o3::{buffer::resident, depot::arena::hash::errors::Create};
use requests::{entries, task};
use sark_core::{deadline::timeline, decode::dispatch, life, manifold::task::app};
use sark_http::field::metadata::section::rules;

use crate::{
    frame, manifold,
    manifold::{
        Phase, encoder, owners, requests,
        server::{self, deadline::state::State, rotation, session::retire::Retire},
    },
};

pub(in crate::manifold::server) fn release<
    'd,
    A,
    L,
    E,
    M: owners::Handoff,
    const ID: u8,
    R,
    const RECONCILE: bool,
>(
    state: &mut server::Session<'d, A, L, E, M>,
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

pub(in crate::manifold::server) fn retained<'d, E: State<'d>>(
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

pub(in crate::manifold::server) fn accept<'d, A, H>(
    h3: &mut manifold::Session<'d, A::Plan>,
    app: &A,
    id: requests::Id,
    done: task::Buffered<'d, A::Prepared>,
) -> Result<encoder::Pending<'d, H>, Result<frame::ErrorCode, crate::conn::Error>>
where
    A: app::Runtime<'d>,
    H: owners::Handoff,
{
    let target = id.target();
    let task::Buffered {
        request: req,
        method,
    } = done;
    req.with_input(|prepared, input| {
        let sent = app.try_project(prepared, input, |scoped| {
            let (req, slot) = scoped.into_parts();
            let enc = encoder::Encoder::from_slot(h3.h3_mut(), target, method, slot);
            match app.try_dispatch(req, enc) {
                Err((error, _)) => Err(match error {
                    dispatch::Error::Busy | dispatch::Error::Sequence(_) => {
                        Ok(frame::ErrorCode::ExcessiveLoad)
                    }
                    dispatch::Error::Encode(cause) => Err(cause),
                    dispatch::Error::Body(never) => match never {},
                    dispatch::Error::Pending
                    | dispatch::Error::Moved
                    | dispatch::Error::Unsupported
                    | dispatch::Error::Invalid(_) => Ok(frame::ErrorCode::Internal),
                }),
                Ok(enc) => H::try_detach(enc).map_err(Err),
            }
        });
        let (enc, owner) = match sent {
            Ok(ready) => ready,
            Err(fail) => {
                fail.discard();
                return Err(Ok(frame::ErrorCode::Internal));
            }
        };
        H::defer_with(encoder::Initial(enc?), owner, life::Outcome::Completed).map_err(Err)
    })
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
    state: &mut server::Session<'d, A, L, E, O::Retention>,
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
