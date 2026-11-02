use conn::scope;
use dope::core::driver::schedule::clock;
use dope_quic::{conn, conn::id, errors::Code, stream, wire::stream::Id};
use o3::buffer::resident;
use requests::duplex;
use sark_core::{
    duplex::{admit, input, machine},
    life::{self, exchange, owners::Projection},
    manifold::{stream::Boundary, task::app},
};
use sark_http::{
    field::Response,
    owner::{self, cause},
    payload::response::status,
};

use crate::{
    conn::io::Resume,
    frame, manifold,
    manifold::{
        owners, requests,
        server::{self, deadline::state::State, rotation},
    },
};

enum Done {
    Idle,
    Flush,
    Reject {
        id: requests::Id,
        error: crate::conn::Error,
    },
    Violation {
        id: requests::Id,
        error: machine::Violation,
    },
    Complete {
        id: requests::Id,
    },
    Closed,
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
        reqs.with_duplex(|work| {
            let id = work.id();
            let stream = work.stream_id();
            let target = work.target();
            let flow = exchange::Id::new(handle, stream);

            if matches!(*work.response, duplex::Response::Pending) {
                let prep = h3
                    .h3_mut()
                    .reply()
                    .prepare(target, Response::new(status::Code::OK, []));
                let commit = prep.and_then(|head| head.open().commit());
                match commit {
                    Ok(charge) => {
                        *work.response = duplex::Response::Sent;
                        owners::Commit::new(0, charge, Default::default()).observe(sink, flow);
                    }
                    Err(error) if h3.h3().queues.is_pressure(&error) => {
                        return duplex::Next::Blocked(Done::Flush);
                    }
                    Err(error) => {
                        return duplex::Next::Finished(Done::Reject { id, error });
                    }
                }
            }

            let (step, feed) = work.input.transact(|emit| {
                let (payload, end, _) = emit.into_parts();
                match payload {
                    admit::Payload::Present { output, permit } => {
                        let boundary = match end {
                            machine::End::Open => Boundary::More,
                            machine::End::FinishOutput | machine::End::FinishSession => {
                                Boundary::End
                            }
                        };
                        let write = h3.h3_mut().reply().send(target, output, boundary);
                        match write {
                            Ok(charge) => {
                                Projection::new(sink).apply(owner::Effect::new(
                                    flow,
                                    charge,
                                    owner::Change::StagingAcquired(cause::Acquire::StreamChunk),
                                ));
                                permit.commit(())
                            }
                            Err((error, rest)) => {
                                if h3.h3().queues.is_pressure(&error) {
                                    permit.pending(rest, ())
                                } else {
                                    permit.returned(error, rest)
                                }
                            }
                        }
                    }
                    admit::Payload::Vacant(permit) => match end {
                        machine::End::Open => permit.commit(()),
                        machine::End::FinishOutput | machine::End::FinishSession => {
                            match h3.h3_mut().reply().finish(target) {
                                Ok(charge) => {
                                    Projection::new(sink).apply(owner::Effect::new(
                                        flow,
                                        charge,
                                        owner::Change::StagingAcquired(cause::Acquire::StreamChunk),
                                    ));
                                    permit.commit(())
                                }
                                Err(error) if h3.h3().queues.is_pressure(&error) => {
                                    permit.blocked(())
                                }
                                Err(error) => permit.reject(error),
                            }
                        }
                    },
                }
            });
            let resume = match (&step, &feed) {
                (
                    machine::Transaction::Pending { .. }
                    | machine::Transaction::Committed {
                        after: machine::After::Idle | machine::After::Turn,
                        ..
                    },
                    input::Feed::Consumed(_),
                ) => Resume::Ready,
                (
                    machine::Transaction::Idle
                    | machine::Transaction::Committed { .. }
                    | machine::Transaction::Rejected(_)
                    | machine::Transaction::Violation(machine::Violation::OutputAfterFinish)
                    | machine::Transaction::Pending { .. },
                    input::Feed::Idle | input::Feed::Consumed(_),
                ) => Resume::Idle,
            };
            match feed {
                input::Feed::Consumed(bytes) => {
                    if let Err(error) = h3.ingress().release(quic, stream, bytes) {
                        manifold::close_connection_scoped(quic, error.code());
                        return duplex::Next::Idle(Done::Closed);
                    }
                }
                input::Feed::Idle => {}
            }
            match resume {
                Resume::Ready => {
                    if let Err(error) = h3.ingress().resume(quic, stream) {
                        manifold::close_connection_scoped(quic, error.code());
                        return duplex::Next::Idle(Done::Closed);
                    }
                }
                Resume::Idle => {}
            }
            match step {
                machine::Transaction::Idle => match work.input.work() {
                    input::Work::Idle => duplex::Next::Idle(Done::Idle),
                    input::Work::Ready => duplex::Next::Ready(Done::Idle),
                },
                machine::Transaction::Pending { .. } => duplex::Next::Blocked(Done::Flush),
                machine::Transaction::Rejected(error) => {
                    duplex::Next::Finished(Done::Reject { id, error })
                }
                machine::Transaction::Violation(error) => {
                    duplex::Next::Finished(Done::Violation { id, error })
                }
                machine::Transaction::Committed {
                    after: machine::After::Finished,
                    ..
                } => duplex::Next::Finished(Done::Complete { id }),
                machine::Transaction::Committed {
                    after: machine::After::Turn,
                    ..
                } => duplex::Next::Ready(Done::Flush),
                machine::Transaction::Committed {
                    after: machine::After::Idle,
                    ..
                } => match work.input.work() {
                    input::Work::Idle => duplex::Next::Idle(Done::Flush),
                    input::Work::Ready => duplex::Next::Ready(Done::Flush),
                },
            }
        })
    };
    let Some(done) = next else {
        return super::Drive::Idle;
    };
    let (id, finish) = match done {
        Done::Idle | Done::Closed => return super::Drive::Progress,
        Done::Flush => {
            if let Err(error) = state.flush_h3(sink, quic, handle) {
                manifold::close_connection_scoped(quic, error.code());
            }
            return super::Drive::Progress;
        }
        Done::Reject { id, error } => (id, Err(error.error_code())),
        Done::Violation { id, error } => {
            let code = match error {
                machine::Violation::OutputAfterFinish => frame::ErrorCode::Internal,
            };
            (id, Err(code))
        }
        Done::Complete { id } => (id, Ok(())),
    };
    let stream = id.stream_id();
    let closed = (|| -> Result<(), manifold::Error> {
        state.h3.ingress().reject(quic, stream)?;
        let resident = state.bind(quic, id.local())?;
        if finish.is_ok() && quic.streams().try_recv_eof(resident)? {
            return Ok(());
        }
        let code = match finish {
            Ok(()) => frame::ErrorCode::RequestCancelled,
            Err(code) => code,
        };
        let app = Code::from_u16(code.wire_code());
        quic.streams().try_stop(resident, app)?;
        if finish.is_err() {
            owners::Projection::new(sink, handle).reset_bound(quic, resident, code);
        }
        Ok(())
    })();
    let outcome = if finish.is_ok() {
        life::Outcome::Completed
    } else {
        life::Outcome::Rejected
    };
    let retired = state.finish(
        quic,
        id,
        sink,
        exchange::Id::new(handle, stream),
        outcome,
        now,
    );
    if let Err(error) = closed {
        manifold::close_connection_scoped(quic, error.code());
    }
    if let Err(error) = retired {
        manifold::close_connection_scoped(quic, error.code());
    }
    if finish.is_ok()
        && !quic.view().lifecycle().is_closing()
        && let Err(error) = state.flush_h3(sink, quic, handle)
    {
        manifold::close_connection_scoped(quic, error.code());
    }
    super::Drive::Progress
}
