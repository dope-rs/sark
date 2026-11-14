use dope_quic::conn::id;

use crate::manifold::server::deadline::state::State;
pub(in crate::manifold::server) mod ingress;
pub(super) mod receive;
use std::{mem, ops::ControlFlow, pin};

use dope_quic::{stream, wire::stream::Id};
use o3::buffer::hold;
use owners::Projection;
use requests::task::{self, Pending};
use sark_core::{
    deadline::timeline,
    life::{self, exchange},
    manifold::task::{app, response},
};
use sark_http::payload::response::{Response, dispatch, status::Code};

use crate::{
    conn::io::replies,
    frame,
    manifold::{
        self, encoder, owners, requests,
        server::{self, deadline, failure, rotation, session::finish},
    },
};

pub(in crate::manifold::server) enum Output<'d, F> {
    Fixed(F),
    Status(Code),
    Head,
    Body(Option<sark_http::payload::response::dispatch::chunk::Chunk<'d>>),
    Failed,
    Resume,
}

pub(in crate::manifold::server) fn cancel<'d, A, C: Copy, O>(
    owned: task::Owned<'d, A::Task, O::Retention>,
    app: pin::Pin<&mut A>,
    life: &mut O,
    flow: exchange::Id<C, Id>,
) -> Option<encoder::Pending<'d, O::Retention>>
where
    A: app::Runtime<'d>,
    O: life::Mode<C, Id>,
    O::Retention: owners::Handoff,
{
    let active = match owned {
        task::Owned::Response(response) => return Some(response),
        task::Owned::Active(active) => active,
    };
    let task::Active { task, pending } = active;
    let unfinished = matches!(pending, Pending::Future);
    let response = match pending {
        Pending::End => return None,
        Pending::Response(response) => Some(response),
        _ => None,
    };
    app::Runtime::cancel(app, task);
    if unfinished {
        life.observe(life::Event::TaskCancelled(flow));
    }
    response
}

impl<'a, 'scope, 'd, A, O, L, E, B, const ID: u8, const RECONCILE: bool>
    ingress::item::Item<'a, 'scope, 'd, A, O, L, E, B, ID, RECONCILE>
where
    A: app::Runtime<'d, Target = server::Handle<'d, ID>>,
    O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>
        + failure::Sink<id::Handle<id::Driver<'d>, ID>>,
    O::Retention: owners::Handoff,
    L: rotation::Policy<'d>,
    E: State<'d>,
    B: stream::Retain<'d>,
{
    pub(in crate::manifold::server) fn respond(
        mut self,
        id: requests::Id,
        output: Output<'d, A::Fixed>,
    ) {
        self.response(Some(id), output);
    }

    pub(in crate::manifold::server) fn resume_response(mut self) -> bool {
        self.response(None, Output::Resume)
    }

    fn response(&mut self, selected: Option<requests::Id>, output: Output<'d, A::Fixed>) -> bool {
        use response::Completion as _;

        let state = &mut *self.state;
        let mut app = self.app.as_mut();
        let life = &mut *self.life;
        let conn = &mut *self.conn;
        let handle = self.handle;
        let now = self.now;
        let (requests, h3, deadline) = (&mut state.requests, &mut state.h3, &mut state.deadline);
        let mut cancellation_error = None;
        let next = requests.with_response(selected, |id, method, owned| {
            let flow = exchange::Id::new(handle, id.stream_id());
            // Only this turn combines the phases; parked entries retain either
            // a live task or a fixed response whose runtime slot is already free.
            let (mut task, mut pending, mut output) = match owned {
                Ok(task::Owned::Active(active)) => (Some(active.task), active.pending, output),
                Ok(task::Owned::Response(response)) => (None, Pending::Response(response), output),
                Err(code) => (None, Pending::Future, Output::Status(code)),
            };
            let result = (|| {
                match &output {
                    Output::Fixed(_) => {
                        drop(task.take());
                        if !matches!(pending, Pending::Future) {
                            drop(mem::replace(&mut output, Output::Resume));
                            return Err(frame::ErrorCode::Internal);
                        }
                        pending = Pending::Body;
                        life.observe(life::Event::TaskPollReady(flow));
                    }
                    Output::Body(body) => {
                        let next = body.as_ref().map_or(Pending::End, |_| Pending::Body);
                        let expected = matches!(pending, Pending::Body);
                        if let Pending::Response(response) = mem::replace(&mut pending, next) {
                            response.discard(h3.h3_mut(), id);
                        }
                        if !expected {
                            return Err(frame::ErrorCode::Internal);
                        }
                    }
                    _ => {}
                }
                match h3.ingress().cancelled(conn, id.stream_id(), life, handle) {
                    Ok(false) => {}
                    Ok(true) => return Ok(ControlFlow::Continue(life::Outcome::Cancelled)),
                    Err(error) => {
                        cancellation_error = Some((id, error));
                        return Ok(ControlFlow::Continue(life::Outcome::Cancelled));
                    }
                }
                if matches!(output, Output::Head) {
                    if task.is_none() || !matches!(pending, Pending::Future) {
                        return Err(frame::ErrorCode::Internal);
                    }
                    pending = Pending::Body;
                    life.observe(life::Event::TaskPollReady(flow));
                }
                if matches!(output, Output::Fixed(_) | Output::Head) {
                    E::transition(deadline, id, timeline::Transition::ResponseReady, now)
                        .map_err(deadline::code)?;
                }
                let emitted = match mem::replace(&mut output, Output::Resume) {
                    Output::Status(code) => Some(
                        dispatch::Immediate::emit(
                            Response::new(code),
                            encoder::Encoder::new(h3.h3_mut(), id.target(), method),
                        )
                        .map(|enc| (enc, response::State::Complete(life::Outcome::Rejected))),
                    ),
                    Output::Fixed(output) => Some(
                        output
                            .emit(|request| {
                                encoder::Encoder::task(h3.h3_mut(), id.target(), method, request)
                            })
                            .map(|(enc, outcome)| (enc, response::State::Complete(outcome))),
                    ),
                    Output::Head => {
                        let Some(task) = task.as_ref() else {
                            unreachable!("task")
                        };
                        match app::Runtime::emit(app.as_mut(), task, |request| {
                            encoder::Encoder::task(h3.h3_mut(), id.target(), method, request)
                        }) {
                            response::Access::Ready(emitted) => Some(emitted),
                            response::Access::Absent | response::Access::Active => {
                                return Err(frame::ErrorCode::Internal);
                            }
                        }
                    }
                    Output::Body(Some(body)) => {
                        let plan = replies::try_data(body)
                            .map_err(|(cause, _)| failure::response(life, handle, id, cause))?;
                        pending = Pending::Chunk(plan);
                        None
                    }
                    Output::Resume | Output::Body(None) => None,
                    Output::Failed => return Err(frame::ErrorCode::Internal),
                };
                if let Some(emitted) = emitted {
                    let response = emitted
                        .map_err(|(cause, _)| cause)
                        .and_then(|(enc, state)| {
                            <O::Retention as owners::Handoff>::defer(encoder::Initial(enc), state)
                        })
                        .map_err(|cause| failure::response(life, handle, id, cause))?;
                    pending = Pending::Response(response);
                }
                let end = matches!(pending, Pending::End);
                let charge = match mem::replace(&mut pending, Pending::Body) {
                    Pending::Response(response) => {
                        match response
                            .send(h3.h3_mut(), id, life, flow)
                            .map_err(|cause| failure::response(life, handle, id, cause))?
                        {
                            ControlFlow::Break(response) => {
                                pending = Pending::Response(response);
                                return Ok(ControlFlow::Break(()));
                            }
                            ControlFlow::Continue(state) => match state {
                                response::State::Complete(outcome) => {
                                    return Ok(ControlFlow::Continue(outcome));
                                }
                                response::State::Streaming => {
                                    E::transition(
                                        deadline,
                                        id,
                                        timeline::Transition::ResponseCommitted,
                                        now,
                                    )
                                    .map_err(deadline::code)?;
                                    None
                                }
                            },
                        }
                    }
                    Pending::Chunk(chunk) => {
                        let reply = h3.h3_mut().reply();
                        let sent = if O::STREAM_CHARGE {
                            Some(reply.send_chunk::<hold::evidence::On>(id.target(), chunk))
                        } else {
                            reply
                                .send_chunk::<hold::evidence::Off>(id.target(), chunk)
                                .map_or_else(|error| Some(Err(error)), |_| None)
                        };
                        sent.map(|sent| sent.map_err(|(error, chunk)| (error, Pending::Chunk(chunk))))
                    }
                    Pending::End => {
                        pending = Pending::End;
                        Some(
                            h3.h3_mut()
                                .reply()
                                .finish(id.target())
                                .map_err(|error| (error, Pending::End)),
                        )
                    }
                    Pending::Future | Pending::Body => return Err(frame::ErrorCode::Internal),
                };
                if let Some(charge) = charge {
                    let charge = match charge {
                        Ok(charge) => charge,
                        Err((error, held)) if h3.h3().queues.is_pressure(&error) => {
                            pending = held;
                            return Ok(ControlFlow::Break(()));
                        }
                        Err((error, _)) => {
                            return Err(failure::response(life, handle, id, error));
                        }
                    };
                    use sark_http::owner::{self, cause};
                    sark_core::life::owners::Projection::new(life).apply(owner::Effect::new(
                        flow,
                        charge,
                        owner::Change::StagingAcquired(cause::Acquire::StreamChunk),
                    ));
                }
                if end {
                    return Ok(ControlFlow::Continue(life::Outcome::Completed));
                }
                let Some(task) = task.as_ref() else {
                    return Err(frame::ErrorCode::Internal);
                };
                match app::Runtime::resume(app.as_mut(), task) {
                    response::Access::Ready(()) | response::Access::Active => {
                        Ok(ControlFlow::Break(()))
                    }
                    response::Access::Absent => Err(frame::ErrorCode::Internal),
                }
            })();
            let outcome = match result {
                Ok(ControlFlow::Break(())) => {
                    return ControlFlow::Break(match task {
                        Some(task) => task::Owned::Active(task::Active { task, pending }),
                        None => match pending {
                            Pending::Response(response) => task::Owned::Response(response),
                            _ => unreachable!("response"),
                        },
                    });
                }
                Ok(ControlFlow::Continue(outcome)) => Ok(outcome),
                Err(code) => Err(code),
            };
            let retained = match task {
                Some(task) => cancel(
                    task::Owned::Active(task::Active { task, pending }),
                    app.as_mut(),
                    life,
                    flow,
                ),
                None => match pending {
                    Pending::Response(response) => Some(response),
                    _ => None,
                },
            };
            if let Some(response) = retained {
                response.discard(h3.h3_mut(), id);
            }
            ControlFlow::Continue((id, outcome))
        });
        let progressed = match next {
            Ok(Some(ControlFlow::Continue(((id, outcome), removed)))) => {
                self.finish_response(id, outcome, removed);
                true
            }
            Ok(Some(ControlFlow::Break(()))) => true,
            Ok(None) => false,
            Err(error) => {
                manifold::close_connection_scoped(self.conn, server::request_error_code(error));
                true
            }
        };
        if let Some((id, error)) = cancellation_error {
            if let Err(error) = self
                .state
                .discard_response(self.life, self.handle, id.stream_id())
            {
                manifold::close_connection_scoped(self.conn, error.error_code());
            }
            manifold::close_connection_scoped(self.conn, error.code());
        }
        if progressed && !self.conn.view().lifecycle().is_closing() {
            self.state.flush_h3(self.life, self.conn, self.handle);
        }
        progressed
    }

    fn finish_response(
        &mut self,
        id: requests::Id,
        outcome: Result<life::Outcome, frame::ErrorCode>,
        removed: Option<bool>,
    ) {
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(code) => {
                if let Err(error) =
                    self.state
                        .discard_response(self.life, self.handle, id.stream_id())
                {
                    manifold::close_connection_scoped(self.conn, error.error_code());
                }
                Projection::new(self.life, self.handle).reset(self.conn, id.target(), code);
                life::Outcome::Rejected
            }
        };
        let flow = exchange::Id::new(self.handle, id.stream_id());
        let retired = match removed {
            Some(finished) => finish::removed(
                &mut self.state.h3,
                self.now.at(&mut self.state.deadline),
                self.conn,
                id,
                !finished,
            ),
            None => finish::retained(&mut self.state.deadline, id, self.now),
        };
        flow.retire(self.life, outcome);
        if let Err(error) = retired {
            manifold::close_connection_scoped(self.conn, error.code());
        }
    }
}
