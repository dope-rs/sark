use std::{convert::Infallible, ops, pin};

use dope::{
    core::{
        BranchError,
        driver::{route::Identity, schedule::app},
    },
    manifold::network::listener::conn::Id,
};
use o3::buffer::{self, hold, resident};
use sark_core::{
    duplex::{
        admit,
        input::{Feed, Work},
        machine,
    },
    life::{self, exchange, owners},
    manifold::{
        schedule,
        schedule::typed,
        stream::Boundary,
        task::{
            app::Runtime,
            response::{Access, Completion, State},
        },
    },
};
use sark_http::{
    field::metadata::section::rules,
    owner::{self, cause},
};

use crate::{
    conn::{
        self,
        machine::{
            Connection,
            streams::{self, Flow},
        },
    },
    frame::numbers,
    server::{
        self,
        response::{Encoder, Handoff, Pending, Status},
        state::{self, exchange::try_dispatch},
        task::{
            response::{Count, Response, input},
            *,
        },
    },
    stream,
};

pub(in crate::server::task) struct Driver<
    'a,
    'd,
    ID: Identity,
    R: Runtime<'d>,
    M,
    F: rules::Policy,
    P,
> {
    pub(in crate::server::task) router: pin::Pin<&'a mut R>,
    pub(in crate::server::task) tasks: &'a mut registry::Table<'d, ID, R::Task>,
    pub(in crate::server::task) index: &'a mut registry::Index,
    pub(in crate::server::task) connection_id: Id<'d, ID>,
    pub(in crate::server::task) connection:
        &'a mut Connection<crate::ServerRole, F, resident::Shared<'d>>,
    pub(in crate::server::task) limits: server::Config,
    pub(in crate::server::task) work: app::Work<'a, 'd>,
    pub(in crate::server::task) mode: &'a mut M,
    pub(in crate::server::task) report: P,
}

impl<'d, ID: Identity + 'd, R, M, F, P> Driver<'_, 'd, ID, R, M, F, P>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    M: state::Transitions<Id<'d, ID>>,
    M::Retention: Handoff,
    F: rules::Policy,
    P: FnMut(Id<'d, ID>, stream::Key, &dyn std::fmt::Debug),
{
    pub(super) fn error(&mut self, stream: stream::Key, cause: conn::Error) -> numbers::ErrorCode {
        server::runtime::app::response(
            self.mode,
            identify(self.connection_id, stream),
            &cause,
            &mut self.report,
        );
        cause.code()
    }

    fn release_task(&mut self, stream: stream::Key) -> bool {
        let Some((_, task, unreported)) = self.tasks.take_stream(self.index, stream) else {
            return false;
        };
        Runtime::cancel(self.router.as_mut(), task);
        unreported
    }

    fn finish(&mut self, stream: stream::Key, outcome: life::Outcome) {
        identify(self.connection_id, stream).retire(self.mode, outcome);
    }

    fn resume(&mut self, stream: stream::Key) -> Result<(), numbers::ErrorCode> {
        let Some(task) = self.tasks.get(self.index, stream) else {
            return Err(numbers::ErrorCode::InternalError);
        };
        match Runtime::resume(self.router.as_mut(), task) {
            Access::Ready(()) => Ok(()),
            Access::Absent | Access::Active => Err(numbers::ErrorCode::InternalError),
        }
    }

    fn drive_body(
        &mut self,
        stream: stream::Key,
        pending: &mut Pending<'d>,
        state: &State,
    ) -> Result<schedule::Turn<conn::DataBlock>, numbers::ErrorCode> {
        let end = match state {
            State::Complete(_) => Boundary::End,
            State::Streaming => Boundary::More,
        };
        let mut owner = owners::Sink::new(&mut *self.mode, self.connection_id);
        let turn = pending
            .turn_seen(self.connection, stream, end, &mut owner)
            .map_err(|error| self.error(stream, error))?;
        if matches!(state, State::Streaming) && matches!(turn, schedule::Turn::Complete) {
            self.resume(stream)?;
            return Ok(schedule::Turn::Idle);
        }
        Ok(turn)
    }

    fn drive_duplex(
        &mut self,
        stream: stream::Key,
        input: &mut input::Input<'d, R::Duplex>,
    ) -> Result<schedule::Turn<conn::DataBlock>, numbers::ErrorCode> {
        let turn = input.transact(|emission| {
            let (payload, end, _) = emission.into_parts();
            match payload {
                admit::Payload::Present { output, permit } => {
                    let boundary = match (end, output.is_single()) {
                        (machine::End::Open, _)
                        | (machine::End::FinishOutput | machine::End::FinishSession, false) => {
                            Boundary::More
                        }
                        (machine::End::FinishOutput | machine::End::FinishSession, true) => {
                            Boundary::End
                        }
                    };
                    let held = buffer::Residency::resident(output.front());
                    match self.connection.streams().retained().try_chunk(
                        stream,
                        output,
                        boundary,
                        streams::Limit::QUANTUM,
                    ) {
                        Ok(Flow::Blocked(block, rest)) => {
                            permit.pending(rest, schedule::Turn::Blocked(block))
                        }
                        Ok(Flow::Sent { bytes, rest }) => {
                            if bytes != numbers::Length::ZERO {
                                self.acquire_out(stream, bytes.into_usize(), held);
                            }
                            match rest.take() {
                                Some(rest) => permit.pending(rest, schedule::Turn::Progress),
                                None => permit.commit(schedule::Turn::Progress),
                            }
                        }
                        Err((error, rest)) => permit.returned(self.error(stream, error), rest),
                    }
                }
                admit::Payload::Vacant(permit) => match end {
                    machine::End::Open => permit.commit(schedule::Turn::Progress),
                    machine::End::FinishOutput | machine::End::FinishSession => {
                        match self.connection.streams().try_to(
                            stream,
                            &[],
                            &[],
                            Boundary::End,
                            streams::Limit::QUANTUM,
                        ) {
                            Ok(conn::DataOutcome::Sent(_) | conn::DataOutcome::Ended) => {
                                permit.commit(schedule::Turn::Progress)
                            }
                            Ok(conn::DataOutcome::Blocked(block)) => {
                                permit.blocked(schedule::Turn::Blocked(block))
                            }
                            Err(error) => permit.reject(self.error(stream, error)),
                        }
                    }
                },
            }
        });
        let (step, feed) = turn.into_parts();
        match feed {
            Feed::Idle | Feed::Consumed(()) => {}
        }
        match step {
            machine::Transaction::Idle => Ok(schedule::Turn::Idle),
            machine::Transaction::Pending { receipt } => Ok(receipt),
            machine::Transaction::Rejected(error) => Err(error),
            machine::Transaction::Violation(
                machine::Violation::OutputAfterFinish | machine::Violation::PrefixPending,
            ) => Err(numbers::ErrorCode::InternalError),
            machine::Transaction::Committed {
                after: machine::After::Finished,
                ..
            } => Ok(schedule::Turn::Complete),
            machine::Transaction::Committed {
                after: machine::After::Turn,
                ..
            } => Ok(schedule::Turn::Progress),
            machine::Transaction::Committed {
                after: machine::After::Idle,
                ..
            } => match input.work() {
                Work::Idle => Ok(schedule::Turn::Idle),
                Work::Ready => Ok(schedule::Turn::Progress),
            },
        }
    }

    pub(super) fn acquire_out(&mut self, stream: stream::Key, written: usize, held: usize) {
        if M::ENABLED && written != 0 {
            owners::Projection::new(self.mode).apply(owner::Effect::new(
                identify(self.connection_id, stream),
                hold::Charge::from_owner(written, held),
                owner::Change::StagingAcquired(cause::Acquire::StreamChunk),
            ));
        }
    }

    fn removed(
        &mut self,
        stream: stream::Key,
        error: BranchError<conn::Error, numbers::ErrorCode>,
    ) -> typed::Control<Reply<'d, R>, Data<'d, R>, Infallible, conn::DataBlock, numbers::ErrorCode>
    {
        let error = match error {
            BranchError::First(error) => self.error(stream, error),
            BranchError::Second(code) => code,
        };
        self.reject(identify(self.connection_id, stream), error);
        typed::Control::Remove
    }

    fn reject(
        &mut self,
        exchange: exchange::Id<Id<'d, ID>, stream::Key>,
        error: numbers::ErrorCode,
    ) {
        if self.release_task(exchange.stream()) {
            self.mode.observe(life::Event::TaskCancelled(exchange));
        }
        if let Some(write) = self.connection.streams().retire(exchange.stream(), error) {
            release_retired(
                self.mode,
                exchange.connection(),
                write,
                cause::Staging::Reset,
            );
        }
        exchange.retire(self.mode, life::Outcome::Rejected);
    }
}

impl<'a, 'd, ID: Identity + 'd, R, M, F, P> typed::Settle<Reply<'d, R>, Data<'d, R>, Infallible>
    for Driver<'a, 'd, ID, R, M, F, P>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    M: state::Transitions<Id<'d, ID>>,
    M::Retention: Handoff,
    F: rules::Policy,
    P: FnMut(Id<'d, ID>, stream::Key, &dyn std::fmt::Debug),
{
    type Key = stream::Key;
    type Error = numbers::ErrorCode;

    fn settle(
        &mut self,
        stream: Self::Key,
        response: Resident<'d, R>,
        terminal: schedule::Terminal<Self::Error>,
    ) -> schedule::Settlement<Resident<'d, R>> {
        if matches!(terminal, schedule::Terminal::Idle)
            && matches!(&response, Value::Data(response::Data::Duplex { .. }))
        {
            return schedule::Settlement::Keep(response);
        }
        let exchange = identify(self.connection_id, stream);
        match terminal {
            schedule::Terminal::Failed(error) => {
                response::discard(
                    response,
                    self.connection,
                    exchange,
                    self.mode,
                    cause::Staging::Reset,
                );
                self.reject(exchange, error);
            }
            schedule::Terminal::Complete => match response {
                Value::Data(response::Data::Body {
                    state: State::Complete(outcome),
                    ..
                }) => self.finish(stream, outcome),
                Value::Data(response::Data::Duplex { input }) => {
                    drop(input);
                    self.connection.release_duplex(stream);
                    self.finish(stream, life::Outcome::Completed)
                }
                _ => {}
            },
            schedule::Terminal::Idle => {}
        }
        schedule::Settlement::Remove
    }
}

impl<'a, 'd, ID: Identity + 'd, R, M, F, P> typed::Driver<Reply<'d, R>, Data<'d, R>, Infallible>
    for Driver<'a, 'd, ID, R, M, F, P>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    M: state::Transitions<Id<'d, ID>>,
    M::Retention: Handoff,
    F: rules::Policy,
    P: FnMut(Id<'d, ID>, stream::Key, &dyn std::fmt::Debug),
{
    type Block = conn::DataBlock;
    type Permit = app::Permit<'a, 'd>;

    fn permit(&mut self) -> Option<Self::Permit> {
        if self.connection.output().len() >= self.limits.max_outbound_bytes {
            return None;
        }
        self.work.permit()
    }

    fn control(
        &mut self,
        _: Self::Permit,
        stream: Self::Key,
        response: Reply<'d, R>,
    ) -> typed::Control<Reply<'d, R>, Data<'d, R>, Infallible, Self::Block, Self::Error> {
        let exchange = identify(self.connection_id, stream);
        let fresh = Count::Pending;
        let (driven, state, count) = match response {
            work @ (Response::Status(_)
            | Response::Sync(_)
            | Response::Fixed(_)
            | Response::Task
            | Response::DuplexOpen { .. })
                if !self.connection.streams().headers().is_ready() =>
            {
                return typed::Control::Control {
                    value: work,
                    turn: schedule::Turn::Blocked(conn::DataBlock::Egress),
                };
            }
            Response::End => {
                return match self.connection.streams().try_to(
                    stream,
                    &[],
                    &[],
                    Boundary::End,
                    streams::Limit::QUANTUM,
                ) {
                    Ok(conn::DataOutcome::Blocked(block)) => typed::Control::Control {
                        value: Response::End,
                        turn: schedule::Turn::Blocked(block),
                    },
                    Ok(conn::DataOutcome::Sent(_) | conn::DataOutcome::Ended) => {
                        self.finish(stream, life::Outcome::Completed);
                        typed::Control::Complete
                    }
                    Err(error) => self.removed(stream, BranchError::First(error)),
                };
            }
            Response::Status(status) => {
                let head = match Status::new(status).prepare(self.connection, stream, Boundary::End)
                {
                    Ok(head) => head,
                    Err(error) if error.is_pressure() => {
                        return typed::Control::Control {
                            value: Response::Status(status),
                            turn: schedule::Turn::Blocked(conn::DataBlock::Egress),
                        };
                    }
                    Err(error) => return self.removed(stream, BranchError::First(error)),
                };
                match head.drive(self.connection) {
                    Ok(driven) => (driven, State::Complete(life::Outcome::Rejected), fresh),
                    Err(error) => return self.removed(stream, BranchError::First(error)),
                }
            }
            Response::Sync(request) => {
                match try_dispatch::<R, M::Retention, _, F>(
                    request,
                    self.router.as_ref().get_ref(),
                    self.connection,
                    stream,
                ) {
                    Ok(driven) => (driven, State::Complete(life::Outcome::Completed), fresh),
                    Err((error, failed)) => return self.removed(failed, error),
                }
            }
            Response::Fixed(output) => 'fixed: {
                let (error, failed) = {
                    match output.emit(|id| Encoder::task(&mut *self.connection, stream, id)) {
                        Ok((prepared, outcome)) => {
                            let failed = prepared.stream();
                            match prepared.defer::<M::Retention>() {
                                Ok(driven) => {
                                    break 'fixed (driven, State::Complete(outcome), fresh);
                                }
                                Err(error) => (error, failed),
                            }
                        }
                        Err(error) => error,
                    }
                };
                return self.removed(failed, BranchError::First(error));
            }
            Response::Task => {
                let Some(task) = self.tasks.get(self.index, stream) else {
                    return self.removed(
                        stream,
                        BranchError::Second(numbers::ErrorCode::InternalError),
                    );
                };
                let (driven, state) = 'emission: {
                    let (error, failed) = {
                        match Runtime::emit(self.router.as_mut(), task, |id| {
                            Encoder::task(&mut *self.connection, stream, id)
                        }) {
                            Access::Ready(Ok((prepared, state))) => {
                                match prepared.defer::<M::Retention>() {
                                    Ok(driven) => break 'emission (driven, state),
                                    Err(error) => (BranchError::First(error), stream),
                                }
                            }
                            Access::Ready(Err((error, failed))) => {
                                (BranchError::First(error), failed)
                            }
                            Access::Absent | Access::Active => (
                                BranchError::Second(numbers::ErrorCode::InternalError),
                                stream,
                            ),
                        }
                    };
                    return self.removed(failed, error);
                };
                if matches!(state, State::Complete(_)) {
                    self.release_task(stream);
                }
                (driven, state, fresh)
            }
            Response::Head { head, state, count } => match head.drive(self.connection) {
                Ok(driven) => (driven, state, count),
                Err(error) => return self.removed(stream, BranchError::First(error)),
            },
            Response::DuplexOpen { request } => {
                return match self.open(stream, request) {
                    Ok(control) => control,
                    Err(error) => self.removed(stream, BranchError::Second(error)),
                };
            }
            Response::DuplexHead { head, input } => {
                return match self.resume_open(stream, head, input) {
                    Ok(control) => control,
                    Err(error) => self.removed(stream, BranchError::Second(error)),
                };
            }
        };
        let receipt = match driven {
            ops::ControlFlow::Continue(receipt) => receipt,
            ops::ControlFlow::Break(head) => {
                return typed::Control::Control {
                    value: Response::Head { head, state, count },
                    turn: schedule::Turn::Blocked(conn::DataBlock::Egress),
                };
            }
        };
        match receipt.observe(self.mode, exchange) {
            Some(mut pending) => {
                let turn = self.drive_body(stream, &mut pending, &state);
                let value = response::Data::Body { pending, state };
                match turn {
                    Ok(turn) => typed::Control::Fused { value, turn },
                    Err(error) => typed::Control::Failed {
                        value: Value::Data(value),
                        error,
                    },
                }
            }
            None => match state {
                State::Streaming => match self.resume(stream) {
                    Ok(()) => typed::Control::Remove,
                    Err(error) => self.removed(stream, BranchError::Second(error)),
                },
                State::Complete(outcome) => {
                    self.finish(stream, outcome);
                    match count {
                        Count::Pending => typed::Control::Complete,
                        Count::Settled => typed::Control::Remove,
                    }
                }
            },
        }
    }

    fn data(
        &mut self,
        _: Self::Permit,
        stream: stream::Key,
        response: &mut Data<'d, R>,
    ) -> Result<schedule::Turn<Self::Block>, Self::Error> {
        match response {
            response::Data::Body { pending, state } => self.drive_body(stream, pending, state),
            response::Data::Duplex { input } => self.drive_duplex(stream, input),
        }
    }
}
