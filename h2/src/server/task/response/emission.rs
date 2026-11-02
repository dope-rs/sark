use std::ops;

use conn::machine::streams::{self, Flow};
use dope::{core::driver::route::Identity, manifold::network::listener::conn::Id};
use duplex::{
    admit,
    input::{Feed, Work},
    machine,
};
use o3::buffer;
use sark_core::{
    duplex, life,
    manifold::{
        schedule,
        stream::Boundary,
        task::{app::Runtime, response::Completion as _},
    },
};

use crate::{
    conn,
    frame::numbers,
    server::{
        response::{Encoder, Handoff, Head, Receipt},
        state,
        task::{self, response, response::input},
    },
    stream,
};

pub(super) struct Emission<'a, 'b, 'd, ID: Identity, R, M>
where
    R: Runtime<'d>,
{
    pub(super) driver: &'a mut response::Driver<'b, 'd, ID, R, M>,
}

impl<'d, ID: Identity + 'd, R, M> Emission<'_, '_, 'd, ID, R, M>
where
    R: Runtime<'d, Target = task::Target<'d, ID>>,
    M: state::Transitions<Id<'d, ID>>,
    M::Retention: Handoff,
{
    pub(super) fn complete(
        self,
        stream: stream::Key,
        done: R::Completion,
    ) -> Result<(ops::ControlFlow<Head<'d>, Receipt<'d>>, life::Outcome), (conn::Error, stream::Key)>
    {
        let id = done.id();
        let encoder = Encoder::task(self.driver.connection, stream, id);
        let (prepared, outcome) = done.emit(encoder)?;
        let failed = prepared.stream();
        let flow = prepared
            .defer::<M::Retention>()
            .map_err(|error| (error, failed))?;
        Ok((flow, outcome))
    }

    pub(super) fn drive(
        self,
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
                    match self.driver.connection.streams().retained().try_chunk(
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
                                self.driver.acquire_out(stream, bytes.into_usize(), held);
                            }
                            match rest.take() {
                                Some(rest) => permit.pending(rest, schedule::Turn::Progress),
                                None => permit.commit(schedule::Turn::Progress),
                            }
                        }
                        Err((error, rest)) => permit.returned(error.code(), rest),
                    }
                }
                admit::Payload::Vacant(permit) => match end {
                    machine::End::Open => permit.commit(schedule::Turn::Progress),
                    machine::End::FinishOutput | machine::End::FinishSession => {
                        match self.driver.connection.streams().try_to(
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
                            Err(error) => permit.reject(error.code()),
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
            machine::Transaction::Violation(machine::Violation::OutputAfterFinish) => {
                Err(numbers::ErrorCode::InternalError)
            }
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
}
