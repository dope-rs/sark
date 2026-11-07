use core::mem;
use std::rc::Rc;

use dope::{
    core::driver::{
        schedule::clock,
        settings::{self, claim, claim::Claim, demands, demands::slots, errors},
    },
    manifold::app::dispatch,
    runtime::control,
};
use dope_quic::{
    errors::Code,
    runtime::{endpoint, endpoint::client, receive, routing},
};
use sark_h3::{frame, manifold::server::startup::Plan};

use crate::common::server::h3::{
    Client,
    apps::{cancel_stream, stop_stream},
    proof::{CancelAction, DIALER, Dispatch, Failure, LISTENER, Metric, Shared, connection_key},
};

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
#[dispatcher(driver = 'd)]
#[coordinate]
pub(crate) struct AsyncRuntime<'a, 'd, S> {
    #[pin]
    #[manifold]
    pub(crate) server: S,
    #[pin]
    #[manifold(control)]
    pub(crate) client: endpoint::Endpoint<'d, DIALER, Client<'a>>,
    #[dispatcher(schedule)]
    pub(crate) deadline: clock::Deadline<'d>,
}

impl<'a, 'd, S> AsyncRuntime<'a, 'd, S>
where
    S: dispatch::raw::Manifold<'d>,
{
    fn coordinate(this: async_runtime_coord::Fields<'_, 'a, 'd, S>) -> control::Flow {
        let proof = Rc::clone(&this.client.handler().proof);
        match Self::try_step(this, &proof) {
            Ok(flow) => flow,
            Err(error) => {
                proof.fail(error);
                control::Flow::Idle
            }
        }
    }

    fn try_step(
        mut this: async_runtime_coord::Fields<'_, 'a, 'd, S>,
        proof: &Shared<'a>,
    ) -> super::proof::Result<control::Flow> {
        {
            let mut proof = proof.write()?;
            if proof.is_cancelled() {
                proof.replies.finished[0] = true;
                proof.stop_done();
            }
        }
        let clock = this.step.clock();
        let now = clock.now();
        if this.deadline.is_due(now) {
            return Err(Failure::Scenario("H3 async runtime timed out".to_owned()));
        }
        let connects = mem::take(&mut proof.write()?.network.connect);
        for connect in connects {
            let handle = this
                .client
                .at(clock)
                .connect(
                    connect.peer,
                    &connect.authority,
                    connect.params,
                    connect.initial_dcid,
                )
                .map_err(Failure::Connect)?;
            let index = usize::try_from(handle.index()).map_err(Failure::Index)?;
            if index != connect.slot {
                return Err(Failure::Counter(
                    Metric::ConnectionOrder,
                    index,
                    connect.slot,
                ));
            }
        }
        let cancel = {
            let proof = proof.read()?;
            if let Some(cancel) = proof.cancel.request
                && !proof.cancel.issued
                && proof.cancel.ready
            {
                proof
                    .replies
                    .streams
                    .first()
                    .copied()
                    .flatten()
                    .map(|(conn, stream)| {
                        (
                            conn,
                            cancel.action,
                            stream,
                            proof.network.protocol.qpack.decoder.table_bytes != 0,
                        )
                    })
            } else {
                None
            }
        };
        if let Some((target, action, stream, decoder_open)) = cancel {
            let Some(handle) = this.client.sole() else {
                return Err(Failure::Scenario(
                    "cancellation target connection is no longer active".to_owned(),
                ));
            };
            if connection_key(handle) != target {
                return Err(Failure::Scenario(
                    "cancellation target connection identity changed".to_owned(),
                ));
            }
            this.client
                .at(clock)
                .with_conn(handle, |mut conn| match action {
                    CancelAction::RequestCancelled => {
                        cancel_stream(&mut conn, stream, frame::ErrorCode::RequestCancelled)
                    }
                    CancelAction::StopSending => {
                        stop_stream(&mut conn, stream, frame::ErrorCode::RequestCancelled)
                    }
                    CancelAction::Qpack
                    | CancelAction::QpackAcknowledgment
                    | CancelAction::QpackIncrement => {
                        use sark_h3::qpack::stream::Instruction;

                        let mut wire = Vec::new();
                        let mut streams = conn.streams();
                        let decoder = if decoder_open {
                            // start_scope opens client control stream 2, then decoder stream 6.
                            streams.view().resolve_send(
                                dope_quic::wire::stream::Id::new(6)
                                    .ok_or(dope_quic::conn::stream::Error::InvalidStream)?,
                            )?
                        } else {
                            wire.push(frame::STREAM_TYPE_QPACK_DECODER.get() as u8);
                            streams.send().open_uni()?
                        };
                        let instruction = match action {
                            CancelAction::QpackAcknowledgment => {
                                Instruction::SectionAcknowledgment {
                                    stream_id: stream.get(),
                                }
                            }
                            CancelAction::QpackIncrement => Instruction::InsertCountIncrement {
                                increment: std::num::NonZero::<u64>::MIN,
                            },
                            _ => Instruction::StreamCancellation {
                                stream_id: stream.get(),
                            },
                        };
                        instruction.encode(&mut wire);
                        streams.send().send(decoder, &wire)
                    }
                    CancelAction::KeyClose => {
                        conn.lifecycle()
                            .close(Code::ZERO, b"public H3 cancellation proof".to_vec());
                        Ok(())
                    }
                })
                .map_err(Failure::Access)?
                .map_err(Failure::Stream)?;
            proof.write()?.cancel.issued = true;
        }
        let followup = {
            let proof = proof.read()?;
            if proof.cancel.followup {
                None
            } else if proof.is_cancelled()
                && proof
                    .requests
                    .iter()
                    .any(|request| request.dispatch == Dispatch::AfterCancellation)
            {
                Some(Dispatch::AfterCancellation)
            } else if proof.has_failed()
                && proof
                    .requests
                    .iter()
                    .any(|request| request.dispatch == Dispatch::AfterFailure)
            {
                Some(Dispatch::AfterFailure)
            } else {
                None
            }
        };
        if let Some(dispatch) = followup {
            let Some(handle) = this.client.sole() else {
                return Err(Failure::Scenario(
                    "follow-up target connection is no longer active".to_owned(),
                ));
            };
            this.client
                .at(clock)
                .with_app(handle, |mut selected| {
                    let (_, link, net) = selected.split();
                    if let Err(error) = link.try_send(proof, net, dispatch) {
                        proof.fail(error);
                        return;
                    }
                    match proof.write() {
                        Ok(mut proof) => proof.cancel.followup = true,
                        Err(error) => proof.fail(error),
                    }
                })
                .map_err(Failure::Access)?;
        }
        let pending = {
            let mut proof = proof.write()?;
            if proof.is_cancelled() {
                proof.replies.finished[0] = true;
                proof.stop_done();
            }
            proof.cancel.request.is_some() && !proof.network.failures.done.get()
        };
        Ok(if pending {
            control::Flow::More
        } else {
            control::Flow::Idle
        })
    }
}

pub(crate) struct RunPlan<'a, Q: routing::LocalRoute, P> {
    pub(crate) plan: Plan<LISTENER, receive::Retained, Q>,
    pub(crate) proof: Rc<Shared<'a>>,
    pub(crate) client: client::Plan<DIALER>,
    pub(crate) reservation: P,
    pub(crate) task_capacity: slots::Slots,
}

impl<Q: routing::LocalRoute, P> settings::Resource for RunPlan<'_, Q, P> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = self.plan.include_in(needs)?;
        let needs = self.client.as_ref().include_in(needs)?;
        needs.include(
            Claim::new(settings::Routes::EMPTY).schedule(demands::Schedule::new(
                self.task_capacity,
                self.task_capacity,
            )),
        )
    }
}
