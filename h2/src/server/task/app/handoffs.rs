use std::fmt;

use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use o3::buffer::hold;
use sark_core::{
    life::{self, owners},
    manifold::{
        schedule::typed,
        task::{
            app::{Handoff, Runtime},
            response::{Event, State},
        },
    },
};
use sark_http::{
    field::metadata::section::rules,
    owner::{self, cause},
};

use crate::{
    frame::numbers,
    server::{
        self, state,
        task::{response, *},
    },
    stream,
};

pub(super) struct Completion<'a, 'state, 'd, ID: Identity + 'd, A, M, P, F>
where
    A: Runtime<'d, Target = Target<'d, ID>>,
    F: rules::Policy,
{
    pub(super) tasks: &'a mut registry::Table<'d, ID, A::Task>,
    pub(super) connection: conn::Id<'d, ID>,
    pub(super) task_index: &'a mut registry::Index,
    pub(super) state: &'a mut Borrowed<'state, 'd, A, F>,
    pub(super) limits: server::Config,
    pub(super) mode: &'a mut M,
    pub(super) report: P,
}

impl<'d, ID: Identity + 'd, A, M, P, F> Handoff<'d, Target<'d, ID>, A::Fixed>
    for Completion<'_, '_, 'd, ID, A, M, P, F>
where
    A: Runtime<'d, Target = Target<'d, ID>>,
    M: state::Transitions<conn::Id<'d, ID>>,
    P: FnMut(conn::Id<'d, ID>, stream::Key, &dyn fmt::Debug),
    F: rules::Policy,
{
    type Ready = Result<(), (stream::Key, numbers::ErrorCode)>;

    fn pending(self, target: Target<'d, ID>) {
        if !M::ENABLED {
            return;
        }
        let Some(entry) = self.tasks.owner(target, self.connection) else {
            return;
        };
        self.mode.observe(life::Event::TaskPollPending(identify(
            self.connection,
            entry.as_ref().stream,
        )));
    }

    fn complete(self, target: Target<'d, ID>, output: A::Fixed) -> Self::Ready {
        let Some(taken) = self
            .tasks
            .owner(target, self.connection)
            .and_then(|entry| registry::Table::take_target(self.task_index, entry))
        else {
            return Ok(());
        };
        if !taken.unreported {
            return Err((taken.stream, numbers::ErrorCode::InternalError));
        }
        let stream = taken.stream;
        drop(taken);
        let call = identify(self.connection, stream);
        self.mode.observe(life::Event::TaskPollReady(call));
        let next = self.state.activation(self.limits);
        self.state
            .responses
            .try_insert(
                stream,
                typed::Next::Control(response::Response::Fixed(output), next),
            )
            .map_err(rejected::<A>)?;
        state::Transitions::ready(self.mode, call);
        Ok(())
    }

    fn ready<E: fmt::Debug>(mut self, target: Target<'d, ID>, event: Event<'d, E>) -> Self::Ready {
        let Some(mut entry) = self.tasks.owner(target, self.connection) else {
            return Ok(());
        };
        let record = entry.as_mut();
        let stream = record.stream;
        let call = identify(self.connection, stream);
        let next = self.state.activation(self.limits);
        let mut acquired = hold::Charge::default();
        let head = matches!(&event, Event::Head);
        let value = match event {
            Event::Head => {
                if !record.unreported {
                    return Err((stream, numbers::ErrorCode::InternalError));
                }
                typed::Next::Control(response::Response::Task, next)
            }
            Event::Chunk(chunk) => {
                let Some(pending) = Pending::single(chunk.into_owner()) else {
                    unreachable!("chunk")
                };
                if M::ENABLED {
                    acquired = pending.retention();
                }
                typed::Next::Data(
                    response::Data::Body {
                        pending,
                        state: State::Streaming,
                    },
                    next,
                )
            }
            Event::End => {
                let Some(taken) = registry::Table::take_target(self.task_index, entry) else {
                    return Ok(());
                };
                drop(taken);
                return self
                    .state
                    .responses
                    .try_insert(stream, typed::Next::Control(response::Response::End, next))
                    .map_err(rejected::<A>);
            }
            Event::Failed(cause) => {
                crate::server::runtime::app::response(self.mode, call, &cause, &mut self.report);
                return Err((stream, numbers::ErrorCode::InternalError));
            }
        };
        self.state
            .responses
            .try_insert(stream, value)
            .map_err(rejected::<A>)?;
        if head {
            record.unreported = false;
            self.mode.observe(life::Event::TaskPollReady(call));
            state::Transitions::ready(self.mode, call);
        } else if !acquired.is_empty() {
            owners::Projection::new(self.mode).apply(owner::Effect::new(
                call,
                acquired,
                owner::Change::StagingAcquired(cause::Acquire::StreamChunk),
            ));
        }
        Ok(())
    }
}
