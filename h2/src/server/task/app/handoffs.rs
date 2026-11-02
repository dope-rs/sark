use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use sark_core::{
    life,
    manifold::{
        schedule::{self, typed},
        task::{
            app::{Handoff, Runtime},
            response::{Outcome, Output, Pack},
        },
    },
    route,
};

use crate::{
    server::{
        self, state,
        task::{response, *},
    },
    stream,
};

pub(super) struct Completion<'a, 'state, 'd, ID: Identity + 'd, A, M>
where
    A: Runtime<'d, Target = Target<'d, ID>>,
{
    pub(super) tasks: &'a mut registry::Table<'d, ID, A::Task>,
    pub(super) connection: conn::Id<'d, ID>,
    pub(super) task_index: &'a mut registry::Index,
    pub(super) state: &'a mut Borrowed<'state, 'd, A>,
    pub(super) limits: server::Config,
    pub(super) mode: &'a mut M,
}

impl<'a, 'state, 'd, ID: Identity + 'd, A, M> Handoff<'d, Target<'d, ID>, A::Completion>
    for Completion<'a, 'state, 'd, ID, A, M>
where
    A: Runtime<'d, Target = Target<'d, ID>>,
    M: state::Transitions<conn::Id<'d, ID>>,
{
    type Complete = schedule::InsertResult<stream::Key, Next<'d, A>>;

    fn pending(self, target: Target<'d, ID>) {
        if !M::ENABLED {
            return;
        }
        let Some((conn, stream)) = self.tasks.owner(target) else {
            return;
        };
        if conn != self.connection {
            return;
        }
        self.mode
            .observe(life::Event::TaskPollPending(identify(conn, stream)));
    }

    fn complete<R, S, P>(self, target: Target<'d, ID>, output: Output<'d, R, S>) -> Self::Complete
    where
        R: route::Task<'d, S>,
        P: Pack<'d, R, S, A::Completion>,
    {
        let conn = self.connection;
        let Some(taken) = self.tasks.take_target(self.task_index, target, conn) else {
            return Ok(());
        };
        let registry::Taken { stream, .. } = taken;
        let call = identify(conn, stream);
        self.mode.observe(life::Event::TaskPollReady(call));
        let value = match output.into_parts() {
            (.., Outcome::Rejected(reject)) => response::Response::Status(reject.status()),
            (input, Outcome::Response(output)) => {
                response::Response::Completion(P::pack(Output::response(input, output)))
            }
        };
        let next = self.state.activation(self.limits);
        match self
            .state
            .responses
            .try_insert(stream, typed::Next::Control(value, next))
        {
            Ok(()) => {
                state::Transitions::ready(self.mode, call);
                Ok(())
            }
            Err(failure) => Err(failure),
        }
    }
}
