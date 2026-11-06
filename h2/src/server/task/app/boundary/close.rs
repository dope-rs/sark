use std::pin;

use dope::{
    core::driver::{route::Identity, schedule::ready::task::owner},
    manifold::network::listener::conn::Id,
    net::wire,
};
use sark_core::{
    life,
    manifold::{server::app as semantic, task::app::Runtime},
};

use crate::server::{
    runtime::output::flush,
    task::{Context, Retired, *},
};

pub(in crate::server::task::app) struct Close<'a, 'd, ID: Identity + 'd, R, M>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
{
    pub(super) router: pin::Pin<&'a mut R>,
    pub(super) tasks: &'a mut registry::Table<'d, ID, R::Task>,
    pub(super) mode: &'a mut M,
}

impl<'d, ID: Identity + 'd, R, M> Close<'_, 'd, ID, R, M>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
{
    pub(in crate::server::task::app) fn new<'a>(
        router: pin::Pin<&'a mut R>,
        tasks: &'a mut registry::Table<'d, ID, R::Task>,
        mode: &'a mut M,
    ) -> Close<'a, 'd, ID, R, M> {
        Close {
            router,
            tasks,
            mode,
        }
    }

    pub(in crate::server::task::app) fn apply<W, D, A>(
        self,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, R, W, M, D, A>,
    ) where
        W: wire::Spec,
        M: Transport<'d, ID>,
        M::Output: flush::Evidence<'d>,
        D: timers::Policy<'d>,
        A: service::Select<R> + semantic::Application<'d, service::Connection<'d, A>, Id<'d, ID>>,
    {
        let Self {
            mut router,
            tasks,
            mode,
        } = self;
        let id = conn.id();
        mode.observe(life::Event::SessionClosing(id));
        let state = conn.state_mut();
        let index = &mut state.task_index;
        if !index.is_empty() {
            for at in 0..index.capacity() {
                let Some(stream) = index.key_at(at) else {
                    continue;
                };
                let Some(taken) = tasks.take_stream(index, stream) else {
                    continue;
                };
                let (_, stream, task, unreported) = taken.into_parts();
                Runtime::cancel(router.as_mut(), task);
                if unreported {
                    mode.observe(life::Event::TaskCancelled(identify(id, stream)));
                }
                if state.book.responses.entry(stream).is_none() {
                    identify(id, stream).retire(mode, life::Outcome::Cancelled);
                }
            }
            index.clear();
        }
        if let Err(cause) = Runtime::try_retire(router.as_ref(), owner::Owner::from(id)) {
            life::Mode::diagnostic(mode, &cause);
        }
        state.turn().close(id, mode);
    }

    pub(in crate::server::task::app) fn audit<W, D, A>(
        self,
        conn: &mut Retired<'_, '_, 'd, ID, R, W, M, D, A>,
    ) where
        W: wire::Spec,
        M: Transport<'d, ID>,
        M::Output: flush::Evidence<'d>,
        D: timers::Policy<'d>,
        A: service::Select<R> + semantic::Application<'d, service::Connection<'d, A>, Id<'d, ID>>,
    {
        let id = conn.id();
        let bytes = conn.response_len();
        let state = conn.state_mut();
        let count = state.task_index.len();
        let empty = state.task_index.is_empty();
        life::Mode::close(self.mode, id, || {
            state.turn().audit(count, empty).wire_bytes(bytes)
        });
    }
}
