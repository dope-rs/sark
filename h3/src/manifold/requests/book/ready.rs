use crate::manifold::owners;
use sark_core::manifold::task::app;
use std::ops::ControlFlow;

use sark_core::body;
use sark_http::payload::response::{Method, status::Code};

use crate::manifold::requests::{
    self,
    book::Book,
    duplex::{self, work},
    task,
};

impl<'d, A: app::Runtime<'d>, M: owners::Handoff> Book<'d, A, M> {
    pub(in crate::manifold) fn with_duplex<R>(
        &mut self,
        run: impl for<'turn> FnOnce(work::Work<'turn, 'd, A::Duplex>) -> duplex::Next<R>,
    ) -> Option<R> {
        let live = self.entries.front()?;
        live.with_duplex(run)
    }

    pub(in crate::manifold) fn dispatch_one<E>(
        &mut self,
        route: impl FnOnce(
            requests::Id,
            task::Input<'d, A::Prepared, <A::Build as body::Build<'d>>::Output>,
        ) -> Result<A::Task, E>,
    ) -> Result<task::Dispatch<E>, requests::Error> {
        let Some(live) = self.entries.front() else {
            return Ok(task::Dispatch::Idle);
        };
        live.dispatch(route).map_err(|_| requests::Error::Dispatch)
    }

    pub(in crate::manifold) fn take_task(
        &mut self,
        id: requests::Id,
    ) -> Result<Option<task::Owned<'d, A::Task, M>>, requests::Error> {
        Ok(self.entries.identify(id)?.task())
    }

    pub(in crate::manifold) fn with_response<R>(
        &mut self,
        id: Option<requests::Id>,
        run: impl FnOnce(
            requests::Id,
            Method,
            Result<task::Owned<'d, A::Task, M>, Code>,
        ) -> ControlFlow<task::Owned<'d, A::Task, M>, R>,
    ) -> Result<Option<ControlFlow<(), (R, Option<bool>)>>, requests::Error> {
        let live = match id {
            Some(id) => self.entries.identify(id)?,
            None => match self.entries.front() {
                Some(live) => live,
                None => return Ok(None),
            },
        };
        Ok(live.with_response(run))
    }
}
