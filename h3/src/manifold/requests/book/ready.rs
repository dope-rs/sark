use sark_core::{body, decode::request, duplex::Session};
use sark_http::payload::response::{Method, status::Code};

use crate::manifold::requests::{
    self,
    book::Book,
    duplex::{self, work},
    entries::status,
    task,
};

impl<'d, P: request::Prepared, B: body::Build<'d>, T, D: Session<'d>> Book<'d, P, B, T, D> {
    pub(in crate::manifold) fn with_duplex<R>(
        &mut self,
        run: impl for<'turn> FnOnce(work::Work<'turn, 'd, D>) -> duplex::Next<R>,
    ) -> Option<R> {
        let live = self.entries.front()?;
        live.output().with_duplex(run)
    }

    pub(in crate::manifold) fn with_status<R>(
        &mut self,
        run: impl FnOnce(requests::Id, Method, Code) -> status::Next<R>,
    ) -> Result<Option<(R, bool)>, requests::Error> {
        let Some(live) = self.entries.front() else {
            return Ok(None);
        };
        live.output().with_status(run)
    }

    pub(in crate::manifold) fn dispatch_one<E>(
        &mut self,
        route: impl FnOnce(requests::Id, task::Input<'d, P, B::Output>) -> Result<T, E>,
    ) -> Result<task::Dispatch<E>, requests::Error> {
        let Some(live) = self.entries.front() else {
            return Ok(task::Dispatch::Idle);
        };
        live.dispatch(route).map_err(|_| requests::Error::Dispatch)
    }

    pub(in crate::manifold) fn take_task(
        &mut self,
        id: requests::Id,
    ) -> Result<Option<T>, requests::Error> {
        Ok(self.entries.identify(id)?.task())
    }

    pub(in crate::manifold) fn close_task(
        &mut self,
        id: requests::Id,
    ) -> Result<task::Close, requests::Error> {
        Ok(self.entries.identify(id)?.close_task())
    }
}
