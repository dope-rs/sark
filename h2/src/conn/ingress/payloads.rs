use o3::{
    alloc,
    buffer::{
        hold::{self, scope},
        pool::{self, owners, state},
        resident,
        segments::{self, queue},
    },
};
use sark_core::life::session;

use crate::conn::{self, event};

#[derive(Clone, Copy)]
pub(in crate::conn) struct Plan {
    pub(in crate::conn) permits: pool::Width<1>,
    pub(in crate::conn) segments: segments::Plan,
}

pub(super) struct Store<C: session::Storage> {
    permits: pool::Pool<state::Uninit, owners::Plain<pool::Width<1>>>,
    segments: segments::Pool<C>,
}

impl<C: session::Storage> Store<C> {
    pub(super) fn try_new(plan: Plan) -> Result<Self, alloc::Error> {
        Ok(Self {
            permits: pool::Pool::try_from(plan.permits)?,
            segments: segments::Pool::try_new(plan.segments)?,
        })
    }

    pub(super) fn try_retain(
        &self,
        bytes: queue::Slice<'_, C>,
        policy: &hold::Retention<C>,
    ) -> Result<event::DataPayload<C>, conn::Error> {
        let permit = self.try_permit()?;
        let segments = bytes.try_bound(&self.segments, policy, crate::INGRESS_RETENTION)?;
        Ok(event::DataPayload::new(segments, permit))
    }

    fn try_permit(
        &self,
    ) -> Result<pool::Span<owners::Plain<pool::Width<1>>, pool::Owned, pool::Frozen>, conn::Error>
    {
        let limit = self.permits.slots();
        let used = limit - self.permits.available();
        Ok(self
            .permits
            .acquire()
            .ok_or(conn::Error::PayloadFull { used, limit })?
            .freeze())
    }
}

impl<'d> Store<resident::Shared<'d>> {
    pub(super) fn try_scoped<R: hold::raw::Retain<'d>>(
        &self,
        source: scope::Bound<'_, 'd, R>,
    ) -> Result<event::DataPayload<resident::Shared<'d>>, conn::Error> {
        let permit = self.try_permit()?;
        let segments = self.segments.try_retain::<R, conn::Error>(source)?;
        Ok(event::DataPayload::new(segments, permit))
    }
}
