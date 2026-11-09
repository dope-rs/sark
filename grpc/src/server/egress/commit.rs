//! Linear response commit into the resident call and egress stores.

use sark_core::{life::session, manifold::schedule};
use sark_h2::stream::Key;

use crate::{
    Reason, metadata,
    server::{calls, egress::draft, service, service::reply},
};

impl<'r, T, C, P, B> calls::Store<'r, T, C, P, B>
where
    C: session::Storage,
    B: schedule::Block,
{
    pub(in crate::server) fn response<'d>(
        &mut self,
        response: service::Response<'d, draft::Draft<'d, Key>>,
    ) -> Result<(), crate::Status> {
        let (metadata, trailers, status, draft) = response.into_parts();
        self.apply(
            draft,
            metadata,
            trailers,
            Some(status),
            draft::Lifecycle::Keep,
        )
    }

    pub(in crate::server) fn open<'d>(
        &mut self,
        response: service::Response<'d, draft::Draft<'d, Key>>,
    ) -> Result<(), crate::Status> {
        let (metadata, trailers, draft) = response.try_open()?;
        self.apply(draft, metadata, trailers, None, draft::Lifecycle::Open)
    }

    pub(in crate::server) fn reply<'d>(
        &mut self,
        reply: reply::Reply<'d, draft::Draft<'d, Key>>,
    ) -> Result<(), crate::Status> {
        let (metadata, trailers, status, draft) = reply.into_parts();
        self.apply(draft, metadata, trailers, status, draft::Lifecycle::Keep)
    }

    fn apply(
        &mut self,
        draft: draft::Draft<'_, Key>,
        metadata: metadata::Metadata,
        trailers: metadata::Metadata,
        status: Option<crate::Status>,
        life: draft::Lifecycle,
    ) -> Result<(), crate::Status> {
        let prepared = draft.prepare(metadata, trailers, status, life)?;
        let key = prepared.key();
        let work = prepared.work();
        match self.update(key, move |call| {
            let call = call
                .active()
                .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::ReplyInactive))?;
            prepared.apply(&mut call.output);
            Ok((work, ()))
        }) {
            Ok(()) => Ok(()),
            Err(schedule::Error::Missing) => Err(crate::Status::new(
                crate::Code::Internal,
                Reason::ReplyUnknown,
            )),
            Err(schedule::Error::Rejected(status)) => Err(status),
        }
    }
}
