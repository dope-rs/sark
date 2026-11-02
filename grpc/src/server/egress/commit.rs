//! Linear response commit into the resident call and egress stores.

use sark_core::{
    life::session,
    manifold::{self, schedule},
};

use crate::{
    Reason, metadata,
    server::{calls, egress::draft, service, service::reply},
};

pub(in crate::server) struct Commit<'a, 'r, K, T, C, P, B>
where
    K: manifold::raw::Key,
    C: session::Storage,
    B: schedule::Block,
{
    calls: &'a mut calls::Store<'r, K, T, C, P, B>,
}

impl<'a, 'r, K, T, C, P, B> Commit<'a, 'r, K, T, C, P, B>
where
    K: manifold::raw::Key,
    C: session::Storage,
    B: schedule::Block,
{
    pub(in crate::server) fn new(calls: &'a mut calls::Store<'r, K, T, C, P, B>) -> Self {
        Self { calls }
    }

    pub(in crate::server) fn response<'d>(
        self,
        response: service::Response<'d, draft::Draft<'d, K>>,
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
        self,
        response: service::Response<'d, draft::Draft<'d, K>>,
    ) -> Result<(), crate::Status> {
        let (metadata, trailers, draft) = response.try_open()?;
        self.apply(draft, metadata, trailers, None, draft::Lifecycle::Open)
    }

    pub(in crate::server) fn reply<'d>(
        self,
        reply: reply::Reply<'d, draft::Draft<'d, K>>,
    ) -> Result<(), crate::Status> {
        let (metadata, trailers, status, draft) = reply.into_parts();
        self.apply(draft, metadata, trailers, status, draft::Lifecycle::Keep)
    }

    fn apply(
        self,
        draft: draft::Draft<'_, K>,
        metadata: metadata::Metadata,
        trailers: metadata::Metadata,
        status: Option<crate::Status>,
        life: draft::Lifecycle,
    ) -> Result<(), crate::Status> {
        let prepared = draft.prepare(metadata, trailers, status, life)?;
        let key = prepared.key();
        let work = prepared.work();
        match self.calls.records.update(key, move |call| {
            let output = call
                .output_mut()
                .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::ReplyInactive))?;
            prepared.apply(output);
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
