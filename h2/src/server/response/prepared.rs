use std::{convert::Infallible, ops};

use o3::buffer::{bytes::scope, hold::evidence::Off, storage::Shared};
use sark_core::{decode, request, request::frame::proof};
use sark_http::{
    field::metadata::section::rules,
    owner::handoff::{self, stage},
    request::body,
};

use crate::{
    conn::{self, event},
    server::response::{Detached, Handoff, Head, Pending, Receipt, head::Staged},
    stream,
};

#[doc(hidden)]
pub struct Prepared<
    'a,
    'req,
    'd,
    F: rules::Policy,
    E: event::Evidence = event::Retirement,
    S = Infallible,
    B = body::Retained<'req, 'd>,
> {
    pub(in crate::server::response) staged: Staged<'a, 'd, F, E>,
    pub(in crate::server::response) body: handoff::Plan<'req, 'd, S, B>,
}

impl<'a, 'req, 'd: 'req, F: rules::Policy, E: event::Evidence, B: stage::Body<'req, 'd>>
    Prepared<'a, 'req, 'd, F, E, Infallible, B>
{
    pub(crate) fn stream(&self) -> stream::Key {
        self.staged.stream
    }

    #[doc(hidden)]
    pub fn discard(self) {
        drop(self);
    }

    #[doc(hidden)]
    pub fn finish(self) -> Result<Option<Pending<'d>>, conn::Error> {
        let Self { staged, body } = self;
        let receipt = Off::resolve(body)?;
        staged.commit()?;
        Ok(receipt.into_pending())
    }

    pub(in crate::server) fn defer<M: Handoff>(
        self,
    ) -> Result<ops::ControlFlow<Head<'d>, Receipt<'d>>, conn::Error> {
        let Self { staged, body } = self;
        let receipt = M::resolve(body)?;
        staged.defer(receipt)
    }

    pub(in crate::server) fn detach<M: Handoff>(
        self,
    ) -> Result<Detached<'a, 'd, F, M, E, B::Deferred>, conn::Error>
    where
        B::Deferred: stage::Deferred<'d>,
    {
        let Self { staged, body } = self;
        let body = M::detach(body)?;
        Ok(Detached { staged, body })
    }
}

impl<'a, 'req, 'd: 'req, 'id, F: rules::Policy, E: event::Evidence, B: stage::Body<'req, 'd>>
    decode::Lineage<'id> for Prepared<'a, 'req, 'd, F, E, scope::Range<'id>, B>
{
    type Output = Prepared<'a, 'req, 'd, F, E, Infallible, B>;

    fn resolve_head<O: Into<scope::Owner<'id, Shared>>, P: proof::Proof>(
        self,
        head: request::Head<O, P>,
    ) -> Self::Output {
        let Self { staged, body } = self;
        Prepared {
            staged,
            body: body.resolve_head(head.into_owner()),
        }
    }
}
