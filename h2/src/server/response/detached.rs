use core::ops;
use std::convert::Infallible;

use o3::buffer::hold::evidence::{Mode, Off};
use sark_http::{
    field::metadata::section::rules,
    owner::handoff::{self, stage},
};

use crate::{
    conn::{self, event},
    server::response::{Handoff, Head, Payload, Receipt, head::Staged},
};

pub(in crate::server) struct Detached<
    'a,
    'd,
    F: rules::Policy,
    M: Mode = Off,
    E: event::Evidence = event::Retirement,
    D = Infallible,
> {
    pub(in crate::server::response) staged: Staged<'a, 'd, F, E>,
    pub(in crate::server::response) body: Payload<handoff::Pending<'d, M, D>>,
}

impl<'a, 'd, F: rules::Policy, M: Handoff, E: event::Evidence, D: stage::Deferred<'d>>
    Detached<'a, 'd, F, M, E, D>
{
    pub(in crate::server) fn defer(
        self,
        request: D::Owner,
    ) -> Result<ops::ControlFlow<Head<'d>, Receipt<'d>>, conn::Error> {
        let Self { staged, body } = self;
        let receipt = match body {
            Payload::Handoff(body) => M::finish(body, request)?,
            Payload::Inline(body) => {
                drop(request);
                Receipt::inline(body)
            }
        };
        staged.defer(receipt)
    }
}
