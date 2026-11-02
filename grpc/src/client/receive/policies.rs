use dope::core::driver::schedule::ready::wake::{Waker, table};
use receive::budget;
use sark_core::{
    life::session,
    manifold::client::{Connection, domain::Plan},
};
use sark_h2::{conn, stream};

use crate::{
    client::{receive, state::calls},
    protocol::section,
};

pub(in crate::client) trait Policy<P, C>
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    fn cancel(
        &mut self,
        asm: &mut receive::Assembler<P, C>,
        response: Option<calls::Response<C>>,
    ) -> Option<calls::Target> {
        let response = response?;
        let target = response.target;
        asm.cancel_reply(response);
        Some(target)
    }

    fn push_unary(
        &mut self,
        asm: &mut receive::Assembler<P, C>,
        key: receive::UnaryKey,
        value: Result<receive::UnaryResult<C>, crate::Status>,
        charge: Option<budget::Charge>,
    ) -> Result<(), crate::Status> {
        asm.claims
            .push_unary(key, value, charge)
            .map_err(|error| error.status(&mut asm.fault, &mut asm.budget))
    }

    fn push_event(
        &mut self,
        asm: &mut receive::Assembler<P, C>,
        key: receive::StreamKey,
        value: Result<receive::Response<C>, crate::Status>,
        charge: Option<budget::Charge>,
    ) -> Result<(), crate::Status> {
        asm.claims
            .push_event(key, value, charge)
            .map_err(|error| error.status(&mut asm.fault, &mut asm.budget))
    }
}

pub(in crate::client) struct Direct;

pub(in crate::client) struct Notify<'scope, 'life, 'd> {
    scope: receive::Scope<'scope>,
    done: table::Sink<'scope, 'life, 'd>,
}

impl<'scope, 'life, 'd> Notify<'scope, 'life, 'd> {
    pub(in crate::client) fn new(
        scope: receive::Scope<'scope>,
        done: table::Sink<'scope, 'life, 'd>,
    ) -> Self {
        Self { scope, done }
    }

    fn complete(&self, index: receive::Index) {
        use table::raw::Index as _;

        self.scope.slot(index).wake(&self.done);
    }

    pub(in crate::client) fn try_register<'target>(
        &self,
        index: receive::Index,
        wait: &mut table::Registration<'target, 'd>,
        wake: &Waker<'d>,
    ) -> Result<(), table::Error>
    where
        'life: 'target,
    {
        use table::raw::Index as _;

        self.scope.slot(index).try_register(&self.done, wait, wake)
    }
}

impl<P, C> Policy<P, C> for Direct
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
}

impl<P, C> Policy<P, C> for Notify<'_, '_, '_>
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    fn cancel(
        &mut self,
        asm: &mut receive::Assembler<P, C>,
        response: Option<calls::Response<C>>,
    ) -> Option<calls::Target> {
        let response = response?;
        let target = response.target;
        if let receive::Cancel::Complete(slot) = asm.cancel_reply(response) {
            self.complete(slot);
        }
        Some(target)
    }

    fn push_unary(
        &mut self,
        asm: &mut receive::Assembler<P, C>,
        key: receive::UnaryKey,
        value: Result<receive::UnaryResult<C>, crate::Status>,
        charge: Option<budget::Charge>,
    ) -> Result<(), crate::Status> {
        let slot = key.slot();
        Direct.push_unary(asm, key, value, charge)?;
        self.complete(slot);
        Ok(())
    }

    fn push_event(
        &mut self,
        asm: &mut receive::Assembler<P, C>,
        key: receive::StreamKey,
        value: Result<receive::Response<C>, crate::Status>,
        charge: Option<budget::Charge>,
    ) -> Result<(), crate::Status> {
        let slot = key.slot(asm.claims.offset);
        Direct.push_event(asm, key, value, charge)?;
        self.complete(slot);
        Ok(())
    }
}
