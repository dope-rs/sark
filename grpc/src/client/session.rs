use std::marker;

use dope::{
    core::driver::route::Identity,
    manifold::client::app::request,
    net::link::egress::data::{compose, pair},
};
use io::{Failure, Flush};
use o3::{
    alloc,
    buffer::{pool, resident, storage},
};
use receive::policies::{Notify, Policy as _};
use sark_core::{
    life::session::Storage,
    manifold::client::{
        Connection,
        domain::Plan,
        io::{Offer, Prepared as _, Reject, Write as _},
    },
};
use sark_h2::{client, conn, stream};
use state::plan;

use crate::{
    client::{config::Limits, io, receive, state},
    protocol::{request::Scheme, section},
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Create<B, S> {
    Connection(B),
    CallDomain(S),
    CallRequests(alloc::Error),
    Output(alloc::Error),
    Ingress(alloc::Error),
}

pub struct Session<'scope, P, C = storage::Shared>
where
    C: Storage,
    P: Plan<C, Connection: Connection<Error = conn::Error>>,
{
    pub(in crate::client) brand: marker::PhantomData<fn(&'scope ()) -> &'scope ()>,
    pub(in crate::client) state: state::State<P, C>,
}

impl<'d> Session<'_, client::Plan<section::Plan>, resident::Shared<'d>> {
    /// Binds receive copies to one resident domain, rejecting a different domain.
    pub fn try_bind(&mut self, budget: resident::Budget<'d>) -> Result<(), conn::Error> {
        self.state.connection.ingress().try_bind(budget)
    }
}

impl<P> Session<'_, P, storage::Shared>
where
    P: Plan<
            storage::Shared,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    /// Runs a client session under a fresh invariant capability brand.
    pub fn with<R>(
        carrier: P,
        run: impl for<'scope> FnOnce(Session<'scope, P>) -> R,
    ) -> Result<R, Create<P::Build, P::Storage>> {
        Self::with_limits(carrier, Limits::default(), run)
    }
}

impl<P, C> Session<'_, P, C>
where
    C: Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    /// Selects the origin scheme emitted by subsequent requests.
    pub fn with_scheme(mut self, scheme: Scheme) -> Self {
        self.state.scheme = scheme;
        self
    }

    /// Runs a session whose retained responses preserve storage type `C`.
    pub fn with_limits<R>(
        carrier: P,
        limits: Limits,
        run: impl for<'scope> FnOnce(Session<'scope, P, C>) -> R,
    ) -> Result<R, Create<P::Build, P::Storage>> {
        let plan = plan::Plan::new(limits, carrier);
        let conn = carrier.open().map_err(Create::Connection)?;
        let session = Session::try_plan(plan, conn)?;
        Ok(run(session))
    }

    pub(in crate::client) fn flush<'notify, 'd, ID: Identity>(
        &mut self,
        prefix: pool::Cursor,
        requests: &mut request::Requests<
            '_,
            '_,
            '_,
            'd,
            compose::Either<pool::Cursor, pair::Source<C>>,
            ID,
        >,
        done: &mut Notify<'notify, '_, '_>,
    ) -> Result<Flush, Failure> {
        let before = self.state.connection.outbound_len();
        {
            let prepared = match self.state.connection.prepare(prefix) {
                Ok(prepared) => prepared,
                Err(Reject::Empty(_)) => return Ok(Flush::Empty),
                Err(Reject::Failed { cause, prefix: _ }) => {
                    return Err(Failure::Prepare(cause));
                }
            };
            match prepared.try_bind().map_err(Failure::Payload)? {
                Offer::Prefix(value) => match requests.admit(value) {
                    request::Admission::Ready(ready) => ready.commit_left(),
                    request::Admission::Exhausted(_) => return Ok(Flush::Exhausted),
                    request::Admission::Saturated(_) => return Ok(Flush::Saturated),
                    request::Admission::Rejected(cause) => {
                        return Err(Failure::Reserve(cause.cause()));
                    }
                },
                Offer::Body(value) => match requests.admit(value) {
                    request::Admission::Ready(ready) => ready.commit_right(),
                    request::Admission::Exhausted(_) => return Ok(Flush::Exhausted),
                    request::Admission::Saturated(_) => return Ok(Flush::Saturated),
                    request::Admission::Rejected(cause) => {
                        return Err(Failure::Reserve(cause.cause()));
                    }
                },
            }
        }
        let egress = io::release(before, self.state.connection.outbound_len());
        self.state
            .calls
            .drive(&mut self.state.connection, egress, &mut |record| {
                let _ = done.cancel(&mut self.state.ingress, record.response);
            })
            .map_err(Failure::Drive)?;
        Ok(Flush::Written)
    }

    pub(in crate::client) fn try_plan<'scope>(
        plan: plan::Plan<P>,
        conn: P::Connection,
    ) -> Result<Session<'scope, P, C>, Create<P::Build, P::Storage>> {
        Ok(Session {
            brand: marker::PhantomData,
            state: state::State::try_new(plan, conn)?,
        })
    }
}
