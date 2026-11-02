use std::marker;

use dope::{
    core::{
        BranchError,
        driver::{
            route::Identity,
            settings,
            settings::{claim, errors},
        },
    },
    manifold::{app::env, network::listener::topology::factory, timing},
    net::{
        link::pool::recv::input,
        tcp,
        wire::{self, domain},
    },
};

use crate::server::{
    self,
    runtime::{modes, plan::receive_claim},
};

/// Owns the service and binding whose resources outlive driver construction.
pub struct Local<F, M, B = ()> {
    service: F,
    connection: server::Plan,
    binding: B,
    mode: marker::PhantomData<fn() -> M>,
}

impl<F, M, B> Local<F, M, B> {
    pub(in crate::server) const fn new(service: F, plan: server::Plan, binding: B) -> Self {
        Self {
            service,
            connection: plan,
            binding,
            mode: marker::PhantomData,
        }
    }
}

impl<F, M, B> settings::Resource for Local<F, M, B> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(receive_claim(self.connection.receive()))
    }
}

impl<F, M: factory::ReceiveSpec, B> factory::ReceiveSpec for Local<F, M, B> {
    type Input = M::Input;
}

impl<ID: Identity, F, M>
    factory::Direct<ID, env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>> for Local<F, M>
where
    M: modes::Failure + factory::ReceiveSpec,
    F: modes::Direct<M, wire::Identity, ID>,
{
    type Application<'d>
        = F::Application<'d>
    where
        ID: 'd,
        Self: 'd;
    type Error = M::Error;

    type Wire<'d>
        = domain::Local<'d, wire::Identity, ID>
    where
        ID: 'd,
        Self: 'd;

    fn build<'d>(self) -> Result<(Self::Application<'d>, Self::Wire<'d>), Self::Error>
    where
        ID: 'd,
        Self: 'd,
    {
        let app = F::build(self.service, self.connection)?;
        Ok((app, domain::Local::new(Default::default())))
    }
}

impl<ID: Identity, F, M, W, B> factory::Direct<ID, env::Bundle<tcp::Tcp, W, timing::Throughput>>
    for Local<F, M, (&domain::Owner<W, ID>, B)>
where
    W: input::Execution,
    W::Bootstrap: domain::Bootstrap<W>,
    M: modes::Failure + factory::ReceiveSpec,
    F: modes::Direct<M, W, ID>,
    B: domain::Plan<W, ID>,
{
    type Application<'d>
        = F::Application<'d>
    where
        ID: 'd,
        Self: 'd,
        W: 'd;
    type Wire<'d>
        = domain::Endpoint<'d, W, ID>
    where
        ID: 'd,
        Self: 'd,
        W: 'd;
    type Error = BranchError<B::Error, M::Error>;

    fn build<'d>(self) -> Result<(Self::Application<'d>, Self::Wire<'d>), Self::Error>
    where
        ID: 'd,
        Self: 'd,
        W: 'd,
        Self::Input: 'd,
    {
        let (owner, bind) = self.binding;
        let endpoint = owner.bind(bind).map_err(BranchError::First)?;
        let app = F::build(self.service, self.connection).map_err(BranchError::Second)?;
        Ok((app, endpoint))
    }
}
