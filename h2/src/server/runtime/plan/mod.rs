use core::marker::PhantomData;
use std::num::NonZero;

use dope::core::{
    driver::{
        route::Identity,
        settings::{claim, errors},
    },
    io::socket::Listener,
};
pub(super) mod tasks;

use dope::{
    core::driver::{route::table::conn, settings, storage},
    manifold::{
        app::{env, receive},
        network::listener::{
            config, handler,
            topology::{Direct, Layout, factory},
        },
        timing,
    },
    net::{
        link::pool::recv::input,
        tcp,
        wire::{self, domain},
    },
    runtime::topology,
};
use tasks::Tasks;

use crate::{
    conn::config::Config,
    server::{
        self,
        runtime::{Local, Recipe, modes},
    },
};

type Plain<ID, F, M> = topology::Plan<
    (),
    Direct<ID, env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>, Local<F, M>>,
>;

type Bound<'a, ID, W, F, B, M> = topology::Plan<
    (),
    Direct<
        ID,
        env::Bundle<tcp::Tcp, W, timing::Throughput>,
        Local<F, M, (&'a domain::Owner<W, ID>, B)>,
    >,
>;

/// HTTP/2 listener inputs whose protocol is admitted before connections open.
///
/// A prepared listener cannot be retagged onto another driver route.
///
#[must_use = "an H2 plan must be completed into a topology component"]
pub struct Plan<ID: Identity> {
    pub(in crate::server) listener: config::Prepared<tcp::Tcp>,
    connection: server::Plan,
    bound: Option<Listener>,
    lifetime: Option<handler::Lifetime>,
    route: PhantomData<fn() -> ID>,
}

impl<ID: Identity> Plan<ID> {
    pub fn new(
        listener: config::Prepared<tcp::Tcp>,
        receive: settings::Receive,
        config: server::Config,
    ) -> Result<Self, server::ConfigError> {
        Ok(Self {
            listener,
            connection: server::Plan::new(config, receive)?,
            bound: None,
            lifetime: None,
            route: PhantomData,
        })
    }

    pub const fn connections(&self) -> conn::Capacity {
        self.listener.connections()
    }

    pub fn with_config(mut self, protocol: Config) -> Self {
        self.connection = self.connection.with_config(protocol);
        self
    }

    /// Selects per-connection stream storage before protocol admission.
    pub fn with_streams(mut self, streams: NonZero<usize>) -> Self {
        self.connection = self.connection.with_streams(streams);
        self
    }

    pub fn with_bound(mut self, listener: Listener) -> Self {
        self.bound = Some(listener);
        self
    }

    pub const fn with_life(mut self, lifetime: handler::Lifetime) -> Self {
        self.lifetime = Some(lifetime);
        self
    }

    /// Couples generated tasks to this exact listener and protocol plan.
    pub const fn tasks<F>(self, factory: F, limit: usize) -> Tasks<ID, F> {
        Tasks::new(self, factory, limit)
    }

    /// Keeps service storage borrowed through the complete plaintext listener run.
    pub fn with_server<S, F>(self, storage: S, recipe: F) -> topology::Plan<S, Recipe<ID, F>>
    where
        S: storage::Plan,
        F: modes::Family<S, ID>,
    {
        topology::Plan::new(storage, Recipe::new(self, recipe))
    }

    /// Builds plaintext listeners without a wire-storage admission path.
    pub fn with_plain<F, M>(self, service: F) -> Plain<ID, F, M>
    where
        M: modes::Failure + factory::ReceiveSpec,
    {
        let Self {
            listener,
            connection: conn,
            bound,
            lifetime,
            ..
        } = self;
        let install = Direct::new(listener, Local::new(service, conn, ())).with_bound(bound);
        let install = match lifetime {
            Some(lifetime) => install.with_life(lifetime),
            None => install,
        };
        topology::Plan::new((), install)
    }

    /// Binds wire resources which remain owned outside the driver scope.
    pub fn with_direct<W, F, B, M>(
        self,
        owner: &domain::Owner<W, ID>,
        service: F,
        bind: B,
    ) -> Bound<'_, ID, W, F, B, M>
    where
        W: input::Execution,
        M: modes::Failure + factory::ReceiveSpec,
        F: modes::Direct<M, W, ID>,
        B: domain::Plan<W, ID>,
    {
        let (listener, conn, bound, lifetime) = self.into_parts();
        let factory = Local::new(service, conn, (owner, bind));
        let install = Direct::new(listener, factory).with_bound(bound);
        let install = match lifetime {
            Some(life) => install.with_life(life),
            None => install,
        };
        topology::Plan::new((), install)
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        config::Prepared<tcp::Tcp>,
        server::Plan,
        Option<Listener>,
        Option<handler::Lifetime>,
    ) {
        (self.listener, self.connection, self.bound, self.lifetime)
    }
}

pub(super) fn receive_claim(receive: settings::Receive) -> claim::Claim {
    claim::Claim::new(settings::Routes::EMPTY).receive(receive.into())
}

impl<ID: Identity> settings::Resource for Plan<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = needs.include(receive_claim(self.connection.receive()))?;
        Layout::<ID, receive::Scoped>::new(self.listener.connections()).include_in(needs)
    }
}
