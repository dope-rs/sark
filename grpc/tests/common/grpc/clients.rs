use std::{convert::Infallible, net, time};

use dope::{
    core::driver::{life::recovery, route::KeyTag, settings::errors},
    manifold::{
        self,
        app::{Host, env},
        network::client::Pool,
        service::{self, bindings, sync},
        timing,
    },
    net::{tcp, wire},
    runtime::{
        executor::session,
        topology::{self, error},
    },
};
use dope_test::harness::GUARD;
use sark_grpc::{
    client::{
        config::Limits,
        manifold::{Driver, channel, settings},
    },
    protocol::section,
};
use sark_h2::{client, conn::config};

const MAX_CONNECTIONS: usize = u32::MAX as usize;
type Network = env::Bundle<tcp::Tcp, wire::Identity, timing::Balanced>;
type Provider<'life, 'e, 'd> =
    bindings::Bound<'life, 'd, channel::Port<'e, 'd>, Driver, KeyTag<0>, MAX_CONNECTIONS>;
type Owner<'q, 'h, 'p, 'life, 'e, 'd> = Host<
    Pool<
        'q,
        'h,
        'p,
        'd,
        'd,
        KeyTag<0>,
        MAX_CONNECTIONS,
        Provider<'life, 'e, 'd>,
        sync::Preserve,
        net::SocketAddr,
        Network,
    >,
>;

#[derive(Debug)]
pub enum Failure {
    Carrier(config::Error),
    Settings(settings::Error),
    Claim(errors::Claim),
}

#[derive(Debug)]
pub enum Error<'a> {
    Install(&'a manifold::BuildError<Infallible>),
    Session(session::Cause<'a>),
    Topology(
        error::Kind<
            'a,
            settings::Error,
            Result<Result<(), session::Cause<'a>>, &'a manifold::BuildError<Infallible>>,
        >,
    ),
}

pub struct Plan {
    settings: settings::Settings,
    address: net::SocketAddr,
}

impl Plan {
    pub fn new(address: net::SocketAddr, limits: Limits, capacity: usize) -> Result<Self, Failure> {
        let carrier = client::Plan::<section::Plan>::new(capacity).map_err(Failure::Carrier)?;
        let settings =
            settings::Settings::try_new(carrier, limits, 1).map_err(Failure::Settings)?;
        Ok(Self { settings, address })
    }

    pub fn enter<R, Out>(
        self,
        run: impl for<'app, 'life, 'q, 'h, 'p, 'e, 'd> FnOnce(
            Scope<'app, 'life, 'q, 'h, 'p, 'e, 'd>,
        ) -> R,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<R, Error<'a>>) -> Out,
    ) -> Result<Out, Failure> {
        let peers = service::Snapshot::from_one(
            service::Revision::new(1),
            service::Endpoint::new(self.address, self.address),
        );
        let (settings, install) = self
            .settings
            .install::<Network>(
                peers,
                time::Duration::from_millis(200),
                time::Duration::from_secs(1),
            )
            .map_err(Failure::Settings)?;
        let prepared = topology::Plan::new(settings, install)
            .for_profile::<timing::Balanced>()
            .map_err(Failure::Claim)?;
        let mut finish = session::Completion::new();
        let (prepared, completion, timeout) = (prepared, &mut finish, GUARD);
        Ok(prepared.try_scoped(
            |worker| {
                worker.lend(|setup, stage| {
                    setup.install(stage, |root, stage| {
                        topology::lending::Stage::with_quiesce(
                            stage,
                            completion,
                            root,
                            timeout,
                            |mut app| {
                                let handle = app.root_client();
                                run(Scope { app, handle })
                            },
                        )
                    })
                })
            },
            policy,
            |result| match result {
                Ok(Ok(Ok(result))) => done(Ok(result)),
                Ok(Ok(Err(error))) => done(Err(Error::Session(error.cause()))),
                Ok(Err(error)) => done(Err(Error::Install(&error))),
                Err(error) => done(Err(Error::Topology(error.kind().map(|output| {
                    output
                        .as_ref()
                        .map(|session| session.as_ref().map(|_| ()).map_err(session::Error::cause))
                })))),
            },
        ))
    }
}

pub struct Scope<'app, 'life: 'app, 'q, 'h, 'p, 'e: 'life, 'd: 'e> {
    pub app: session::App<'app, 'life, 'd, Owner<'q, 'h, 'p, 'life, 'e, 'd>>,
    pub handle: channel::Handle<'app, 'life, 'e, 'd, Provider<'life, 'e, 'd>>,
}
