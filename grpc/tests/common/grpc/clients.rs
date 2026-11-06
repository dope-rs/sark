use std::{convert::Infallible, net, num::NonZero, time};

use dope::{
    core::{
        driver::{self, life::recovery, route::KeyTag, settings::errors},
        io::socket,
    },
    manifold::{
        self,
        app::{Host, creation, env, receive},
        client::{app::request, session::codec::staging},
        network::{client::Pool, client::topology as connector, listener},
        service::{self, bindings, sync},
        timing,
    },
    net::{tcp, wire},
    runtime::{
        executor::session,
        topology::{self, error, lending},
    },
};
use dope_fiber::net::{read, server};
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
type Setup = connector::Install<
    KeyTag<0>,
    MAX_CONNECTIONS,
    Network,
    channel::Binding,
    connector::plan::Standard,
    sync::Preserve,
    net::SocketAddr,
    16,
    connector::plan::Local<staging::Direct>,
    request::Enabled,
>;
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
type Peer<'n, 'p, 's, 'port, 'w, 'd> = server::Listener<
    'n,
    'p,
    's,
    'port,
    'w,
    'd,
    KeyTag<1>,
    tcp::Tcp,
    wire::Identity,
    for<'a> fn(wire::open::Error<'a, wire::Identity>),
>;

#[pin_project::pin_project]
#[derive(dope::Application)]
pub struct Root<A, B> {
    #[pin]
    #[app]
    grpc: A,
    #[pin]
    #[manifold]
    peer: B,
}

#[derive(Debug)]
pub enum Failure {
    Carrier(config::Error),
    Settings(settings::Error),
    Claim(errors::Claim),
    Receive(read::Capacity),
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

pub struct Plan(topology::Plan<settings::Settings, Setup>);

impl Plan {
    pub fn new(address: net::SocketAddr, limits: Limits, capacity: usize) -> Result<Self, Failure> {
        Self::with_waiters(address, limits, capacity, 1)
    }

    pub fn with_waiters(
        address: net::SocketAddr,
        limits: Limits,
        capacity: usize,
        waiters: usize,
    ) -> Result<Self, Failure> {
        let carrier = client::Plan::<section::Plan>::new(capacity).map_err(Failure::Carrier)?;
        let settings =
            settings::Settings::try_new(carrier, limits, waiters).map_err(Failure::Settings)?;
        let peers = service::Snapshot::from_one(
            service::Revision::new(1),
            service::Endpoint::new(address, address),
        );
        let (settings, install) = settings
            .install::<Network>(
                peers,
                time::Duration::from_millis(200),
                time::Duration::from_secs(1),
            )
            .map_err(Failure::Settings)?;
        Ok(Self(topology::Plan::new(settings, install)))
    }

    pub fn enter<R, Out>(
        self,
        run: impl for<'app, 'life, 'q, 'h, 'p, 'e, 'd> FnOnce(
            session::App<'app, 'life, 'd, Owner<'q, 'h, 'p, 'life, 'e, 'd>>,
            channel::Handle<'app, 'life, 'e, 'd, Provider<'life, 'e, 'd>>,
        ) -> R,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<R, Error<'a>>) -> Out,
    ) -> Result<Out, Failure> {
        let prepared = self
            .0
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
                                run(app, handle)
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

    pub fn enter_peer<R, Out>(
        self,
        bound: socket::Listener,
        run: impl for<'app, 'life, 'q, 'h, 'p, 'e, 'd, 'n, 'g, 's, 'port, 'w, 'handle> FnOnce(
            session::App<
                'app,
                'life,
                'd,
                Root<Owner<'q, 'h, 'p, 'life, 'e, 'd>, Peer<'n, 'g, 's, 'port, 'w, 'd>>,
            >,
            channel::Handle<'app, 'life, 'e, 'd, Provider<'life, 'e, 'd>>,
            server::Handle<'handle, 'w, 'd, wire::Identity, KeyTag<1>>,
        ) -> R,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<R, Error<'a>>) -> Out,
    ) -> Result<Out, Failure> {
        let capacity = driver::route::table::conn::Capacity::fixed::<1>();
        let peer = server::Plan::<wire::Identity, KeyTag<1>>::new(capacity, wire::Identity)
            .with_receive(read::Config {
                slots: NonZero::<u32>::MIN.saturating_add(3),
                chunks: NonZero::<u32>::MIN.saturating_add(3),
                bytes: NonZero::<u32>::MIN.saturating_add((64 << 10) - 1),
            })
            .map_err(Failure::Receive)?;
        let (storage, setup) = self.0.into_parts();
        let prepared = topology::Plan::new(
            storage,
            (
                setup,
                listener::topology::Layout::<KeyTag<1>, receive::Owned>::new(capacity),
            ),
        )
        .with_config(
            driver::settings::Config::for_profile::<timing::Balanced>()
                .with_receive(driver::settings::Receive::fixed::<4, { 16 << 10 }>()),
        )
        .map_err(Failure::Claim)?;
        let mut finish = session::Completion::new();
        Ok(prepared.try_scoped(
            |worker| {
                worker.lend(|(setup, _), stage| {
                    let port = peer
                        .try_build(stage.driver().storage)
                        .map_err(creation::Error::Allocation)
                        .map_err(manifold::BuildError::Create)?;
                    setup.install(stage, |root, stage| {
                        let hash = stage.hash_state(listener::Domain::for_listener(1));
                        server::Listener::<KeyTag<1>, tcp::Tcp, wire::Identity, _>::adopt(
                            &port,
                            server::Config {
                                direct_flights: Default::default(),
                                bind: bound.local_addr(),
                                backlog: socket::Backlog::fixed::<1>(),
                                stream: Default::default(),
                                transport: Default::default(),
                                egress: Default::default(),
                                bound,
                                wire: wire::domain::Local::new(wire::Identity),
                            },
                            (|cause| cause.eliminate())
                                as fn(wire::open::Error<'_, wire::Identity>),
                            hash,
                            stage,
                            lending::Stage::driver,
                            |listener, stage| {
                                let peer = listener.handle();
                                stage.with_quiesce(
                                    &mut finish,
                                    Root {
                                        grpc: root,
                                        peer: listener,
                                    },
                                    GUARD,
                                    |mut app| {
                                        let client = app.client(|root| root.project_ref().grpc);
                                        run(app, client, peer)
                                    },
                                )
                            },
                        )
                        .map_err(manifold::BuildError::Create)
                    })?
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
