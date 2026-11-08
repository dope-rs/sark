//! Execution-neutral H3-over-QUIC startup planning.

use std::net::SocketAddr;

use dope::core::driver::{
    self, settings,
    settings::{claim, errors},
};
use dope_quic::{
    conn::life::options,
    proto::datagram,
    runtime::{
        endpoint::{Endpoint, Run, config, owner::Owned, server},
        mux::app,
        receive, routing,
    },
    rustls::ServerConfig,
    stream::Retain,
};
use o3::buffer::resident;
use sark_core::manifold::task::app::Bind;
use sark_http::payload::compress::dynamic::hash;

use crate::manifold::server::{
    Config, ConfigError, Handle, Handler, Mode, Setup, deadline, failure, rotation, startup,
};

/// Validated H3 and QUIC startup authority for one driver route.
#[must_use = "an H3 plan must be bound to its generated application"]
pub struct Plan<
    'g,
    const ID: u8,
    R: receive::Policy = receive::Retained,
    Q: routing::LocalRoute = routing::Plain,
> {
    endpoint: server::Plan<'g, ID, R, Q>,
    config: Setup,
}

impl<const ID: u8> Plan<'static, ID> {
    pub fn try_new(
        bind: SocketAddr,
        tls: ServerConfig,
        conn: options::Options,
        ep: config::Config,
        app: Config,
    ) -> Result<Self, startup::PrepareError> {
        let config = Setup::try_new(app).map_err(startup::PrepareError::Application)?;
        let endpoint =
            server::Plan::new(bind, tls, conn, ep).map_err(startup::PrepareError::Endpoint)?;
        Ok(Self { endpoint, config })
    }
}

impl<'g, const ID: u8, R: receive::Policy, Q: routing::LocalRoute> Plan<'g, ID, R, Q> {
    /// Adds validated H3 limits to an existing QUIC plan without rebuilding its authority.
    pub fn try_from(
        endpoint: server::Plan<'g, ID, R, Q>,
        app: Config,
    ) -> Result<Self, ConfigError> {
        Ok(Self {
            endpoint,
            config: Setup::try_new(app)?,
        })
    }

    /// Binds caller-selected observation, lifetime, deadline and task policies once.
    pub fn try_with<'pool, 'd, C, T, O, L, E, A>(
        self,
        router: T,
        observer: O,
        lifetime: L,
        deadline: E,
        tasks: A,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<
        Endpoint<
            'd,
            ID,
            Handler<'d, T::Runtime, O, L, E>,
            R::Buffer<'d>,
            Run<A, C>,
            Owned<Q>,
            datagram::Sealed<'pool>,
        >,
        startup::BindError<T::Error>,
    >
    where
        'g: 'd,
        T: Bind<'d, Target = Handle<'d, ID>>,
        O: Mode<'d, ID> + failure::Packet,
        L: rotation::Policy<'d>,
        E: deadline::Policy<'d>,
        R::Buffer<'d>: Retain<'d>,
        A: app::Driver<
                'd,
                Handler<'d, T::Runtime, O, L, E>,
                ID,
                R::Buffer<'d>,
                resident::Shared<'d>,
            >,
        C: app::Receipt<
                'd,
                Handler<'d, T::Runtime, O, L, E>,
                ID,
                R::Buffer<'d>,
                resident::Shared<'d>,
                A,
            >,
    {
        let Self { endpoint, config } = self;
        let seed = hash::Seed::new(driver.driver_ref().random_seed());
        endpoint.bind_with::<C, _, _, _, _>(driver, |binding| {
            Handler::try_bind(binding, router, config, observer, lifetime, deadline, seed)
                .map(|handler| (handler, tasks))
        })
    }
}

impl<'g, const ID: u8, R: receive::Policy, Q: routing::LocalRoute> AsRef<server::Plan<'g, ID, R, Q>>
    for Plan<'g, ID, R, Q>
{
    fn as_ref(&self) -> &server::Plan<'g, ID, R, Q> {
        &self.endpoint
    }
}

impl<const ID: u8, R: receive::Policy, Q: routing::LocalRoute> settings::Resource
    for Plan<'_, ID, R, Q>
{
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        settings::Resource::include_in(self.endpoint.as_ref(), needs)
    }
}
