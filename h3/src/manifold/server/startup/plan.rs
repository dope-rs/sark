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
        receive,
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

type Bound<'pool, 'd, const ID: u8, R, T, O, L, E, A, C> = Result<
    Endpoint<
        'd,
        ID,
        Handler<'d, <T as Bind<'d>>::Runtime, O, L, E>,
        <R as receive::Policy>::Buffer<'d>,
        Run<A, C>,
        Owned,
        datagram::Sealed<'pool>,
    >,
    startup::BindError<<T as Bind<'d>>::Error>,
>;

/// Validated H3 and QUIC startup authority for one driver route.
#[must_use = "an H3 plan must be bound to its generated application"]
pub struct Plan<const ID: u8, R: receive::Policy = receive::Retained> {
    endpoint: server::Plan<ID, R>,
    config: Setup,
}

impl<const ID: u8> Plan<ID> {
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

impl<const ID: u8, R: receive::Policy> Plan<ID, R> {
    /// Adds validated H3 limits to an existing QUIC plan without rebuilding its authority.
    pub fn try_from(endpoint: server::Plan<ID, R>, app: Config) -> Result<Self, ConfigError> {
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
    ) -> Bound<'pool, 'd, ID, R, T, O, L, E, A, C>
    where
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
        endpoint
            .bind_with::<C, _, _, _, _>(driver, |binding| {
                Handler::try_bind::<ID, _>(
                    binding, router, config, observer, lifetime, deadline, seed,
                )
                .map(|handler| (handler, tasks))
                .map_err(startup::BindError::Application)
            })
            .map_err(Into::into)
    }
}

impl<const ID: u8, R: receive::Policy> AsRef<server::Plan<ID, R>> for Plan<ID, R> {
    fn as_ref(&self) -> &server::Plan<ID, R> {
        &self.endpoint
    }
}

impl<const ID: u8, R: receive::Policy> settings::Resource for Plan<ID, R> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        settings::Resource::include_in(self.endpoint.as_ref(), needs)
    }
}
