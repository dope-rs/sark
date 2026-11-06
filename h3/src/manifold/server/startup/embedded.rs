//! Socket-free H3 startup ownership.
use std::marker;

use dope::core::driver::{
    self, settings,
    settings::{claim, errors},
};
use dope_quic::{
    conn::{id, life::options},
    errors::Connect,
    runtime::{
        embedded,
        mux::{app::Driver, handler, handler::net},
        receive,
    },
    rustls::ServerConfig,
    stream,
    wire::stream::Id,
};
use embedded::{plan, server};
use handler::hook::wire::packet;
use life::endpoint::Mode;
use o3::buffer::resident;
use sark_core::{life, life::session, manifold::task::app};
use sark_http::payload::compress::dynamic::hash;

use crate::manifold::owners;
use crate::manifold::server::{
    Config, ConfigError, Handle, Session, Setup, Tasks, deadline, startup::Local,
};

#[derive(Debug)]
pub enum PrepareError {
    Application(ConfigError),
    Transport(Connect),
}

#[derive(Debug)]
pub enum BindError<E> {
    Application(E),
    Transport(server::BindError),
}

impl<E> From<server::BindFailure<E>> for BindError<E> {
    fn from(error: server::BindFailure<E>) -> Self {
        match error {
            server::BindFailure::Application(error) => Self::Application(error),
            server::BindFailure::Endpoint(error) => Self::Transport(error),
        }
    }
}

type Bound<'d, const ID: u8, A, O, B, E> = Result<
    embedded::Endpoint<'d, ID, Local<'d, A, O, packet::Off>, B, embedded::Run<Tasks>>,
    BindError<E>,
>;

/// Validated H3 server authority for a socket-free QUIC component.
#[must_use = "an embedded H3 plan must be bound to its generated application"]
pub struct Embedded<const ID: u8, R: receive::Policy = receive::Retained> {
    quic: server::Plan<ID>,
    config: Setup,
    receive: marker::PhantomData<fn() -> R>,
}

impl<const ID: u8, R: receive::Policy> Embedded<ID, R> {
    pub fn try_new(
        tls: ServerConfig,
        conn: options::Options,
        plan: plan::Plan,
        app: Config,
    ) -> Result<Self, PrepareError> {
        let config = Setup::try_new(app).map_err(PrepareError::Application)?;
        let quic = server::Plan::new(tls, conn, plan).map_err(PrepareError::Transport)?;
        Ok(Self {
            quic,
            config,
            receive: marker::PhantomData,
        })
    }

    pub fn try_bind<'d, T, O>(
        self,
        router: T,
        mode: O,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Bound<'d, ID, T::Runtime, O, R::Buffer<'d>, T::Error>
    where
        T: app::Bind<'d, Target = Handle<'d, ID>>,
        O: life::Mode<id::Handle<id::Driver<'d>, ID>, Id>
            + Mode<id::Domain<id::Driver<'d>, ID>, net::Owner<id::Driver<'d>, ID>>,
        O::Retention: owners::Handoff,
        Tasks: Driver<
                'd,
                Local<'d, T::Runtime, O, packet::Off>,
                ID,
                R::Buffer<'d>,
                resident::Shared<'d>,
            >,
        R::Buffer<'d>: stream::Retain<'d>,
        Local<'d, T::Runtime, O, packet::Off>: handler::Handler<
                ID,
                R::Buffer<'d>,
                resident::Shared<'d>,
                id::Driver<'d>,
                Connection = Session<
                    'd,
                    T::Runtime,
                    session::Disabled,
                    deadline::Disabled,
                    O::Retention,
                >,
            >,
    {
        let quic = self.quic;
        let config = self.config;
        let seed = hash::Seed::new(driver.driver_ref().random_seed());
        quic.bind_with::<Local<'d, T::Runtime, O, packet::Off>, R::Buffer<'d>, Tasks, _, _>(
            driver,
            |binding| {
                Local::try_bind::<ID, _>(
                    binding,
                    router,
                    config,
                    mode,
                    session::Disabled,
                    deadline::Disabled,
                    seed,
                )
                .map(|handler| (handler, Tasks::new()))
            },
        )
        .map_err(Into::into)
    }
}

impl<const ID: u8, R: receive::Policy> settings::Resource for Embedded<ID, R> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        self.quic.include_in(needs)
    }
}
