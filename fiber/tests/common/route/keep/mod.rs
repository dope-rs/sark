use std::{convert::Infallible, time::Duration};

use dope::{
    core::{
        BranchError,
        driver::{life::recovery, route::KeyTag, settings::errors::Claim},
        io::socket::Listener,
    },
    manifold::{self, network::listener::conn},
    runtime::{
        executor,
        topology::{self, error},
    },
};
use sark_core::life;
use sark_fiber::{
    Config,
    dope::net::{wire, wire::domain},
};
use sark_h1::server::{listener::bind, run, run::prepare};
use sark_http::payload::{fixed, response::status};

use crate::common::server;

pub static LARGE_STATIC_BODY: [u8; 32 * 1024] = [b's'; 32 * 1024];

#[sark_fiber::request]
pub struct BodyReq {
    #[raw_body]
    pub payload: owned::Segments,
}

#[sark_fiber::response(raw)]
pub struct Reply {
    pub status: status::Code,
    pub body: Vec<u8>,
}

#[sark_fiber::response(raw)]
pub struct StaticReply<'d> {
    pub status: status::Code,
    pub body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
fn ping() -> Reply {
    Reply {
        status: status::Code::OK,
        body: b"pong".to_vec(),
    }
}

#[sark_fiber::handler(pub)]
fn body(req: BodyReq) -> Reply {
    Reply {
        status: status::Code::OK,
        body: format!("body:{}", req.payload.len()).into_bytes(),
    }
}

#[sark_fiber::handler(pub)]
fn large() -> Reply {
    Reply {
        status: status::Code::OK,
        body: vec![b'x'; 32 * 1024],
    }
}

#[sark_fiber::handler(pub)]
fn large_static() -> StaticReply<'d> {
    StaticReply {
        status: status::Code::OK,
        body: fixed::Body::new(&LARGE_STATIC_BODY),
    }
}

sark_fiber::define_route! {
    pub KeepAliveDispatch: () => {
        GET "/ping" => ping,
        GET "/large" => large,
        GET "/large-static" => large_static,
        POST "/body" => body,
    }
}

pub struct Server<O> {
    plan: run::Plan<
        KeyTag<{ server::HTTP_LISTENER_ID }>,
        KeyTag<{ server::DATE_UPDATER_ID }>,
        sark_fiber::Throughput,
    >,
    observer: O,
}

impl<O> Server<O> {
    pub const fn new(
        plan: run::Plan<
            KeyTag<{ server::HTTP_LISTENER_ID }>,
            KeyTag<{ server::DATE_UPDATER_ID }>,
            sark_fiber::Throughput,
        >,
        observer: O,
    ) -> Self {
        Self { plan, observer }
    }
    pub fn try_run<C, R>(
        self,
        context: C,
        listener: Listener,
        policy: impl recovery::Observe,
        finish: impl for<'a> FnOnce(
            Result<(), error::Run<'a, Infallible, manifold::BuildError<bind::Error>>>,
        ) -> R,
    ) -> Result<R, BranchError<prepare::Error, Claim>>
    where
        C: executor::Build,
        for<'d> O: life::Observer<conn::Id<'d, KeyTag<{ server::HTTP_LISTENER_ID }>>, u64>,
    {
        use executor::session;

        let prepared = self.plan.with_bound(listener).prepare(())?;
        let observer = self.observer;
        let mut report = session::Completion::new();
        let complete = &mut report;
        let result = prepared.enter_with(
            context,
            move |scope| {
                scope.lend(|plan, stage| {
                    let app = KeepAliveDispatch::with_observer(
                        &(),
                        Config::new(server::MAX_CONNECTIONS),
                        observer,
                    );
                    plan.install(
                        stage,
                        domain::Local::<wire::Identity, _>::new(wire::Identity),
                        app,
                        |root, stage| {
                            let result = stage
                                .run_quiesce(complete, root, Duration::from_secs(2))
                                .map_err(topology::Failure::Run)?;
                            result.map_err(|error| {
                                topology::Failure::Run(session::Error::Drive(error))
                            })
                        },
                    )
                    .map_err(topology::Failure::Install)?
                })
            },
            policy,
            |result| {
                let result = match result {
                    Ok(result) => result.map_err(topology::RunError::Failure),
                    Err(error) => Err(topology::RunError::from(error)),
                };
                finish(result.as_ref().copied().map_err(topology::RunError::cause))
            },
        );
        Ok(result)
    }
}

pub mod events;
