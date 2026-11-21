use std::{cell::Cell, time::Duration};

use dope::{
    core::{
        BranchError,
        driver::{route::KeyTag, settings::errors::Claim},
        io::socket::Listener,
    },
    manifold::network::listener::conn,
    runtime::{executor::session, shutdown::ReadySource, topology},
};
use sark_core::life;
use sark_fiber::{
    Config,
    dope::net::{wire, wire::domain},
};
use sark_h1::server::run::prepare;
use sark_http::payload::{fixed, response::status};

use crate::common::{route::body::OwnedReply, server};

pub static LARGE_STATIC_BODY: [u8; 32 * 1024] = [b's'; 32 * 1024];

#[sark_fiber::request]
pub struct BodyReq {
    #[raw_body]
    pub payload: owned::Segments,
}

#[sark_fiber::response(raw)]
pub struct StaticReply<'d> {
    pub status: status::Code,
    pub body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
fn body(req: BodyReq) -> OwnedReply {
    OwnedReply {
        status: status::Code::OK,
        body: format!("body:{}", req.payload.len()).into_bytes(),
    }
}

#[sark_fiber::handler(pub)]
fn large() -> OwnedReply {
    OwnedReply {
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
        GET "/ping" => crate::common::route::body::ping,
        GET "/large" => large,
        GET "/large-static" => large_static,
        POST "/body" => body,
    }
}

pub fn record(client: impl FnOnce(std::net::SocketAddr)) -> Vec<&'static str> {
    let (pair, plan) = server::Plan::isolated();
    let mut events = Vec::new();
    pair.run(
        |context, listener| {
            let audits = Cell::new(None);
            try_run(
                plan,
                context,
                listener,
                events::Log(Cell::from_mut(&mut events), &audits, events::diagnose),
                &audits,
            )
        },
        client,
    )
    .expect("observed H1 server");
    events
}

pub fn try_run(
    plan: server::Http1Plan,
    context: ReadySource<'_>,
    listener: Listener,
    observer: impl for<'d> life::Observer<conn::Id<'d, KeyTag<{ server::HTTP_LISTENER_ID }>>, u64>,
    audits: &Cell<Option<life::audit::Snapshot>>,
) -> Result<(), BranchError<prepare::Error, Claim>> {
    let mut reports = server::Plan::try_reports(&plan).map_err(BranchError::Second)?;
    let prepared = plan.with_bound(listener).prepare(())?;
    let mut complete = session::Completion::new();
    prepared.enter_with(
        context,
        |scope| {
            scope.lend(|plan, stage| {
                let app = KeepAliveDispatch::with_observer(
                    &(),
                    Config::new(server::MAX_CONNECTIONS),
                    observer,
                );
                plan.install(
                    stage,
                    domain::Local::new(wire::Identity),
                    app,
                    |root, _wait, stage| {
                        let result = stage
                            .run_quiesce(&mut complete, root, Duration::from_secs(2))
                            .map_err(topology::Failure::Run)?;
                        result.map_err(|error| topology::Failure::Run(session::Error::Drive(error)))
                    },
                )
                .map_err(topology::Failure::Install)?
            })
        },
        server::reporting(&mut reports),
        |result| {
            let result = match result {
                Ok(result) => result.map_err(topology::RunError::Failure),
                Err(error) => Err(topology::RunError::from(error)),
            };
            result
                .as_ref()
                .map_err(topology::RunError::cause)
                .expect("observed server");
        },
    );
    assert_eq!(audits.get(), None);
    server::reclaimed(&reports);
    Ok(())
}

pub mod events;
