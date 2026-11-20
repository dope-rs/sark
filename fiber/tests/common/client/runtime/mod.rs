use std::convert::Infallible;

use dope::{
    core::driver::{schedule::clock, settings::errors},
    manifold::app::creation,
    runtime::{executor::session, topology},
};
use dope_test::fibers;
use sark_fiber::client::{config, dynamic::channel};

#[derive(Debug)]
pub enum Failure {
    Plan(config::Error),
    Claim(errors::Claim),
    Clock(clock::Range),
    Port(creation::Error),
    DynamicPort(channel::CreateError),
    Connector(creation::Error),
}

impl From<Infallible> for Failure {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

type Output<'report, T> = Result<session::Result<'report, T>, Failure>;

fn finish<'report, I, R, Q>(
    result: Result<
        Output<'report, R>,
        topology::EnterError<'_, Infallible, I, Output<'report, R>, Q>,
    >,
) -> R {
    let session = match result {
        Ok(output) => Ok(output),
        Err(ref error) => Err(error.kind().map(|output| {
            output
                .as_ref()
                .map(|session| session.as_ref().map(|_| ()).map_err(session::Error::cause))
        })),
    }
    .expect("client topology")
    .expect("client setup");
    match session {
        Ok(value) => Ok(value),
        Err(ref error) => Err(error.cause()),
    }
    .expect("client session")
}

pub mod calls;
pub mod origins;

pub mod pools;

pub mod peer;

pub mod tls;

/// Failure while waiting for an HTTP endpoint.
#[derive(Debug)]
pub enum WaitError {
    Drive(fibers::Run),
    Connector(sark_client::Error),
}

pub mod cases;

pub mod sockets;

pub mod server;
