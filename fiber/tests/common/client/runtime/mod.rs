use std::convert::Infallible;

use dope::{
    core::driver::{schedule::clock, settings::errors},
    manifold::app::creation,
    runtime::{executor::session, topology, topology::error},
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

#[derive(Debug)]
pub enum Error<'a> {
    Setup(&'a Failure),
    Session(session::Cause<'a>),
    Topology(error::Kind<'a, Infallible, Result<Result<(), session::Cause<'a>>, &'a Failure>>),
}

type Output<'report, T> = Result<session::Result<'report, T>, Failure>;

pub fn finish<'report, I, R, Out, Q>(
    result: Result<
        Output<'report, R>,
        topology::EnterError<'_, Infallible, I, Output<'report, R>, Q>,
    >,
    done: impl for<'a> FnOnce(Result<R, Error<'a>>) -> Out,
) -> Out {
    match result {
        Ok(Ok(Ok(value))) => done(Ok(value)),
        Ok(Ok(Err(error))) => done(Err(Error::Session(error.cause()))),
        Ok(Err(error)) => done(Err(Error::Setup(&error))),
        Err(error) => done(Err(Error::Topology(error.kind().map(|output| {
            output
                .as_ref()
                .map(|session| session.as_ref().map(|_| ()).map_err(session::Error::cause))
        })))),
    }
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
