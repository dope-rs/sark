use std::{convert::Infallible, io, net, time::Duration};

use dope::{
    core::{
        driver::{
            life::recovery,
            route::{FrameworkTag, KeyTag, Tag, kind},
            settings::errors,
        },
        io::socket::Listener,
    },
    manifold,
    runtime::{
        executor::{self, session::Completion},
        topology::{self, error, lending},
    },
};
use dope_test::reclaim;
use sark_h2::{
    conn::config,
    server::{Server, runtime, service::Handler},
};

pub trait Serve: Handler + Sized {
    fn try_serve<C: executor::Build>(
        self,
        plan: runtime::Plan<KeyTag<0>>,
        context: C,
        listener: Listener,
        recover: impl recovery::Recover,
        done: impl for<'a> FnOnce(
            Result<(), error::Run<'a, Infallible, manifold::BuildError<config::Error>>>,
        ),
    ) -> Result<reclaim::Reports<1>, errors::Claim> {
        let prepared = Server::from_handler(self, plan).prepare_on(listener)?;
        let mut reports = reclaim::Reports::<1>::new(
            prepared.layout(),
            [<FrameworkTag<kind::Any> as Tag>::ROUTE],
        );
        let mut finish = Completion::new();
        prepared.enter_with(
            context,
            |worker| {
                worker
                    .with(lending::Run(&mut finish))
                    .map_err(topology::Failure::Install)?
                    .map_err(topology::Failure::Run)
            },
            recovery::Policy::new(|report| reports.push(report), recover),
            |result| {
                let result = match result {
                    Ok(result) => result.map_err(topology::RunError::Failure),
                    Err(error) => Err(topology::RunError::from(error)),
                };
                done(
                    result
                        .as_ref()
                        .map(|_| ())
                        .map_err(topology::RunError::cause),
                )
            },
        );
        Ok(reports)
    }
}

impl<H: Handler> Serve for H {}

pub trait Connect {
    fn try_connect(self, timeout: Duration) -> io::Result<net::TcpStream>;
}

impl Connect for net::SocketAddr {
    fn try_connect(self, timeout: Duration) -> io::Result<net::TcpStream> {
        let socket = net::TcpStream::connect(self)?;
        socket.set_nodelay(true)?;
        socket.set_read_timeout(Some(timeout))?;
        Ok(socket)
    }
}
