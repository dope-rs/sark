use std::{convert::Infallible, net::AddrParseError};

use dope::{
    core::driver::{life::recovery, schedule::clock},
    manifold::client::attempt::queue::mailbox,
    runtime::executor::drive,
};
use dope_fiber::{BlockOn as _, task::time::sleep};
use o3::alloc;
use sark_client::{
    Failure,
    reply::{Contiguous, Scoped},
};
use sark_fiber::client::{self, config, dynamic::channel};
use sark_http::method::Method;

use crate::common::client::runtime::{self, WaitError, cases::Flavor, origins, pools};

#[derive(Debug)]
pub enum Error<F = runtime::Failure, D = runtime::Failure> {
    Host(alloc::Exhausted),
    Address(AddrParseError),
    FixedPlan(config::Error),
    DynamicPlan(channel::CreateError),
    Fixed(F),
    Dynamic(D),
    Step(Step),
}

#[derive(Debug)]
pub enum Step {
    Wait(WaitError),
    Drive(drive::Error),
    Clock(clock::Range),
    Timer(sleep::Error),
}

pub struct Request<'a> {
    flavor: Flavor,
    addr: &'a str,
    config: config::Config,
    method: Method,
    path: &'a str,
    headers: &'a [(&'a str, &'a str)],
    body: &'a [u8],
}

impl<'a> Request<'a> {
    pub fn new(
        flavor: Flavor,
        addr: &'a str,
        config: config::Config,
        method: Method,
        path: &'a str,
        headers: &'a [(&'a str, &'a str)],
        body: &'a [u8],
    ) -> Self {
        Self {
            flavor,
            addr,
            config,
            method,
            path,
            headers,
            body,
        }
    }

    pub fn try_run<R, C>(
        self,
        check: C,
        policy: impl recovery::Observe,
        done: impl for<'run> FnOnce(Result<Result<R, Step>, runtime::Error<'run>>) -> Result<R, Step>,
    ) -> Result<R, Error>
    where
        C: for<'d> FnOnce(Result<Contiguous, Failure<Infallible, mailbox::Error<'d>>>) -> R,
    {
        let Self {
            flavor,
            addr,
            config,
            method,
            path,
            headers,
            body,
        } = self;
        let output = match flavor {
            Flavor::Fixed => pools::Plan::new(addr.parse().map_err(Error::Address)?, config, 1, 1)
                .map_err(Error::FixedPlan)?
                .enter(
                    move |mut app| -> Result<_, Step> {
                        let client = app.handle;
                        app.wait().map_err(Step::Wait)?;
                        let outcome = app
                            .app
                            .block_on(client.collect_with(method, path, headers, body))
                            .map_err(Step::Drive)?
                            .map_err(Failure::widen);
                        Ok::<R, Step>(check(outcome))
                    },
                    policy,
                    done,
                )
                .map_err(Error::Fixed)?
                .map_err(Error::Step)?,
            Flavor::OnDemand => origins::Plan::new(config, 1, 1, 1)
                .map_err(Error::DynamicPlan)?
                .enter(
                    move |mut app| -> Result<_, Step> {
                        let client = app.handle;
                        let outcome = app
                            .app
                            .block_on(client.collect_with(method, path, headers, body))
                            .map_err(Step::Drive)?;
                        Ok::<R, Step>(check(outcome))
                    },
                    policy,
                    done,
                )
                .map_err(Error::Dynamic)?
                .map_err(Error::Step)?,
        };
        Ok(output)
    }
    pub fn try_scoped<R, C>(
        self,
        check: C,
        policy: impl recovery::Observe,
        done: impl for<'run> FnOnce(Result<Result<R, Step>, runtime::Error<'run>>) -> Result<R, Step>,
    ) -> Result<R, Error>
    where
        C: for<'scope, 'd> FnOnce(
            Result<Scoped<'scope, 'd>, Failure<Infallible, mailbox::Error<'d>>>,
        ) -> R,
    {
        let Self {
            flavor,
            addr,
            config,
            method,
            path,
            headers,
            body,
        } = self;
        let output = match flavor {
            Flavor::Fixed => pools::Plan::new(addr.parse().map_err(Error::Address)?, config, 1, 1)
                .map_err(Error::FixedPlan)?
                .enter(
                    move |mut app| -> Result<_, Step> {
                        let client = app.handle;
                        app.wait().map_err(Step::Wait)?;
                        let outcome = app
                            .app
                            .block_on(client::Request::send_with(
                                &client, method, path, headers, body,
                            ))
                            .map_err(Step::Drive)?
                            .map_err(Failure::widen);
                        Ok::<R, Step>(check(outcome))
                    },
                    policy,
                    done,
                )
                .map_err(Error::Fixed)?
                .map_err(Error::Step)?,
            Flavor::OnDemand => origins::Plan::new(config, 1, 1, 1)
                .map_err(Error::DynamicPlan)?
                .enter(
                    move |mut app| -> Result<_, Step> {
                        let client = app.handle;
                        let outcome = app
                            .app
                            .block_on(client::Request::send_with(
                                &client, method, path, headers, body,
                            ))
                            .map_err(Step::Drive)?;
                        Ok::<R, Step>(check(outcome))
                    },
                    policy,
                    done,
                )
                .map_err(Error::Dynamic)?
                .map_err(Error::Step)?,
        };
        Ok(output)
    }
}
