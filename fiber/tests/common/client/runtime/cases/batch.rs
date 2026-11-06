use std::{convert::Infallible, net::SocketAddr, num::NonZero, time::Duration};

use dope::{core::driver::life::recovery, manifold::client::attempt::queue::mailbox};
use dope_fiber::{BlockOn as _, task::done};
use sark_client::{Failure, reply::Scoped};
use sark_fiber::client::{Ready, Request, config::Config};
use sark_http::method::Method;

use crate::common::client::runtime::{
    self, WaitError,
    cases::{
        Flavor,
        request::{Error, Step},
    },
    origins, pools,
};

pub struct Batch<'a> {
    flavor: Flavor,
    addr: SocketAddr,
    config: Config,
    capacity: NonZero<usize>,
    first: &'a [&'a str],
    second: &'a [&'a str],
    gap: Option<Duration>,
}

impl<'request> Batch<'request> {
    pub fn new(
        flavor: Flavor,
        addr: SocketAddr,
        config: Config,
        capacity: NonZero<usize>,
        first: &'request [&'request str],
        second: &'request [&'request str],
        gap: Option<Duration>,
    ) -> Self {
        Self {
            flavor,
            addr,
            config,
            capacity,
            first,
            second,
            gap,
        }
    }

    pub fn try_run<C>(
        self,
        mut check: C,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(
            Result<Result<Vec<u16>, Step>, runtime::Error<'a>>,
        ) -> Result<Vec<u16>, Step>,
    ) -> Result<Vec<u16>, Error>
    where
        C: for<'scope, 'd> FnMut(
            Result<Scoped<'scope, 'd>, Failure<Infallible, mailbox::Error<'d>>>,
        ) -> u16,
    {
        let Self {
            flavor,
            addr,
            config,
            capacity,
            first,
            second,
            gap,
        } = self;
        let codes = match flavor {
            Flavor::Fixed => pools::Plan::new(addr, config, capacity.get(), 1)
                .map_err(Error::FixedPlan)?
                .enter(
                    (),
                    move |mut app| -> Result<_, Step> {
                        let client = app.handle;
                        app.wait().map_err(Step::Wait)?;
                        let mut codes = Vec::with_capacity(first.len() + second.len());
                        for (paths, pause) in [(first, None), (second, gap)] {
                            if let Some(pause) = pause {
                                let sleep = client.sleep(pause).map_err(Step::Timer)?;
                                app.app.block_on(sleep).map_err(Step::Drive)?;
                            }
                            for path in paths {
                                codes.push(check(
                                    app.app
                                        .block_on(client.get(path))
                                        .map_err(Step::Drive)?
                                        .map_err(Failure::widen),
                                ));
                            }
                        }
                        Ok::<_, Step>(codes)
                    },
                    policy,
                    done,
                )
                .map_err(Error::Fixed)?
                .map_err(Error::Step)?,
            Flavor::OnDemand => origins::Plan::new(config, 1, 1, 1)
                .map_err(Error::DynamicPlan)?
                .enter(
                    (),
                    move |mut app| -> Result<_, Step> {
                        let client = app.handle;
                        let mut codes = Vec::with_capacity(first.len() + second.len());
                        for (paths, pause) in [(first, None), (second, gap)] {
                            if let Some(pause) = pause {
                                let sleep = client.sleep(pause).map_err(Step::Timer)?;
                                app.app.block_on(sleep).map_err(Step::Drive)?;
                            }
                            for path in paths {
                                codes.push(check(
                                    app.app
                                        .block_on(client.send_with(Method::GET, path, &[], &[]))
                                        .map_err(Step::Drive)?,
                                ));
                            }
                        }
                        Ok::<_, Step>(codes)
                    },
                    policy,
                    done,
                )
                .map_err(Error::Dynamic)?
                .map_err(Error::Step)?,
        };
        Ok(codes)
    }

    pub fn try_clock<C>(
        self,
        mut check: C,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(
            Result<Result<Vec<u16>, Step>, runtime::Error<'a>>,
        ) -> Result<Vec<u16>, Step>,
    ) -> Result<Vec<u16>, Error>
    where
        C: for<'scope, 'd> FnMut(
            Result<Scoped<'scope, 'd>, Failure<Infallible, mailbox::Error<'d>>>,
        ) -> u16,
    {
        let Self {
            flavor,
            addr,
            config,
            capacity,
            first,
            second,
            gap,
        } = self;
        let codes = match flavor {
            Flavor::Fixed => pools::Plan::new(addr, config, capacity.get(), 1)
                .map_err(Error::FixedPlan)?
                .enter_at(
                    Duration::from_millis(1),
                    move |mut app| -> Result<_, Step> {
                        let client = app.handle;
                        let start = app.start;
                        app.app
                            .drive(start, done::Once::new(client.wait_active()))
                            .map_err(Step::Drive)?
                            .into_output()
                            .map_err(|error| Step::Wait(WaitError::Connector(error)))?;
                        let next = start
                            .try_add(gap.unwrap_or_default())
                            .map_err(Step::Clock)?;
                        let mut codes = Vec::with_capacity(first.len() + second.len());
                        for (paths, now) in [(first, start), (second, next)] {
                            for path in paths {
                                codes.push(check(
                                    app.app
                                        .drive(now, done::Once::new(client.get(path)))
                                        .map_err(Step::Drive)?
                                        .into_output()
                                        .map_err(Failure::widen),
                                ));
                            }
                        }
                        Ok::<_, Step>(codes)
                    },
                    policy,
                    done,
                )
                .map_err(Error::Fixed)?
                .map_err(Error::Step)?,
            Flavor::OnDemand => origins::Plan::new(config, 1, 1, 1)
                .map_err(Error::DynamicPlan)?
                .enter_at(
                    Duration::from_millis(1),
                    move |mut app| -> Result<_, Step> {
                        let client = app.handle;
                        let start = app.start;
                        let next = start
                            .try_add(gap.unwrap_or_default())
                            .map_err(Step::Clock)?;
                        let mut codes = Vec::with_capacity(first.len() + second.len());
                        for (paths, now) in [(first, start), (second, next)] {
                            for path in paths {
                                codes.push(check(
                                    app.app
                                        .drive(now, done::Once::new(client.get(path)))
                                        .map_err(Step::Drive)?
                                        .into_output(),
                                ));
                            }
                        }
                        Ok::<_, Step>(codes)
                    },
                    policy,
                    done,
                )
                .map_err(Error::Dynamic)?
                .map_err(Error::Step)?,
        };
        Ok(codes)
    }
}
