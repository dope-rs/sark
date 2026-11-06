use std::{convert::Infallible, net::SocketAddr, num::NonZero, time::Duration};

use dope::{
    core::driver::life::recovery, manifold::client::attempt::queue::mailbox,
    runtime::executor::drive,
};
use dope_fiber::{BlockOn as _, task::time::sleep};
use dope_test::alloc::Tracker;
use sark_client::{Failure, protocol::response};
use sark_fiber::client::config::Config;
use sark_http::method::Method;

use crate::common::client::runtime::{
    self,
    cases::{
        Flavor,
        request::{Error, Step},
    },
    origins, pools,
};

#[derive(Debug)]
pub enum Exchange<'d> {
    Response(Failure<Infallible, mailbox::Error<'d>>),
    Drive(drive::Error),
    Timer(sleep::Error),
    Head(Option<response::Event<'d>>),
    Allocation { allocs: usize, frees: usize },
}

pub enum Scenario<'a> {
    Interleave {
        first: &'a str,
        second: &'a str,
        gap: Duration,
    },
    Cancel {
        first: &'a str,
        method: Method,
        second: &'a str,
        body: &'a [u8],
    },
}

pub struct Stream<'a> {
    flavor: Flavor,
    addr: SocketAddr,
    config: Config,
    capacity: NonZero<usize>,
    scenario: Scenario<'a>,
}

impl<'a> Stream<'a> {
    pub fn new(
        flavor: Flavor,
        addr: SocketAddr,
        config: Config,
        capacity: NonZero<usize>,
        scenario: Scenario<'a>,
    ) -> Self {
        Self {
            flavor,
            addr,
            config,
            capacity,
            scenario,
        }
    }

    pub fn try_run<R, C>(
        self,
        check: C,
        policy: impl recovery::Observe,
        done: impl for<'run> FnOnce(Result<Result<R, Step>, runtime::Error<'run>>) -> Result<R, Step>,
    ) -> Result<R, Error>
    where
        C: for<'d> FnOnce(Result<u16, Exchange<'d>>) -> R,
    {
        let Self {
            flavor,
            addr,
            config,
            capacity,
            scenario,
        } = self;
        let output = match flavor {
            Flavor::Fixed => pools::Plan::new(addr, config, capacity.get(), 2).map_err(Error::FixedPlan)?
                .enter((), move |mut app| -> Result<_, Step> {
                    let client = app.handle;
                    app.wait().map_err(Step::Wait)?;
                    let outcome = (|| match scenario {
                        Scenario::Interleave { first, second, gap } => {
                            let mut first = app.app.block_on(client.stream_get(first)).map_err(Exchange::Drive)?.map_err(|error| Exchange::Response(error.widen()))?;
                            let event = app.app.block_on(first.next_event()).map_err(Exchange::Drive)?.ok_or(Exchange::Head(None))?.map_err(|error| Exchange::Response(error.widen()))?;
                            if !matches!(&event, response::Event::Head(_)) { return Err(Exchange::Head(Some(event))); }
                            if !gap.is_zero() {
                                let sleep = client.sleep(gap).map_err(Exchange::Timer)?;
                                app.app.block_on(sleep).map_err(Exchange::Drive)?;
                            }
                            let second = app.app.block_on(client.get(second)).map_err(Exchange::Drive)?.map_err(|error| Exchange::Response(error.widen()))?;
                            while let Some(event) = app.app.block_on(first.next_event()).map_err(Exchange::Drive)? {
                                event.map_err(|error| Exchange::Response(error.widen()))?;
                            }
                            Ok(second.status().as_u16())
                        }
                        Scenario::Cancel { first, method, second, body } => {
                            app.app.block_on(dope_fiber::fiber!('_ => async move {
                                let first = client.stream_get(first).await.map_err(|error| Exchange::Response(error.widen()))?;
                                let ((), [allocs, frees]) = Tracker::counts(|| drop(first));
                                if (allocs, frees) != (0, 0) {
                                    return Err(Exchange::Allocation { allocs, frees });
                                }
                                let second = client.send(method, second, body).await.map_err(|error| Exchange::Response(error.widen()))?;
                                Ok(second.status().as_u16())
                            })).map_err(Exchange::Drive)?
                        }
                    })();
                    Ok::<R, Step>(check(outcome))
                }, policy, done).map_err(Error::Fixed)?.map_err(Error::Step)?,
            Flavor::OnDemand => origins::Plan::new(config, capacity.get(), 2, 1).map_err(Error::DynamicPlan)?
                .enter((), move |mut app| -> Result<_, Step> {
                    let client = app.handle;
                    let outcome = (|| match scenario {
                        Scenario::Interleave { first, second, gap } => {
                            let mut first = app.app.block_on(client.stream_get(first)).map_err(Exchange::Drive)?.map_err(Exchange::Response)?;
                            let event = app.app.block_on(first.next_event()).map_err(Exchange::Drive)?.ok_or(Exchange::Head(None))?.map_err(|error| Exchange::Response(error.widen()))?;
                            if !matches!(&event, response::Event::Head(_)) { return Err(Exchange::Head(Some(event))); }
                            if !gap.is_zero() {
                                let sleep = client.sleep(gap).map_err(Exchange::Timer)?;
                                app.app.block_on(sleep).map_err(Exchange::Drive)?;
                            }
                            let second = app.app.block_on(client.get(second)).map_err(Exchange::Drive)?.map_err(Exchange::Response)?;
                            while let Some(event) = app.app.block_on(first.next_event()).map_err(Exchange::Drive)? {
                                event.map_err(|error| Exchange::Response(error.widen()))?;
                            }
                            Ok(second.status().as_u16())
                        }
                        Scenario::Cancel { first, method, second, body } => {
                            app.app.block_on(dope_fiber::fiber!('_ => async move {
                                let first = client.stream_get(first).await.map_err(Exchange::Response)?;
                                let ((), [allocs, frees]) = Tracker::counts(|| drop(first));
                                if (allocs, frees) != (0, 0) {
                                    return Err(Exchange::Allocation { allocs, frees });
                                }
                                let second = client.send(method, second, body).await.map_err(Exchange::Response)?;
                                Ok(second.status().as_u16())
                            })).map_err(Exchange::Drive)?
                        }
                    })();
                    Ok::<R, Step>(check(outcome))
                }, policy, done).map_err(Error::Dynamic)?.map_err(Error::Step)?,
        };
        Ok(output)
    }
}
