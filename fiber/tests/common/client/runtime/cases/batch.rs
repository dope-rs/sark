use std::{fmt, net::SocketAddr, num::NonZero, time::Duration};

use dope::{
    core::driver::schedule::clock,
    runtime::executor::{self, session},
};
use dope_fiber::{BlockOn, task::done};
use sark_fiber::client::{Ready, Request, config::Config};
use sark_http::method::Method;

use crate::common::client::{
    assert_shutdown,
    runtime::{
        WaitError,
        cases::{
            Flavor,
            request::{Error, Step},
        },
        origins, pools,
    },
};

fn get<'app, 'd: 'app>(
    app: &mut impl BlockOn<'d>,
    client: &impl Request<'app, 'd, Error: fmt::Debug>,
    paths: &[&str],
    codes: &mut Vec<u16>,
) -> Result<(), Step> {
    for path in paths {
        let reply = app
            .block_on(client.send_with(Method::GET, path, &[], &[]))
            .map_err(Step::Drive)?
            .expect("batch response");
        codes.push(reply.status().as_u16());
    }
    Ok(())
}

fn timed_pair<'app, 'd: 'app, D: executor::raw::App<'d>>(
    app: &mut session::Manual<'_, '_, 'd, D>,
    client: &impl Request<'app, 'd, Error: fmt::Debug>,
    start: clock::Deadline<'d>,
    gap: Duration,
) -> Result<[u16; 2], Step> {
    let next = start.try_add(gap).map_err(Step::Clock)?;
    let mut codes = [0; 2];
    for (code, (path, now)) in codes.iter_mut().zip([("/first", start), ("/second", next)]) {
        let reply = app
            .drive(
                now,
                done::Once::new(client.send_with(Method::GET, path, &[], &[])),
            )
            .map_err(Step::Drive)?
            .into_output()
            .expect("batch response");
        *code = reply.status().as_u16();
    }
    Ok(codes)
}

impl Flavor {
    pub fn try_gets(
        self,
        addr: SocketAddr,
        config: Config,
        capacity: NonZero<usize>,
        first: &[&str],
        second: &[&str],
        gap: Option<Duration>,
    ) -> Result<Vec<u16>, Error> {
        match self {
            Self::Fixed => pools::Plan::new(addr, config, capacity.get(), 1)
                .map_err(Error::FixedPlan)?
                .enter(
                    (),
                    |mut app| {
                        app.wait().map_err(Step::Wait)?;
                        let mut codes = Vec::with_capacity(first.len() + second.len());
                        get(&mut app.app, &app.handle, first, &mut codes)?;
                        if let Some(gap) = gap {
                            let sleep = app.handle.sleep(gap).map_err(Step::Timer)?;
                            app.app.block_on(sleep).map_err(Step::Drive)?;
                        }
                        get(&mut app.app, &app.handle, second, &mut codes)?;
                        Ok(codes)
                    },
                    assert_shutdown,
                )
                .map_err(Error::Fixed)?,
            Self::OnDemand => origins::Plan::new(config, 1, 1, 1)
                .map_err(Error::DynamicPlan)?
                .enter(
                    (),
                    |mut app| {
                        let mut codes = Vec::with_capacity(first.len() + second.len());
                        get(&mut app.app, &app.handle, first, &mut codes)?;
                        if let Some(gap) = gap {
                            let sleep = app.handle.sleep(gap).map_err(Step::Timer)?;
                            app.app.block_on(sleep).map_err(Step::Drive)?;
                        }
                        get(&mut app.app, &app.handle, second, &mut codes)?;
                        Ok(codes)
                    },
                    assert_shutdown,
                )
                .map_err(Error::Dynamic)?,
        }
        .map_err(Error::Step)
    }

    pub fn try_timed_pair(
        self,
        addr: SocketAddr,
        config: Config,
        gap: Duration,
    ) -> Result<[u16; 2], Error> {
        let offset = Duration::from_millis(1);
        match self {
            Self::Fixed => pools::Plan::new(addr, config, 1, 1)
                .map_err(Error::FixedPlan)?
                .enter_at(
                    offset,
                    |mut app| {
                        app.app
                            .drive(app.start, done::Once::new(app.handle.wait_active()))
                            .map_err(Step::Drive)?
                            .into_output()
                            .map_err(|error| Step::Wait(WaitError::Connector(error)))?;
                        timed_pair(&mut app.app, &app.handle, app.start, gap)
                    },
                    assert_shutdown,
                )
                .map_err(Error::Fixed)?,
            Self::OnDemand => origins::Plan::new(config, 1, 1, 1)
                .map_err(Error::DynamicPlan)?
                .enter_at(
                    offset,
                    |mut app| timed_pair(&mut app.app, &app.handle, app.start, gap),
                    assert_shutdown,
                )
                .map_err(Error::Dynamic)?,
        }
        .map_err(Error::Step)
    }
}
