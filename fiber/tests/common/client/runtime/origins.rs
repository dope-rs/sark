use std::time;

use dope::{
    core::driver::{life::recovery, schedule::clock},
    manifold::{app::Host, timing},
    runtime::{executor::session, topology, topology::lending},
};
use dope_test::harness::GUARD;
use dynamic::channel;
use sark_fiber::client::{config, dynamic};

use crate::common::client::runtime::{Error, Failure};

type Handle<'app, 's, 'p, 'o, 'q, 'e, 'b, 'c, 'd> = dynamic::Handle<
    'app,
    'o,
    'q,
    'e,
    'b,
    'c,
    'd,
    dynamic::Connector<'s, 'p, 'o, 'q, 'e, 'b, 'c, 'd>,
>;

pub struct Plan {
    prepared: topology::Prepared<(), channel::Plan>,
}

impl Plan {
    pub fn new(
        config: config::Config,
        capacity: usize,
        timers: usize,
        origins: usize,
    ) -> Result<Self, channel::CreateError> {
        let plan = channel::Plan::new(config, capacity, timers, origins)?;
        let prepared = topology::Plan::new((), plan).for_install::<timing::Balanced>()?;
        Ok(Self { prepared })
    }

    pub fn enter<R, Out>(
        self,
        run: impl for<'app, 'life, 's, 'p, 'o, 'q, 'e, 'b, 'c, 'd> FnOnce(
            Scope<'app, 'life, 's, 'p, 'o, 'q, 'e, 'b, 'c, 'd>,
        ) -> R,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<R, Error<'a>>) -> Out,
    ) -> Result<Out, Failure> {
        let mut finish = session::Completion::new();
        let finish = &mut finish;
        Ok(self.prepared.with_local(
            move |worker| {
                worker.lend(|http, stage| {
                    http.try_with(stage, lending::Stage::driver, |port, stage| {
                        port.try_with(stage, lending::Stage::driver, |client, stage| {
                            Ok::<_, Failure>(stage.with_quiesce(
                                finish,
                                Host::new(client),
                                GUARD,
                                |mut app| {
                                    let handle = app.root_client();
                                    run(Scope { app, handle })
                                },
                            ))
                        })
                        .map_err(Failure::Connector)?
                    })
                    .map_err(Failure::DynamicPort)?
                })
            },
            policy,
            |result| super::finish(result, done),
        ))
    }

    pub fn enter_at<R, Out>(
        self,
        offset: time::Duration,
        run: impl for<'app, 'life, 's, 'p, 'o, 'q, 'e, 'b, 'c, 'd> FnOnce(
            Clock<'app, 'life, 's, 'p, 'o, 'q, 'e, 'b, 'c, 'd>,
        ) -> R,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<R, Error<'a>>) -> Out,
    ) -> Result<Out, Failure> {
        let mut finish = session::Completion::new();
        let finish = &mut finish;
        Ok(self.prepared.with_local(
            move |worker| {
                worker.lend(|http, stage| {
                    http.try_with(stage, lending::Stage::driver, |port, stage| {
                        port.try_with(stage, lending::Stage::driver, |client, stage| {
                            let start = stage
                                .driver()
                                .driver_ref()
                                .clock()
                                .now()
                                .try_add(offset)
                                .map_err(Failure::Clock)?;
                            Ok::<_, Failure>(stage.with_at_for(
                                finish,
                                Host::new(client),
                                start,
                                GUARD,
                                |mut app| {
                                    let handle = app.root_client();
                                    run(Clock { app, handle, start })
                                },
                            ))
                        })
                        .map_err(Failure::Connector)?
                    })
                    .map_err(Failure::DynamicPort)?
                })
            },
            policy,
            |result| super::finish(result, done),
        ))
    }
}

/// Live on-demand HTTP client bound to one application lifetime.
pub struct Scope<'app, 'life: 'app, 's, 'p, 'o, 'q, 'e, 'b, 'c, 'd: 'life> {
    pub app:
        session::App<'app, 'life, 'd, Host<dynamic::Connector<'s, 'p, 'o, 'q, 'e, 'b, 'c, 'd>>>,
    pub handle: Handle<'app, 's, 'p, 'o, 'q, 'e, 'b, 'c, 'd>,
}

/// Logical-clock on-demand client bound to one application lifetime.
pub struct Clock<'app, 'life: 'app, 's, 'p, 'o, 'q, 'e, 'b, 'c, 'd: 'life> {
    pub app:
        session::Manual<'app, 'life, 'd, Host<dynamic::Connector<'s, 'p, 'o, 'q, 'e, 'b, 'c, 'd>>>,
    pub handle: Handle<'app, 's, 'p, 'o, 'q, 'e, 'b, 'c, 'd>,
    pub start: clock::Deadline<'d>,
}
