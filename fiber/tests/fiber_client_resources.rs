use std::convert;

use dope::core::driver::life::recovery;
pub mod common;

use std::{mem, net::SocketAddr, time};

use common::client::recover;
use common::client::runtime::{origins, pools};
use dope::{
    core::{
        driver,
        driver::{
            life::creation::Attempt, route, route::KeyTag, settings, settings::Resource as _,
            storage::Exhausted,
        },
        io::Reclaimed,
    },
    manifold::{app::creation, timing::Balanced},
    net::link::egress,
    runtime::executor::{Executor, session},
};
use dope_test::alloc::Tracker;
use driver::settings::claim::Needs;
use o3::buffer::pool::LayoutError;
use sark_fiber::{
    client,
    client::{
        Host, channel,
        channel::Topology,
        config,
        config::{Config, Error},
        dynamic,
        dynamic::channel::{CreateError, Plan},
    },
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn retains_provider_state() {
    use std::{
        cell::Cell,
        io::{Read, Write},
        net::TcpStream,
    };

    use cartel_redis::client::{plan, settings, settings::init};
    use common::{
        route::glue::{
            task,
            task::generics::{self, App, State},
        },
        server,
    };
    use dope::{
        core::{BranchError, driver::storage::With},
        manifold::{BuildError, app::env::Bundle, service},
        net::{
            tcp::Tcp,
            wire::{Identity, domain},
        },
        runtime::topology::ops,
    };
    use dope_fiber::task::storage::scope::Brand;
    use dope_test::harness::server::Pair;
    use sark_fiber::{Config, Throughput, server::PrepareError};
    use sark_h1::server::{date::Clock, run};

    let harness = Pair::bind().expect("bind dependent listener");
    let plan = server::Plan::new(harness.addr(), time::Duration::from_secs(5)).build();
    harness
        .check(
            |context, listener| {
                let first = String::from("provider response");
                let encoded = Cell::new(0);
                let bytes = first.as_bytes();
                let observed = &encoded;
                let assembly = plan::Plan::<2>::new(
                    settings::Config::new(generics::REDIS).expect("Redis limits"),
                )
                .assemble::<Bundle<Tcp, Identity, Throughput>>(
                    service::Snapshot::empty(service::Revision::new(1)),
                    settings::DEFAULT_BACKOFF,
                    settings::DEFAULT_BACKOFF_MAXIMUM,
                    init::Plan::new(),
                )
                .expect("Redis provider plan");
                let (plan, config, recovery, finish) = (
                    plan.with_bound(listener),
                    Config::new(server::MAX_CONNECTIONS),
                    recovery::Policy::new(
                        drop,
                        |failure: recovery::Recovery<'_, '_>| -> convert::Infallible {
                            panic!("native server failure: {:?}", failure.cause())
                        },
                    ),
                    |result: Result<_, run::RunError<'_, _, _, _, _, _>>| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("server session")
                    },
                );
                let clock = Clock::new().map_err(PrepareError::Date)?;
                let (storage, provider) = assembly.into_parts();
                let storage = With::new(storage, clock);
                let mut completion = session::Completion::new();
                let report = &mut completion;
                let profile = plan.config();
                let plan = provider.compose(storage, plan, move |plan, owner, stage| {
                    stage.hold(owner, |held, stage| {
                        Brand::with(|brand| {
                            plan.prepare_in(
                                stage,
                                domain::Local::new(Identity),
                                |prepared, wait, stage| {
                                    let result = held.lend_scope(stage, report, |issued| {
                                        let (client, permit) = issued.into_parts();
                                        let state = std::pin::pin!(State::<_, 2> {
                                            source: (
                                                client,
                                                task::State {
                                                    first: bytes,
                                                    second: b"",
                                                    encoded: observed,
                                                }
                                            ),
                                        });
                                        App::run_prepared(
                                            brand,
                                            state.as_ref(),
                                            config,
                                            prepared,
                                            wait,
                                            permit,
                                        )
                                    });
                                    run::Failure::lending(result)
                                },
                            )
                            .map_err(|error| run::Failure::Install(BuildError::Create(error)))?
                        })
                    })
                });
                let prepared = plan.with_config(profile).map_err(PrepareError::Layout)?;
                prepared.enter(
                    context,
                    ops::Map(ops::Compose, |result| match result {
                        Ok(Ok(request)) => Ok(request),
                        Ok(Err(error)) => {
                            Err(run::Failure::map_install(error, BranchError::Second))
                        }
                        Err(error) => Err(run::Failure::Install(BranchError::First(error))),
                    }),
                    recovery,
                    |outcome| {
                        let result = match outcome {
                            Ok(result) => result.map_err(run::RunError::Failure),
                            Err(error) => Err(run::RunError::from(error)),
                        };
                        finish(result)
                    },
                );
                assert_eq!(encoded.get(), 1);
                Ok::<_, PrepareError>(())
            },
            |address| {
                let mut socket = TcpStream::connect(address).expect("connect dependent client");
                socket
                    .set_read_timeout(Some(time::Duration::from_secs(3)))
                    .expect("set dependent response deadline");
                socket
                    .write_all(b"GET / HTTP/1.1\r\nHost: task\r\nConnection: close\r\n\r\n")
                    .expect("write dependent request");
                let mut response = Vec::new();
                socket
                    .read_to_end(&mut response)
                    .expect("read dependent response");
                assert!(response.ends_with(b"provider response"));
            },
            |result| assert!(result.is_ok(), "dependent preparation: {:?}", result.err()),
        )
        .expect("dependent server harness");
}

#[test]
fn handles_are_one_pointer() {
    assert_eq!(
        mem::size_of::<
            client::request::Handle<
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                'static,
                (),
            >,
        >(),
        mem::size_of::<&'static ()>()
    );
    assert_eq!(
        mem::size_of::<
            dynamic::Handle<'static, 'static, 'static, 'static, 'static, 'static, 'static, ()>,
        >(),
        mem::size_of::<&'static ()>()
    );
}

#[test]
fn http_storage_layout() {
    let plan = channel::Plan::<KeyTag<0>>::new(
        config::Config::try_new("127.0.0.1").expect("HTTP host allocation"),
        Topology::new(1, 1),
    );
    let plan = plan.expect("valid HTTP port plan");
    let layout = Needs::from_tree(&plan).and_then(|needs| needs.resolve::<Balanced>());
    let layout = layout.expect("HTTP resource layout");
    let resident = config::DEFAULT_REQUEST_SLOTS
        .checked_mul(config::DEFAULT_REQUEST_CAPACITY)
        .expect("default resident capacity fits");
    let expected = egress::Config::shared(egress::Credit::new(
        egress::Entries::new(config::DEFAULT_REQUEST_SLOTS),
        egress::Resident::new(resident),
    ));
    let mut attempt = Attempt::new();
    let executor = Executor::new(&mut attempt, layout);
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    let executor = executor.ok().expect("checked executor creation");
    executor.run(|executor| {
        let result = executor.enter_with(
            |mut scope| {
                plan.try_with(&mut scope, session::Scope::driver, |port, _| {
                    assert!((port.egress()) == (expected))
                })
            },
            recovery::Policy::new(
                |event| std::assert_matches!(event, Reclaimed::Shutdown),
                recover,
            ),
        );
        assert!(matches!(result, Ok(Ok(()))), "HTTP port allocation");
    });
}

#[test]
fn http_requires_resident() {
    let plan = channel::Plan::<KeyTag<0>>::new(
        config::Config::try_new("127.0.0.1").expect("HTTP host allocation"),
        Topology::new(1, 1),
    );
    let plan = plan.expect("test operation must succeed");
    let mut attempt = Attempt::new();
    let executor = Executor::new(&mut attempt, settings::Config::for_profile::<Balanced>());
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    let executor = executor.ok().expect("checked executor creation");
    executor.run(|executor| {
        let fixed = executor.enter_with(
            |mut scope| plan.try_with(&mut scope, session::Scope::driver, |_, _| ()),
            recovery::Policy::new(
                |event| std::assert_matches!(event, Reclaimed::Shutdown),
                recover,
            ),
        );
        assert!(matches!(
            fixed,
            Ok(Err(creation::Error::Resident(Exhausted::Domains {
                capacity: 0
            })))
        ));
    });

    let plan = Plan::<KeyTag<0>>::new(
        config::Config::try_new("127.0.0.1").expect("HTTP host allocation"),
        1,
        1,
        1,
    );
    let plan = plan.expect("test operation must succeed");
    let mut attempt = Attempt::new();
    let executor = Executor::new(&mut attempt, settings::Config::for_profile::<Balanced>());
    executor
        .as_ref()
        .map_err(|error| error.kind())
        .expect("executor creation");
    let executor = executor.ok().expect("checked executor creation");
    executor.run(|executor| {
        let open = executor.enter_with(
            |mut scope| plan.try_with(&mut scope, session::Scope::driver, |_, _| ()),
            recovery::Policy::new(
                |event| std::assert_matches!(event, Reclaimed::Shutdown),
                recover,
            ),
        );
        assert!(matches!(
            open,
            Ok(Err(CreateError::Manifold(creation::Error::Resident(
                Exhausted::Domains { capacity: 0 }
            ))))
        ));
    });
}

#[test]
fn claims_route_once() {
    use driver::settings::{claim::Needs, errors::Claim};
    let http = channel::Plan::<KeyTag<0>>::new(
        config::Config::try_new("127.0.0.1").expect("HTTP host allocation"),
        Topology::new(1, 1),
    );
    let http = http.expect("test operation must succeed");
    let backoff = time::Duration::from_millis(1);
    let plan = http.plan(backoff, backoff);
    let plan = plan.expect("test operation must succeed");
    let needs = plan.include_in(Needs::new());
    let needs = needs.expect("test operation must succeed");
    assert!(
        matches!(plan.include_in(needs), Err(Claim::DuplicateRoute(route)) if route == route::Id::fixed::<0>())
    );
}

#[test]
fn rejects_backoff() {
    let http = channel::Plan::<KeyTag<0>>::new(
        config::Config::try_new("127.0.0.1").expect("HTTP host allocation"),
        Topology::new(1, 1),
    )
    .expect("HTTP plan");
    assert!(matches!(
        http.plan(time::Duration::ZERO, time::Duration::from_secs(1)),
        Err(config::Error::ConnectorConfig(
            dope::manifold::service::health::Error::Base
        ))
    ));
}

#[test]
fn fixed_host_is_borrowed() {
    pools::Plan::new(
        SocketAddr::from(([127, 0, 0, 1], 9)),
        Config::new(String::from("Example.TEST:8080")),
        1,
        1,
    )
    .expect("fixed client topology")
    .enter(
        |scope| {
            let client = scope.handle;
            let (host, alloc) = Tracker::usage(|| client.host());
            assert_eq!(host, "Example.TEST:8080");
            assert_eq!(alloc, (0, 0));
        },
        recovery::Policy::new(
            |event| std::assert_matches!(event, Reclaimed::Shutdown),
            recover,
        ),
        |result| result.expect("client execution"),
    )
    .expect("fixed application shutdown");
}

#[test]
fn host_borrows_authority() {
    origins::Plan::new(Config::new(String::from("[2001:DB8::1]:8080")), 1, 1, 1)
        .expect("on-demand client topology")
        .enter(
            |app| {
                let client = app.handle;
                let (host, alloc) = Tracker::usage(|| client.host());
                assert_eq!(host, "[2001:DB8::1]:8080");
                assert_eq!(alloc, (0, 0));
            },
            recovery::Policy::new(
                |event| std::assert_matches!(event, Reclaimed::Shutdown),
                recover,
            ),
            |result| result.expect("client execution"),
        )
        .expect("on-demand application shutdown");
}

#[test]
fn rejects_request_layout() {
    assert!(matches!(
        channel::Plan::<KeyTag<0>>::new(
            Config::try_new("127.0.0.1")
                .expect("HTTP host allocation")
                .request_pool(0, 1),
            Topology::new(1, 1)
        ),
        Err(Error::RequestSlots)
    ));
    assert!(matches!(
        channel::Plan::<KeyTag<0>>::new(
            Config::try_new("127.0.0.1")
                .expect("HTTP host allocation")
                .request_pool(1, 0),
            Topology::new(1, 1)
        ),
        Err(Error::RequestLayout(LayoutError::ZeroCapacity))
    ));
}

#[test]
fn rejects_invalid_host() {
    assert!(matches!(
        channel::Plan::<KeyTag<0>>::new(
            Config::try_new("bad\nhost").expect("HTTP host allocation"),
            Topology::new(1, 1)
        ),
        Err(Error::HostAuthority(_))
    ));
}

#[test]
fn rejects_userinfo() {
    let host = "user:secret@127.0.0.1:80";
    assert!(matches!(
        channel::Plan::<KeyTag<0>>::new(
            Config::try_new(host).expect("HTTP host allocation"),
            Topology::new(1, 1)
        ),
        Err(Error::HostUserInfo)
    ));
    assert!(matches!(
        Plan::<KeyTag<0>>::new(
            Config::try_new(host).expect("HTTP host allocation"),
            1,
            1,
            1
        ),
        Err(CreateError::Config(Error::HostUserInfo))
    ));
}

#[test]
fn supports_default_plan() {
    for capacity in 1..=5 {
        (channel::Plan::<KeyTag<0>>::new(
            Config::try_new("127.0.0.1").expect("HTTP host allocation"),
            Topology::new(capacity, 1),
        ))
        .as_ref()
        .expect("test operation must succeed");
        (Plan::<KeyTag<0>>::new(
            Config::try_new("127.0.0.1").expect("HTTP host allocation"),
            capacity,
            1,
            capacity,
        ))
        .as_ref()
        .expect("test operation must succeed");
    }
}

#[test]
fn bounds_connection_target() {
    let topology = Topology::new(5, 1);
    assert_eq!(topology.lanes(), 5);
    assert_eq!(topology.target(), 1);
    assert_eq!(topology.timers(), 1);
    let plan = channel::Plan::<KeyTag<0>>::new(
        Config::try_new("127.0.0.1").expect("HTTP host allocation"),
        topology,
    );
    let plan = plan.expect("one connection can use five response lanes");
    assert_eq!(plan.target(), 1);
    for count in [0, 6] {
        assert!(
            matches!(channel::Plan::<KeyTag<0>>::new(Config::try_new("127.0.0.1").expect("HTTP host allocation"),
            Topology::new(5, 1).with_target(count)),
            Err(Error::ConnectorConnections { connections: actual, response_lanes: 5 }) if actual == count)
        );
    }
}

#[test]
fn sizes_each_lane() {
    for capacity in 1..=5 {
        let config = || {
            Config::try_new("127.0.0.1")
                .expect("HTTP host allocation")
                .queue_bytes(64)
                .inflight(1)
        };
        (channel::Plan::<KeyTag<0>>::new(
            config(),
            Topology::new(capacity, 1).with_target(capacity),
        ))
        .as_ref()
        .expect("test operation must succeed");
        (Plan::<KeyTag<0>>::new(config(), capacity, 1, capacity))
            .as_ref()
            .expect("test operation must succeed");
    }
}
