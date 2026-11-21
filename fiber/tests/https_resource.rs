pub mod common;

use std::{cell::Cell, net::Ipv4Addr};

use common::{
    client::runtime::sockets,
    server,
    support::{self, Mode, Routes, resource},
};
use dope::{
    core::{
        BranchError,
        driver::{
            route::{FrameworkTag, Tag, kind},
            settings::errors,
            storage,
        },
    },
    net::wire::{self, domain},
    runtime::topology::{self, Installer as _},
};
use dope_test::reclaim;
use dope_tls::tls::{self, endpoint, roles};
use sark_fiber::{Config, Throughput};
use sark_h1::server::{date, run::prepare};

#[test]
fn preserves_resource() {
    let hello = |response: &[u8]| {
        let reply = sockets::Response::try_parse(response)?;
        assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
        Ok::<_, sockets::Error>(())
    };

    for mode in [Mode::Plain, Mode::Tls] {
        let (harness, server) = server::Plan::isolated();
        match mode {
            Mode::Plain => harness
                .run(
                    move |context, listener| {
                        let plan = server.with_bound(listener);
                        let clock = date::Clock::new().map_err(BranchError::First)?;
                        let storage = storage::With::new((), clock);
                        let failures = Cell::new(Vec::new());
                        let state = server::State(&failures);
                        let prepared =
                            topology::Plan::new(storage, (resource::Install::default(), plan))
                                .for_install::<Throughput>()
                                .map_err(BranchError::Second)?;
                        let mut reports = reclaim::Reports::new(
                            prepared.layout(),
                            [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                        );
                        prepared.enter_with(
                            context,
                            |scope| {
                                scope.lend(|(install, plan), stage| {
                                    let tick = install
                                        .install(&mut stage.stage())
                                        .map_err(BranchError::First)?;
                                    let app = Routes::new::<wire::Identity, _>(
                                        &state,
                                        Config::new(server::MAX_CONNECTIONS),
                                    );
                                    plan.install(
                                        stage,
                                        domain::Local::new(wire::Identity),
                                        app,
                                        |root, _, stage| {
                                            tick.serve(root, stage);
                                        },
                                    )
                                    .map_err(BranchError::Second)
                                })
                            },
                            server::reporting(&mut reports),
                            resource::completed,
                        );
                        server::reclaimed(&reports);
                        let failures = failures.into_inner();
                        assert!(failures.is_empty(), "plaintext listener failures: {failures:?}");
                        Ok::<(), BranchError<date::Error, errors::Claim>>(())
                    },
                    |bind| {
                        let mut client = sockets::Plain::connect(bind)?;
                        client.request("/hello", bind)?;
                        hello(&client.response()?)
                    },
                )
                .expect("plaintext resource harness"),
            Mode::Tls => harness
                .run(
                    move |context, listener| {
                        let config = support::tls().server().expect("TLS server config");
                        let wire = endpoint::Config::from(config);
                        let plan = server.with_bound(listener);
                        let owner = plan
                            .owner::<tls::Tls<roles::Server>>()
                            .map_err(|error| BranchError::First(prepare::Error::Resource(error)))?;
                        let clock = date::Clock::new()
                            .map_err(|error| BranchError::First(prepare::Error::Date(error)))?;
                        let storage = storage::With::new(storage::Value::new(owner), clock);
                        let failures = Cell::new(Vec::new());
                        let state = server::State(&failures);
                        let prepared =
                            topology::Plan::new(storage, (resource::Install::default(), plan))
                                .for_install::<Throughput>()
                                .map_err(BranchError::Second)?;
                        let mut reports = reclaim::Reports::new(
                            prepared.layout(),
                            [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                        );
                        prepared.enter_with(
                            context,
                            |scope| {
                                scope.lend(|(install, plan), stage| {
                                    let tick = install
                                        .install(&mut stage.stage())
                                        .map_err(BranchError::First)?;
                                    let app =
                                        Routes::new(&state, Config::new(server::MAX_CONNECTIONS));
                                    let wire = stage.storage().0.bind(wire).map_err(|error| {
                                        BranchError::Second(BranchError::First(error))
                                    })?;
                                    plan.install(stage, wire, app, |root, _, stage| {
                                        tick.serve(root, stage)
                                    })
                                    .map_err(|error| {
                                        BranchError::Second(BranchError::Second(error))
                                    })
                                })
                            },
                            server::reporting(&mut reports),
                            resource::completed,
                        );
                        server::reclaimed(&reports);
                        let failures = failures.into_inner();
                        assert!(failures.is_empty(), "TLS failures: {failures:?}");
                        Ok::<(), BranchError<prepare::Error, errors::Claim>>(())
                    },
                    |bind| {
                        let mut client = sockets::Tls::connect(
                            bind,
                            support::tls().client().expect("TLS client config"),
                            Ipv4Addr::LOCALHOST.into(),
                        )?;
                        client.request("/hello", bind)?;
                        hello(&client.response()?)
                    },
                )
                .expect("TLS resource harness"),
        }
        .expect("HTTP resource response");
    }
}
