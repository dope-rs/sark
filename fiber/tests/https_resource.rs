pub mod common;

use core::BranchError;
use std::{cell::Cell, convert::Infallible, net::Ipv4Addr, time::Duration};

use common::server::recover;
use common::{
    client::runtime::sockets,
    server,
    support::{
        self,
        resource::{Install, Plain, Tls},
    },
};
use dope::{
    core::{
        self,
        driver::{
            life::recovery::Policy,
            route::{FrameworkTag, Tag, kind},
            settings::errors,
            storage,
        },
    },
    runtime::{
        executor::drive,
        topology::{
            self,
            lending::{self, Compose as _},
        },
    },
};
use dope_test::{harness::server::Pair, reclaim};
use dope_tls::tls::{self, endpoint, roles};
use run::prepare;
use sark_fiber::Throughput;
use sark_h1::server::{date, run};
use support::Mode;

#[test]
fn preserves_resource() {
    for mode in [Mode::Plain, Mode::Tls] {
        let harness = (Pair::bind()).expect("bind resource harness");
        let bind = harness.addr();
        match mode {
            Mode::Plain => {
                let server = server::Plan::new(bind, Duration::from_secs(10)).build();
                let result = harness.run(
                    move |context, listener| {
                        let plan = server.with_bound(listener);
                        let clock = date::Clock::new().map_err(BranchError::First)?;
                        let storage = storage::With::new((), clock);
                        let errors = Cell::new(Vec::new());
                        let prepared =
                            topology::Plan::new(storage, (Install::default(), Plain {
                                plan,
                                state: server::State(&errors),
                                finish: |result: Result<(Result<(), drive::Error>, bool), lending::Error<'_, Infallible, _>>| {
                                    let (result, matches) = result.expect("plain session");
                                    assert!(matches, "composed client must retain the installed interval route");
                                    result.expect("plain drive");
                                },
                            }))
                                .for_install::<Throughput>()
                                .map_err(BranchError::Second)?;
                        {
                            let mut reports = reclaim::Reports::new(
                                prepared.layout(),
                                [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                            );
                            prepared.enter_with(
                                context,
                                |scope| scope.lend(|(head, tail), stage| {
                                    let owner = stage
                                        .with_stage(|stage| topology::Installer::install(head, stage))
                                        .map_err(BranchError::First)?;
                                    tail.compose(owner, stage).map_err(BranchError::Second)
                                }),
                                Policy::new(
                                    |report| reports.push(report),
                                    recover,
                                ),
                                |outcome| {
                                    outcome.as_ref().map_err(|error| error.kind()).expect("plain topology");
                                    outcome.ok().expect("checked topology").expect("plain composed application failed")
                                },
                            );
                            assert!(
                                reports.events().expect("reclaimed resources").is_empty(),
                                "unexpected reclaimed server operations: {reports:?}"
                            );
                        }

                        let errors = errors.into_inner();
                        assert!(errors.is_empty(), "TLS failures: {errors:?}");
                        Ok::<(), BranchError<date::Error, errors::Claim>>(())
                    },
                    |bind| {
                        let mut client = sockets::Plain::connect(bind)?;
                        client.request("/hello", bind)?;
                        let response = client.response()?;
                        let reply = sockets::Response::try_parse(&response)?;
                        assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
                        Ok::<_, sockets::Error>(())
                    },
                );
                assert!(
                    matches!(
                        result.map(|result| result.expect("plain resource harness")),
                        Ok(())
                    ),
                    "plain resource harness"
                );
            }
            Mode::Tls => {
                let server = server::Plan::new(bind, Duration::from_secs(10)).build();
                let result = harness.run(
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
                        let errors = Cell::new(Vec::new());
                        let prepared =
                            topology::Plan::new(storage, (Install::default(), Tls {
                                plan,
                                wire,
                                state: server::State(&errors),
                                finish: |result: Result<(Result<(), drive::Error>, bool), lending::Error<'_, Infallible, _>>| {
                                    let (result, matches) = result.expect("TLS session");
                                    assert!(matches, "composed client must retain the installed interval route");
                                    result.expect("TLS drive");
                                },
                            }))
                                .for_install::<Throughput>()
                                .map_err(BranchError::Second)?;
                        {
                            let mut reports = reclaim::Reports::new(
                                prepared.layout(),
                                [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                            );
                            prepared.enter_with(
                                context,
                                |scope| scope.lend(|(head, tail), stage| {
                                    let owner = stage
                                        .with_stage(|stage| topology::Installer::install(head, stage))
                                        .map_err(BranchError::First)?;
                                    tail.compose(owner, stage).map_err(BranchError::Second)
                                }),
                                Policy::new(
                                    |report| reports.push(report),
                                    recover,
                                ),
                                |outcome| {
                                    outcome.as_ref().map_err(|error| error.kind()).expect("TLS topology");
                                    outcome.ok().expect("checked topology").expect("TLS composed application failed")
                                },
                            );
                            assert!(
                                reports.events().expect("reclaimed resources").is_empty(),
                                "unexpected reclaimed server operations: {reports:?}"
                            );
                        }

                        let errors = errors.into_inner();
                        assert!(errors.is_empty(), "TLS failures: {errors:?}");
                        Ok::<(), BranchError<prepare::Error, errors::Claim>>(())
                    },
                    |bind| {
                        let mut client = sockets::Tls::connect(
                            bind,
                            support::tls().client().expect("TLS client config"),
                            Ipv4Addr::LOCALHOST.into(),
                        )?;
                        client.request("/hello", bind)?;
                        let response = client.response()?;
                        let reply = sockets::Response::try_parse(&response)?;
                        assert_eq!((reply.status, reply.body), (200, b"hello".as_slice()));
                        drop(client);
                        Ok::<_, sockets::Error>(())
                    },
                );
                assert!(
                    matches!(
                        result.map(|result| result.expect("TLS resource harness")),
                        Ok(())
                    ),
                    "TLS resource harness"
                );
            }
        }
    }
}
