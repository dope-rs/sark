use std::{cell::Cell, num::NonZero, time::Duration};

use dope::{
    core::{
        driver::{life::recovery::Policy, route::KeyTag, settings},
        io::socket::Listener,
    },
    manifold::app::receive::limits::Budget,
    net::wire::{self, domain},
    runtime::{executor::session, shutdown},
};
use dope_fiber::task::storage::scope::Brand;
use o3::{buffer, buffer::hold::limits::Bound};
use sark_fiber::Config;
use sark_grpc::server::{
    Bank,
    config::Limits,
    mode,
    runtime::state::Error,
    service::{self, handler, reply, timers},
};
use sark_h2::server::runtime::{
    self,
    app::{Failure, Report},
};

use crate::common::{
    route::body::tunnels::{ConnectDispatch, Duplex, Snapshot, State},
    server::{self, recover},
};

struct OwnedRpc;

impl handler::Handler for OwnedRpc {
    type Call = ();
    type Timer = timers::Off;

    fn request<S: buffer::raw::Segment, W: reply::Write>(
        &mut self,
        _: &mut (),
        _: &mut (),
        _: service::Request<'_, S>,
        response: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        response.push_message(vec![8, 1]);
        handler::RequestState::Complete
    }
}

pub fn run(context: shutdown::ReadySource<'_>, listener: Listener) -> (Snapshot, usize) {
    let plan = server::Plan::new(listener.local_addr(), Duration::from_secs(2))
        .build()
        .gated_scoped();
    let mut reports = server::Plan::try_reports(&plan).expect("claims");
    let prepared = plan.with_bound(listener).prepare(()).expect("resources");
    let bank = Bank::try_new(NonZero::new(1).expect("one connection"), Limits::default())
        .expect("gRPC bank");
    let counters = Duplex::default();
    let state = State {
        counters: &counters,
        finish_output_on_input: false,
        hold_output_on_finish: false,
    };
    let failed = Cell::new(0);
    let mut done = session::Completion::new();
    prepared.enter_with(
        context,
        |worker| {
            worker.lend(|plan, stage| {
                Brand::with(|brand| {
                    let h2 = runtime::app::App::new(
                        mode::Direct.build(OwnedRpc, &bank),
                        sark_h2::server::Plan::new(
                            super::server::CONFIG,
                            settings::Receive::fixed::<64, { 32 << 10 }>(),
                        )
                        .expect("H2 limits"),
                        Report::new(
                            |failure: Failure<
                                '_,
                                '_,
                                '_,
                                wire::Identity,
                                KeyTag<0>,
                                sark_grpc::server::runtime::run::Failure,
                                runtime::Build<Error>,
                            >| {
                                match failure {
                                    Failure::Construction(runtime::Build::App(
                                        Error::Connections { capacity },
                                    )) => {
                                        assert_eq!(capacity.get(), 1);
                                        failed.set(failed.get() + 1);
                                    }
                                    _ => panic!("unexpected H2 failure"),
                                }
                            },
                        ),
                    )
                    .expect("H2 app");
                    plan.prepare_in(
                        stage,
                        domain::Local::<wire::Identity, _>::new(wire::Identity),
                        |prepared, _wait, stage| {
                            let root = ConnectDispatch::cleartext_prepared(
                                brand,
                                &state,
                                Config::new(server::MAX_CONNECTIONS)
                                    .with_budget(Budget::new(Bound::new(4096), 0)),
                                prepared,
                                h2,
                                &mut stage.driver(),
                            )
                            .expect("combined listener");
                            stage
                                .run_quiesce(&mut done, root, Duration::from_secs(2))
                                .expect("session")
                                .expect("drive");
                        },
                    )
                    .expect("prepare listener");
                });
            })
        },
        Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("topology");
        },
    );
    assert!(
        reports.events().expect("retirement").is_empty(),
        "{reports:?}"
    );
    (counters.snapshot(), failed.get())
}

sark_fiber::define_cleartext_route! {
    PlainDispatch: () => {
        GET "/hello" => crate::common::route::glue::hello::gzip,
    }
}

pub fn run_plain(context: shutdown::ReadySource<'_>, listener: Listener) {
    let plan = server::Plan::new(listener.local_addr(), Duration::from_secs(2)).build().scoped::<true>();
    let mut reports = server::Plan::try_reports(&plan).expect("claims");
    let prepared = plan.with_bound(listener).prepare(()).expect("resources");
    let bank = Bank::try_new(NonZero::new(1).expect("one connection"), Limits::default())
        .expect("gRPC bank");
    let mut done = session::Completion::new();
    prepared.enter_with(
        context,
        |worker| worker.lend(|plan, stage| {
            let h2 = runtime::app::App::new(
                mode::Direct.build(OwnedRpc, &bank),
                sark_h2::server::Plan::new(
                    super::server::CONFIG,
                    settings::Receive::fixed::<64, { 32 << 10 }>(),
                ).expect("H2 limits"),
                dope::manifold::network::listener::handler::failures::Close,
            ).expect("H2 app");
            plan.prepare_in(
                stage,
                domain::Local::<wire::Identity, _>::new(wire::Identity),
                |prepared, _wait, stage| {
                    let root = PlainDispatch::cleartext_prepared(
                        &(),
                        Config::new(server::MAX_CONNECTIONS)
                            .with_budget(Budget::new(Bound::new(4096), 0)),
                        prepared,
                        h2,
                        &mut stage.driver(),
                    ).expect("plain combined listener");
                    stage.run_quiesce(&mut done, root, Duration::from_secs(2))
                        .expect("session").expect("drive");
                },
            ).expect("prepare listener");
        }),
        Policy::new(|event| reports.push(event), recover),
        |result| { result.as_ref().map_err(|error| error.cause()).expect("topology"); },
    );
    assert!(reports.events().expect("retirement").is_empty(), "{reports:?}");
}
