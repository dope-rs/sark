pub mod common;

use std::{hint, mem, num::NonZero};

use dope::{
    core::{
        BranchError,
        driver::{
            Binding, life::recovery::Policy, route::KeyTag, schedule::ready::task::owner, settings,
        },
    },
    manifold::app::receive::limits::Budget,
    net::wire::{self, Identity},
};
use dope_fiber::task::storage::scope::Brand;
use dope_test::{
    alloc::Tracker,
    scope::{Driver, Error},
};
use o3::buffer::hold::limits::Bound;
use sark_core::{decode::Decode, duplex};
use sark_fiber::{Config, EmptyState};
use sark_h1::server::{date, deadline::policies};
use sark_h2::server;
use sark_http::field::metadata::block;

use crate::common::{
    h1::{
        DUPLEX_STATE, FiberApplication, Layout as _, MultiDuplexApplication, PortableRouter,
        Session, SingleDuplexApplication,
    },
    h2::{
        apps::Layout as _,
        server::{CONNECT, EXTENDED},
    },
    route::{
        body::tunnels::{self, ConnectFirst, WebsocketFirst, WebsocketOnly},
        glue::agnostic::{self, AgnApp},
        metadata::{DecodeExt as _, EntriesExt as _},
    },
};
use common::server::recover;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn router_is_zst() {
    assert_eq!(mem::size_of::<PortableRouter>(), 0);
}

#[test]
fn duplex_layout_is_exact() {
    let layout = Brand::with(|brand| {
        FiberApplication::new::<Identity, KeyTag<0>>(brand, EmptyState::REF, Config::new(1))
            .dimensions(policies::HeadOnly)
    });
    let budget = Budget::new(Bound::new(4096), 0);
    let single = SingleDuplexApplication::new::<_, 1, Identity, KeyTag<0>>(
        &DUPLEX_STATE,
        Config::new(1).with_budget(budget),
    );
    let multiple = MultiDuplexApplication::new::<_, 1, Identity, KeyTag<0>>(
        &DUPLEX_STATE,
        Config::new(1).with_budget(budget),
    );

    assert!(
        layout
            == (
                mem::size_of::<duplex::Never>(),
                mem::align_of::<duplex::Never>(),
                block::Connect::Disabled,
            ),
    );
    assert!(
        single.dimensions(policies::HeadOnly)
            == (
                mem::size_of::<Session<KeyTag<0>>>(),
                mem::align_of::<Session<KeyTag<0>>>(),
                block::Connect::Enabled,
            ),
    );
    assert!(multiple.dimensions(policies::HeadOnly) == (1, 1, block::Connect::Enabled,),);

    let counters = tunnels::Duplex::default();
    let state = tunnels::State {
        counters: &counters,
        finish_output_on_input: false,
        hold_output_on_finish: false,
    };
    let ws = (
        mem::size_of::<sark_ws::server::Session<tunnels::Websocket<'_>>>(),
        mem::align_of::<sark_ws::server::Session<tunnels::Websocket<'_>>>(),
    );
    assert_eq!(
        WebsocketOnly::multiplexed::<Identity, KeyTag<0>, server::Target<'_, KeyTag<0>>>(
            &state,
            Config::new(1),
        )
        .layout(),
        (
            mem::size_of::<duplex::Never>(),
            mem::align_of::<duplex::Never>()
        ),
    );
    assert_eq!(
        WebsocketFirst::multiplexed::<Identity, KeyTag<0>, server::Target<'_, KeyTag<0>>>(
            &state,
            Config::new(1),
        )
        .layout(),
        ws,
    );
    assert_eq!(
        ConnectFirst::multiplexed::<Identity, KeyTag<0>, server::Target<'_, KeyTag<0>>>(
            &state,
            Config::new(1),
        )
        .layout(),
        ws,
    );
    let echo = Brand::with(|brand| {
        tunnels::ConnectDispatch::multiplexed::<Identity, KeyTag<0>, server::Target<'_, KeyTag<0>>>(
            brand,
            &state,
            Config::new(1),
        )
        .layout()
    });
    assert_eq!(
        echo,
        (
            mem::size_of::<tunnels::Echo<'_>>(),
            mem::align_of::<tunnels::Echo<'_>>()
        )
    );
}

#[test]
fn duplex_bind_no_gzip() {
    let mut reports = Vec::new();
    Driver::new(settings::Config::for_profile::<sark_fiber::Throughput>()).with_driver(
        |driver| {
            let clock = date::Clock::new().expect("HTTP date clock");
            let binding = Binding::new(
                driver.driver_ref().timer(),
                owner::Capacity::new(NonZero::<u32>::MIN),
            );
            let config = Config::new(1).with_budget(Budget::new(Bound::new(4096), 0));
            let app =
                SingleDuplexApplication::new::<_, 1, Identity, KeyTag<0>>(&DUPLEX_STATE, config);
            let (bound, allocs) = Tracker::count(|| {
                sark_h1::server::listener::bind::Bind::try_bind(
                    app,
                    binding,
                    policies::HeadOnly,
                    &clock,
                )
            });
            let bound = bound.expect("duplex application binding");
            assert_eq!(allocs, 0);
            hint::black_box(&bound);
        },
        Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| match error {
                    Error::Create(error) => BranchError::First(error.kind()),
                    Error::Close(error) => BranchError::Second(error.cause()),
                })
                .expect("driver");
        },
    );
    assert!(reports.is_empty(), "driver retirement: {reports:?}");
}

#[test]
fn routes_without_alloc() {
    let mut reports3 = Vec::new();
    Driver::new(settings::Config::for_profile::<sark_fiber::Throughput>()).with_driver(
        |driver| {
            let binding = Binding::new(
                driver.driver_ref().timer(),
                owner::Capacity::new(NonZero::<u32>::MIN),
            );
            let app = sark_core::manifold::task::app::Bind::try_bind(
                AgnApp::multiplexed::<wire::Identity, KeyTag<0>, server::Target<'_, KeyTag<0>>>(
                    EmptyState::REF,
                    Config::new(1),
                ),
                binding,
            )
            .expect("generated runtime must bind");

            let full = agnostic::FIELDS
                .try_raw(block::Connect::Disabled)
                .expect("router metadata fixture")
                .try_request()
                .ok()
                .expect("admitted request");
            let (prepared, (count, bytes)) = Tracker::usage(|| app.try_full(full));
            prepared.expect("route head must be valid");
            assert_eq!((count, bytes), (0, 0));

            let fields = agnostic::FIELDS
                .try_decoded()
                .expect("router metadata fixture");
            let planned = app
                .try_select(fields)
                .expect("retained metadata")
                .try_request()
                .ok()
                .expect("admitted request");
            let (prepared, (count, bytes)) = Tracker::usage(|| app.try_head(planned));
            prepared.expect("route head must be valid");
            assert_eq!((count, bytes), (0, 0));

            let fields = [
                agnostic::FIELDS.as_slice(),
                agnostic::BAD_ENCODING.as_slice(),
            ]
            .concat();
            let full = fields
                .try_raw(block::Connect::Disabled)
                .expect("native field admission")
                .try_request()
                .ok()
                .expect("request");
            let planned = app
                .try_select(fields.try_decoded().expect("decoded fields"))
                .expect("selected fields")
                .try_request()
                .ok()
                .expect("request");
            for result in [app.try_full(full), app.try_head(planned)] {
                assert!(matches!(
                    result,
                    Err(sark_core::decode::HeadError::Schema(
                        agnostic::AgnAppHeadCause::R0001(sark_core::route::Error::Header {
                            field: agnostic::PairReqIdentity::Header0,
                            cause: sark_core::request::field::Error::AcceptEncoding(
                                sark_http::field::head::Error::Value
                            ),
                        })
                    ))
                ));
            }
        },
        Policy::new(|event| reports3.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| match error {
                    Error::Create(error) => BranchError::First(error.kind()),
                    Error::Close(error) => BranchError::Second(error.cause()),
                })
                .expect("driver");
        },
    );
    assert!(
        reports3.is_empty(),
        "identity-only scope returned I/O: {reports3:?}"
    );
}

#[test]
fn full_connect_forms() {
    let app = SingleDuplexApplication::new::<_, 1, Identity, KeyTag<0>>(
        &DUPLEX_STATE,
        Config::new(1).with_budget(Budget::new(Bound::new(4096), 0)),
    );
    let classic = CONNECT.map(|entry| entry.as_record());
    let extended = EXTENDED.map(|entry| entry.as_record());
    let full = [classic.as_slice(), extended.as_slice()].map(|fields| {
        let head = fields
            .try_raw(block::Connect::Enabled)
            .expect("CONNECT metadata")
            .try_request()
            .ok()
            .expect("admitted CONNECT request");
        app.try_full(head).is_ok()
    });
    assert_eq!(
        full,
        [true, true],
        "classic and extended CONNECT full routes"
    );
}

#[test]
fn full_asterisk() {
    let app = AgnApp::new::<Identity, KeyTag<0>>(EmptyState::REF, Config::new(1));
    let head = agnostic::ASTERISK
        .try_raw(block::Connect::Disabled)
        .expect("OPTIONS metadata")
        .try_request()
        .ok()
        .expect("admitted OPTIONS request");
    assert!(app.try_full(head).is_ok(), "full OPTIONS route");
}
