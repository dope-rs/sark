pub mod common;

use std::{cell::Cell, hint, num};

use dope::{
    core::{
        BranchError,
        driver::{
            self, life::recovery::Policy, route::KeyTag, schedule::ready::task::owner, settings,
        },
    },
    net::wire,
};
use dope_fiber::task::storage::scope::Brand;
use dope_test::{
    alloc::Tracker,
    scope::{Driver, Error},
};
use o3::alloc;
use sark_core::manifold::task::app::Bind as _;
use sark_fiber::Config;
use sark_h1::server::{date, deadline::policies, listener::bind};
use sark_h2::server::Target;

use crate::common::{
    retained::State,
    route::glue::storage::{GlobalBudgetApp, MixedStorageApp},
};
use common::server::recover;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn construction_is_lazy() {
    let failures = Cell::new(Vec::new());
    let state = State(&failures);
    let h1_cfg = Config::new(4);
    let h2_cfg = Config::new(4);

    let ((), (h1_alloc, h1_bytes)) = Tracker::usage(|| {
        Brand::with(|brand| {
            hint::black_box(MixedStorageApp::new::<wire::Identity, KeyTag<0>>(
                brand, &state, h1_cfg,
            ));
        });
    });
    assert_eq!(
        h1_alloc, 0,
        "the direct application records a static plan and defers its plane to listener bind",
    );
    assert_eq!(h1_bytes, 0, "the direct plan retains no allocated bytes");

    let ((), (h2_alloc, h2_bytes)) = Tracker::usage(|| {
        Brand::with(|brand| {
            hint::black_box(MixedStorageApp::multiplexed::<
                wire::Identity,
                KeyTag<0>,
                Target<'static, KeyTag<0>>,
            >(brand, &state, h2_cfg));
        });
    });
    assert_eq!(
        h2_alloc, 0,
        "the multiplexed application records a static plan and defers its plane to driver bind",
    );
    assert_eq!(
        h2_bytes, 0,
        "the multiplexed plan retains no allocated bytes"
    );
}

#[test]
fn listener_prepares() {
    let failures = Cell::new(Vec::new());
    let state = State(&failures);
    let mut reports = Vec::new();
    Driver::new(settings::Config::for_profile::<sark_fiber::Throughput>()).with_driver(
        |driver| {
            let clock = date::Clock::new().expect("HTTP date clock");
            let owners = owner::Capacity::new(num::NonZero::<u32>::MIN);
            Brand::with(|brand| {
                let binding = driver::Binding::new(driver.driver_ref().timer(), owners);
                let app = GlobalBudgetApp::new::<wire::Identity, KeyTag<0>>(
                    brand,
                    &state,
                    Config::new(1),
                );
                let (result, activity) = Tracker::activity(|| {
                    Tracker::fail_after(1, || {
                        bind::Bind::try_bind(app, binding, policies::HeadOnly, &clock)
                    })
                });
                assert!(matches!(
                    result,
                    Err(bind::Error::Allocation(alloc::Error::Exhausted))
                ));
                assert_eq!(activity.allocations, 1);
                assert_eq!(activity.deallocations, 1);
                assert_eq!(activity.allocated_bytes, activity.deallocated_bytes);
            });
            Brand::with(|brand| {
                let binding = driver::Binding::new(driver.driver_ref().timer(), owners);
                let app = GlobalBudgetApp::new::<wire::Identity, KeyTag<0>>(
                    brand,
                    &state,
                    Config::new(1),
                );
                let (runtime, allocs) = Tracker::count(|| {
                    bind::Bind::try_bind(app, binding, policies::HeadOnly, &clock)
                });
                let runtime = runtime.expect("direct task storage binding");
                assert_eq!(allocs, 3);
                hint::black_box(&runtime);
            });
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
fn driver_prepares_storage() {
    let failures = Cell::new(Vec::new());
    let state = State(&failures);

    let mut reports = Vec::new();
    Driver::new(settings::Config::for_profile::<sark_fiber::Throughput>()).with_driver(
        |driver| {
            Brand::with(|brand| {
                let app = MixedStorageApp::multiplexed::<
                    wire::Identity,
                    KeyTag<0>,
                    Target<'static, KeyTag<0>>,
                >(brand, &state, Config::new(4));
                let owners = owner::Capacity::new(num::NonZero::<u32>::MIN.saturating_add(3));
                let binding = driver::Binding::new(driver.driver_ref().timer(), owners);
                let (runtime, (allocs, bytes)) = Tracker::usage(|| app.try_bind(binding));
                let runtime = runtime.expect("route task storage must fit the driver binding");

                assert_ne!(
                    allocs, 0,
                    "driver bind must prepare the fixed route pools and owner scheduler",
                );
                assert_ne!(
                    bytes, 0,
                    "prepared route pools must own their backing bytes"
                );
                hint::black_box(&runtime);
            })
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
    assert!(
        reports.is_empty(),
        "metadata-only storage emitted completion: {reports:?}"
    );
}
