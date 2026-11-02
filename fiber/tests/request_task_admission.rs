pub mod common;

use std::{
    cell::Cell,
    num::NonZero,
    pin::{self, pin},
    task::Poll,
};

use dope::core::{
    BranchError,
    driver::{
        life::recovery::Policy,
        retained,
        route::{Epoch, indices::SlotIndex},
        schedule,
    },
};
use dope_fiber::{
    abi::Task,
    context::Context,
    task::storage::{
        fixed::{self, quota, schedule::Pool, staged, tree},
        scope,
    },
};
use dope_test::{
    alloc::Tracker,
    fibers::Yield,
    scope::{Driver, Error},
};
use o3::depot::arena::slab::storage::Heap;
use sark_core::{
    body,
    manifold::task::{request::Input, response::Outcome},
    route::{Own, TaskBuild as _},
};
use sark_fiber::{
    driver::{Binding, route::KeyTag, schedule::ready::task::owner, settings},
    manifold::runtime::{Cold, Initial, TaskRoute as _},
};
use sark_h1::session;
use sark_http::{h1::codec::canned, payload::response::status::Code, request::Seq};

use crate::common::route::glue::{
    empty::{
        self, __SarkHandlerBuiltX6275696C74, __SarkHandlerInspectX696E7370656374,
        __SarkHandlerPlainX706C61696E, __SarkHandlerTrackedX747261636B6564, Carrier, DROPS, Head,
        Inspected,
    },
    storage::Domain,
};
use common::server::recover;

type Plain = __SarkHandlerPlainX706C61696E;
type Tracked = __SarkHandlerTrackedX747261636B6564;
type Inspect = __SarkHandlerInspectX696E7370656374;
type Built = __SarkHandlerBuiltX6275696C74;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn empty_input_has_no_owner() {
    let mut reports = Vec::new();
    Driver::new(settings::Config::default()).with_driver(
        |driver| {
            let timer = driver.driver_ref().timer();
            let binding = Binding::new(timer, owner::Capacity::new(NonZero::<u32>::MIN));
            let ids = Seq::new(binding.sequence());
            let calls = Cell::new(0);
            for wire in [empty::HEAD, empty::BODY, empty::CHUNKED] {
                let (result, allocs) = Tracker::count(|| {
                    Carrier::<Plain>::try_dispatch(
                        ids.try_next().expect("request identity"),
                        wire,
                        true,
                    )
                });
                let (input, outcome, attempts) = result.expect("dispatch");
                assert_eq!(allocs, 0);
                assert_eq!(attempts, 1);
                assert!(matches!(input, Some(Input::Ready { .. })));
                assert!(matches!(
                    outcome,
                    session::RouteOutcome::Outcome(session::Outcome::Park { consumed, .. })
                        if consumed == wire.len()
                ));
                let task = Plain::invoke_task((input.expect("admitted input"), &calls, timer));
                assert_eq!(calls.get(), 0);
                drop(task);
                assert_eq!(calls.get(), 0);
            }
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
fn discard_keeps_pipeline() {
    let mut reports = Vec::new();
    Driver::new(settings::Config::default()).with_driver(
        |driver| {
            let binding = Binding::new(
                driver.driver_ref().timer(),
                owner::Capacity::new(NonZero::<u32>::MIN),
            );
            let ids = Seq::new(binding.sequence());
            for wire in [empty::BODY, empty::CHUNKED] {
                let head = wire
                    .windows(4)
                    .position(|part| part == b"\r\n\r\n")
                    .expect("request head")
                    + 4;
                let packet = [wire, empty::HEAD].concat();
                for split in [packet.len(), head] {
                    let expected = if split == packet.len() {
                        wire.len()
                    } else {
                        wire.len() - split
                    };
                    let (input, consumed, pipeline, rest) =
                        Carrier::<Plain>::try_receive(&ids, &packet[..split], &packet[split..])
                            .expect("plain pipeline");
                    assert_eq!(consumed, expected);
                    assert_eq!(rest, empty::HEAD.len());
                    drop((input, pipeline));

                    let (input, consumed, pipeline, rest) =
                        Carrier::<Inspect>::try_receive(&ids, &packet[..split], &packet[split..])
                            .expect("inspected pipeline");
                    assert_eq!(consumed, expected);
                    assert_eq!(rest, empty::HEAD.len());
                    for (input, len) in [(input, 1), (pipeline, 0)] {
                        let built = match input {
                            Input::Ready { request } => request,
                            Input::Buffered(request) => {
                                let builder = Inspected::start_task(request)
                                    .expect("retained header projection");
                                match builder.step(body::DEFAULT_BUILD_STEP_BUDGET) {
                                    body::Step::Ready { output, .. } => Some(output),
                                    _ => None,
                                }
                                .expect("discarded body ready")
                            }
                        };
                        let (_, request) = built.into_parts();
                        assert_eq!(request.len.len(), len);
                        assert_eq!(
                            request.proof.as_ref().map(|proof| proof.as_slice()),
                            Some(b"kept".as_slice())
                        );
                    }
                }
            }
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
fn first_poll_drops_request() {
    let mut reports = Vec::new();
    Driver::new(settings::Config::default()).with_scope(
        |scope| {
            scope.with_driver(|context, mut control| {
                let core = context.driver_ref();
                let timer = core.timer();
                let binding = Binding::new(timer, owner::Capacity::new(NonZero::<u32>::MIN));
                let ids = Seq::new(binding.sequence());
                let calls = Cell::new(0);
                DROPS.set(0);
                assert!(Head::empty_task(ids.try_next().expect("header drop identity")).is_none());
                assert_eq!(DROPS.get(), 0);
                let target = core
                    .ready()
                    .make_slot(
                        core.targets::<KeyTag<0>>()
                            .bind(SlotIndex::ZERO, Epoch::INITIAL)
                            .dispatch(),
                    )
                    .expect("ready slot");
                let owner = retained::raw::Owner::from_driver(core);
                let mut driver = retained::Context::new(context, owner);
                for wire in [empty::HEAD, empty::BODY, empty::CHUNKED] {
                    calls.set(0);
                    DROPS.set(0);
                    for accept in [false, true] {
                        let (input, outcome, attempts) = Carrier::<Tracked>::try_dispatch(
                            ids.try_next().expect("unpolled identity"),
                            wire,
                            accept,
                        )
                        .expect("tracked admission");
                        assert_eq!(attempts, 1);
                        if accept {
                            assert!(matches!(input, Some(Input::Buffered(_))));
                            drop(Tracked::invoke_task((
                                input.expect("admitted input"),
                                &calls,
                                timer,
                            )));
                        } else {
                            assert!(input.is_none());
                            assert!(matches!(
                                outcome,
                                session::RouteOutcome::Outcome(session::Outcome::Close(reply))
                                    if reply.as_slice() == canned::SERVICE_UNAVAILABLE.as_slice()
                            ));
                        }
                        assert_eq!(calls.get(), 0);
                        assert_eq!(DROPS.get(), 0);
                    }
                    let (input, _, _) = Carrier::<Tracked>::try_dispatch(
                        ids.try_next().expect("polled identity"),
                        wire,
                        true,
                    )
                    .expect("tracked request");
                    let mut task = pin!(Task::new(Tracked::invoke_task((
                        input.expect("admitted input"),
                        &calls,
                        timer,
                    ))));
                    assert_eq!(calls.get(), 0);
                    assert_eq!(DROPS.get(), 0);
                    let turn = control.begin();
                    let output = turn.turn().app().share::<{ schedule::LIMIT }, _>(|share| {
                        Context::with_target(
                            target.target(),
                            share.work(),
                            driver.reborrow(),
                            |context| context.poll(task.as_mut()),
                        )
                    });
                    assert!(matches!(
                        output.map(|output| output.into_parts().1),
                        Poll::Ready(Outcome::Response(reply)) if reply.status == Code::OK
                    ));
                    assert_eq!(calls.get(), 1);
                    assert_eq!(DROPS.get(), 1);
                }
                let mut body = vec![b' '; body::DEFAULT_BUILD_STEP_BUDGET.get()];
                body.extend_from_slice(br#"{"value":"ok"}"#);
                let mut wire = format!(
                    "POST / HTTP/1.1\r\nHost: empty\r\nContent-Length: {}\r\n\r\n",
                    body.len(),
                )
                .into_bytes();
                wire.extend_from_slice(&body);
                let (input, _, _) = Carrier::<Built>::try_dispatch(
                    ids.try_next().expect("yielding request identity"),
                    &wire,
                    true,
                )
                .expect("yielding request");
                let input = input.expect("admitted buffered request");
                assert!(matches!(input, Input::Buffered(_)));
                calls.set(0);
                let mut task = pin!(Task::new(Built::invoke_task((input, &calls, timer))));
                for ready in [false, true] {
                    let turn = control.begin();
                    let output = turn.turn().app().share::<1, _>(|share| {
                        Context::with_target(
                            target.target(),
                            share.work(),
                            driver.reborrow(),
                            |context| context.poll(task.as_mut()),
                        )
                    });
                    if ready {
                        assert!(matches!(
                            output.map(|output| output.into_parts().1),
                            Poll::Ready(Outcome::Response(reply)) if reply.status == Code::OK
                        ));
                    } else {
                        assert!(output.is_pending());
                    }
                    assert_eq!(calls.get(), usize::from(ready));
                }
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
fn routes_share_quota() {
    scope::Brand::with(|brand| {
        let (brand, plan) = <Cold as Initial>::tasks::<
            tree::Branch<
                tree::Leaf<staged::Slab<'_, Yield, 1, Domain, Heap>>,
                tree::Leaf<staged::Slab<'_, Yield, 1, Domain, Heap>>,
            >,
            tree::Leaf<Pool<'_, Yield, 1, Domain, ()>>,
        >(quota::Limit::new(1), brand);
        let tasks = scope::Owned::new(plan.try_prepare().expect("route storage"), brand);
        let mut tasks = pin::pin!(tasks);
        let mut tasks = tasks.as_mut().scope();

        {
            let mut route = tasks.as_mut().route::<tree::Left<tree::Here>>();
            assert!(route.admit().is_some(), "left route must admit one task");
        }
        assert_eq!(tasks.as_ref().get_ref().available().get(), 1);

        let id = {
            let mut route = tasks.as_mut().route::<tree::Left<tree::Here>>();
            route.insert(Yield::new()).expect("available route")
        };
        assert_eq!(tasks.as_ref().get_ref().available().get(), 0);

        {
            let mut route = tasks.as_mut().route::<tree::Right<tree::Here>>();
            assert!(route.admit().is_none());
        }

        {
            let mut route = tasks.as_mut().route::<tree::Left<tree::Here>>();
            assert!(matches!(route.remove(id), fixed::local::Removal::Removed));
        }
        assert_eq!(tasks.as_ref().get_ref().available().get(), 1);
    });
}
