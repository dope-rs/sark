pub mod common;

use std::{cell::Cell, num::NonZero, pin::pin, task::Poll};

use dope::core::driver::route::{Epoch, indices::SlotIndex};
use dope_fiber::{
    abi::Task,
    context::Context,
    task::storage::{
        fixed::schedule::{self, Pool},
        scope,
    },
};
use dope_test::alloc::Tracker;
use sark_core::{
    body,
    manifold::task::{request::Input, response::Outcome},
    route::Own,
};
use sark_fiber::{
    driver::{Binding, route::KeyTag, schedule::ready::task::owner, settings},
    manifold::runtime::TaskRoute as _,
};
use sark_h1::session;
use sark_http::{h1::codec::canned, payload::response::status::Code, request::Seq};

use crate::common::route::glue::{
    empty::{
        self, __SarkHandlerBuiltX6275696C74, __SarkHandlerPlainX706C61696E,
        __SarkHandlerTrackedX747261636B6564, DROPS, Head,
    },
    storage::Domain,
};
use common::{
    retained,
    route::{
        body::discard::PING,
        chunked::{
            apps::ChunkedDispatch,
            peer::{Error, Peer},
        },
        with_scope,
    },
    server::{MAX_CONNECTIONS, Plan},
};

type Plain = __SarkHandlerPlainX706C61696E;
type Tracked = __SarkHandlerTrackedX747261636B6564;
type Built = __SarkHandlerBuiltX6275696C74;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn empty_input_has_no_owner() {
    with_scope(settings::Config::default(), |scope| {
        let driver = scope.context();
        let timer = driver.driver_ref().timer();
        let binding = Binding::new(timer, owner::Capacity::new(NonZero::<u32>::MIN));
        let ids = Seq::new(binding.sequence());
        let calls = Cell::new(0);
        for wire in [
            empty::HEAD,
            empty::BODY,
            empty::CHUNKED,
            empty::ENCODING_BODY,
        ] {
            let packet = [wire, empty::HEAD].concat();
            let (result, allocs) = Tracker::count(|| {
                empty::try_dispatch::<Plain>(
                    ids.try_next().expect("request identity"),
                    &packet,
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
    });
}

#[test]
fn discard_keeps_pipeline() -> Result<(), Error> {
    retained::run::<ChunkedDispatch, _>(Plan::isolated(), MAX_CONNECTIONS, |addr| {
        let mut peer = Peer::try_connect(addr)?;
        for (wire, first) in [
            (empty::BODY, b"body".as_slice()),
            (empty::CHUNKED, b"body"),
            (empty::ENCODING_BODY, b"encoded"),
        ] {
            for split in [false, true] {
                let remaining = if split {
                    let end = wire
                        .windows(4)
                        .position(|w| w == b"\r\n\r\n")
                        .expect("head")
                        + 2;
                    let head = [&wire[..end], b"Expect: 100-continue\r\n\r\n"].concat();
                    assert_eq!(peer.try_exchange(&head)?, (100, Vec::new()));
                    &wire[end + 2..]
                } else {
                    wire
                };
                let next = b"POST /inspect HTTP/1.1\r\nHost: empty\r\nX-Proof: kept\r\n\r\n";
                peer.socket.write_all(&[remaining, next, PING].concat())?;
                for expected in [first, b"empty", b"pong"] {
                    assert_eq!(peer.try_response()?, (200, expected.to_vec()));
                }
            }
        }
        Ok(())
    })
}

#[test]
fn first_poll_drops_request() {
    type Tasks<'d, F> = Pool<'d, F, Domain, ()>;
    with_scope(settings::Config::default(), |scope| {
        let (_, mut driver, mut control) = scope.split();
        let core = driver.driver_ref();
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
        let mut tasks =
            pin!(Tasks::try_new(o3::depot::capacity::Count::new(1)).expect("fixed storage"));
        // Retain earlier outputs while the next request reuses the only task slot.
        let completed = scope::Scope::with(tasks.as_mut(), |mut tasks| {
            [empty::HEAD, empty::BODY, empty::CHUNKED].map(|wire| {
                calls.set(0);
                DROPS.set(0);
                for accept in [false, true] {
                    let (input, outcome, attempts) = empty::try_dispatch::<Tracked>(
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
                let (input, _, _) = empty::try_dispatch::<Tracked>(
                    ids.try_next().expect("polled identity"),
                    wire,
                    true,
                )
                .expect("tracked request");
                let _ = tasks
                    .try_admit((), target.target().into())
                    .expect("fixed quota released before output")
                    .insert((
                        Tracked::invoke_task((input.expect("admitted input"), &calls, timer)),
                        (),
                    ));
                assert_eq!(calls.get(), 0);
                assert_eq!(DROPS.get(), 0);
                let turn = control.begin();
                let schedule::result::Selection::Ready(selected) =
                    tasks.as_mut().select(turn.turn().app())
                else {
                    panic!("fixed task ready");
                };
                let schedule::result::Outcome::Complete { output, .. } =
                    selected.drive(&mut driver)
                else {
                    panic!("fixed task complete");
                };
                assert_eq!(calls.get(), 1);
                assert_eq!(DROPS.get(), 1);
                output
            })
        });
        for output in completed {
            assert!(matches!(
                output.into_parts().1,
                Outcome::Response(reply) if reply.status == Code::OK
            ));
        }
        let pad = body::DEFAULT_BUILD_STEP_BUDGET.get();
        for (json, expected, polls) in [
            (br#"{"value":"ok"}"#.as_slice(), Ok(Code::OK), 2),
            (b"{}".as_slice(), Err(Code::BAD_REQUEST), 3),
        ] {
            let mut wire = format!(
                "POST / HTTP/1.1\r\nHost: empty\r\nContent-Length: {}\r\n\r\n",
                pad + json.len(),
            )
            .into_bytes();
            wire.resize(wire.len() + pad, b' ');
            wire.extend_from_slice(json);
            let id = ids.try_next().expect("yielding request identity");
            let (input, _, _) =
                empty::try_dispatch::<Built>(id, &wire, true).expect("yielding request");
            let input = input.expect("admitted buffered request");
            assert!(matches!(input, Input::Buffered(_)));
            calls.set(0);
            let mut task = pin!(Task::new(Built::invoke_task((input, &calls, timer))));
            for poll in 1..=polls {
                let ready = poll == polls;
                let turn = control.begin();
                let output = turn.turn().app().share::<1, _>(|share| {
                    Context::with_target(
                        target.target(),
                        share.work(),
                        driver.reborrow(),
                        |context| context.poll(task.as_mut()),
                    )
                });
                let status = output.map(|output| {
                    let (received, outcome) = output.into_parts();
                    assert_eq!(received, id);
                    match outcome {
                        Outcome::Response(reply) => Ok(reply.status),
                        Outcome::Rejected(reject) => Err(reject.status()),
                    }
                });
                assert_eq!(
                    status,
                    if ready {
                        Poll::Ready(expected)
                    } else {
                        Poll::Pending
                    }
                );
                assert_eq!(calls.get(), usize::from(ready && expected.is_ok()));
            }
        }
    });
}
