use dope::{
    core::{driver::route::KeyTag, io::socket::Backlog},
    manifold::timing,
};

pub mod common;

use std::{cell::Cell, convert, net::SocketAddr};

use dope::{
    core::driver::{route::table::conn, settings},
    manifold,
};
use dope_test::alloc::Tracker;
use o3::buffer::resident;
use sark_core::manifold::{schedule, server::app};
use sark_grpc::{
    protocol::section,
    server::{Bank, Plan, config::Limits, mode, runtime::state},
};
use sark_h2::{
    ServerRole,
    conn::{event, machine},
    server::runtime::modes,
};
use sark_http::field::metadata::section::rules;

use crate::common::grpc::{
    call::borrowed::{Borrowed, H2},
    settings::Settings,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn borrows_owner() {
    let calls = Cell::new(0_u32);
    let setup = Settings {
        bind: SocketAddr::from(([127, 0, 0, 1], 0)),
        connections: conn::Capacity::fixed::<1>(),
        backlog: Backlog::fixed::<1>(),
        limits: Limits::default(),
    };
    let layout = {
        let bank = Bank::try_new(setup.connections.count(), setup.limits).expect("readiness bank");
        let grpc = Plan::new(Borrowed(&calls), &bank, mode::Direct);
        let plan = setup
            .h2::<KeyTag<H2>>()
            .expect("construct H2 server plan")
            .with_plain::<_, modes::Serve<convert::Infallible>>(grpc);
        let ready = plan
            .for_install::<timing::Throughput>()
            .expect("build exact receive topology");

        assert_eq!(calls.get(), 0);
        ready.layout()
    };
    assert_eq!(
        layout.queue_layout(),
        <manifold::timing::Throughput as settings::Profile>::QUEUES
    );
    calls.set(11);
    assert_eq!(calls.get(), 11);
}

#[test]
fn reserves_once() {
    let calls = Cell::new(0);
    let bank = Bank::try_new(
        conn::Capacity::fixed::<1>().count(),
        Limits::default().try_calls(1).expect("one call"),
    )
    .expect("one readiness resident");
    let app = mode::Direct.build(Borrowed(&calls), &bank);
    check_reservations(app);
}

#[test]
fn drops_prefix() {
    use o3::alloc;

    let cap = conn::Capacity::fixed::<4>().count();
    let limits = Limits::default().try_calls(1).expect("one call");
    let (result, cold) = Tracker::measure(|| Bank::try_new(cap, limits));
    let bank = result.expect("readiness residents");
    assert_eq!(cold[0], 5);
    assert_eq!(cold[2], 0);
    let ((), freed) = Tracker::measure(|| drop(bank));
    assert_eq!(freed, [0, 0, cold[0], cold[1]]);

    for fail in 0..cold[0] {
        let (result, usage) =
            Tracker::measure(|| Tracker::fail_after(fail, || Bank::try_new(cap, limits)));
        assert_eq!(
            result.err().expect("readiness allocation failure"),
            alloc::Error::Exhausted
        );
        assert_eq!(usage[0], fail);
        assert_eq!(usage[2], fail);
        assert_eq!(usage[1], usage[3]);
    }
}

fn check_reservations<'d, A>(app: A)
where
    A: app::Application<
            'd,
            machine::Connection<
                ServerRole,
                section::Plan,
                resident::Shared<'d>,
                rules::RejectPush,
                event::Ignore,
            >,
            (),
            Build = state::Error,
        >,
{
    use app::Capacity;
    use o3::depot::capacity::Count;
    use state::Error;

    let (result, usage) = Tracker::measure(|| app.state(Capacity::new(Count::new(2), 0)));
    let mismatch = result.err().expect("foreign call extent");
    assert!(matches!(mismatch, Error::Extent { requested, capacity: 1 } if requested.get() == 2));
    assert_eq!(usage, [0; 4]);
    let first = app
        .state(Capacity::new(Count::new(1), 0))
        .expect("rejected reservation returns to its bank");
    let (result, usage) = Tracker::measure(|| app.state(Capacity::new(Count::new(1), 0)));
    let full = result.err().expect("resident remains reserved");
    assert!(matches!(full, Error::Connections { capacity } if capacity.get() == 1));
    assert_eq!(usage, [0; 4]);
    drop(first);
    let reused = app
        .state(Capacity::new(Count::new(1), 0))
        .expect("same bank accepts the returned reservation");
    assert!(matches!(A::activity(&reused), schedule::Quiescence::Quiet));
}
