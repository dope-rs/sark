use std::{
    cell::{Cell, RefCell},
    net::SocketAddr,
};

use dope::{
    core::{driver::settings, io::socket::Backlog},
    manifold,
    manifold::{app::env::quota, timing},
    runtime::topology,
};
use dope_test::alloc::Tracker;
use sark_ws::server::{Config, Message, Plan};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn plan_borrows_handler() {
    let count = Cell::new(0_usize);
    let failures = RefCell::new(Vec::new());
    let mut path = String::with_capacity(32);
    path.push('/');
    let layout = {
        let prepared = Plan::new(Config {
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            max_connections: 1,
            per_ip: Some(quota::PerIp::fixed::<1>()),
            backlog: Backlog::fixed::<1>(),
            path,
            max_frame_payload: 1024,
            max_message_size: 16 * 1024 * 1024,
        });
        assert!(prepared.is_ok(), "nonzero connection capacity");
        let prepared = prepared.expect("nonzero connection capacity");
        let (root, allocs) = Tracker::usage(|| {
            Tracker::fail_after(0, || {
                prepared.with_reader(
                    |message| {
                        let bytes = match message {
                            Message::Text(text) => text.len(),
                            Message::Binary(bytes) => bytes.len(),
                        };
                        count.set(bytes);
                    },
                    |error| failures.borrow_mut().push(error),
                )
            })
        });
        assert_eq!(allocs, (0, 0));
        let planned = topology::Plan::new((), root).for_install::<timing::Throughput>();
        assert!(
            planned.is_ok(),
            "listener resources fit the throughput profile"
        );
        let plan = planned.expect("listener resources fit the throughput profile");

        assert_eq!(count.get(), 0);
        plan.layout()
    };
    assert!(
        (layout.queue_layout()) == (<manifold::timing::Throughput as settings::Profile>::QUEUES)
    );
    assert!(
        failures.borrow().is_empty(),
        "reader failures: {:?}",
        failures.borrow()
    );
    count.set(9);
    assert_eq!(count.get(), 9);
}
