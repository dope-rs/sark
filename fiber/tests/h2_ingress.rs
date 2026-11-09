pub mod common;

use std::{cell::Cell, fmt, net::SocketAddr, time::Duration};

use common::h2::recover;
use common::h2::{
    apps::{DUPLEX_LAYOUT, H2TaskApp, Layout as _},
    events::{Count, Log},
    facts::{Ingress, ROUTE, State},
    peer::{Peer, Sender},
    server::{CONFIG, Request, Server},
};
use dope::{
    core::{
        driver::{
            life::recovery,
            route::{FrameworkTag, KeyTag, Tag, kind},
        },
        io::socket::Listener,
    },
    manifold::{app::Host, timing},
    net::wire,
    runtime::{executor::session, topology},
};
use dope_fiber::task::storage::scope::Brand;
use dope_test::{fibers, reclaim::Reports};
use sark_core::manifold::stream::Boundary;
use sark_h2::server::task::timers;

#[test]
fn ingress_yields() {
    const BURST: usize = 256;
    let listener = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("listener");
    let address = listener.local_addr();
    let server = Server::new(
        CONFIG,
        8,
        State {
            ingress: Some(Ingress {
                flood_started: Cell::new(0),
                probe_seen_at: Cell::new(usize::MAX),
            }),
            ..State::default()
        },
    );
    let plan = server.try_plan(address).expect("plan");
    let prepared = topology::Plan::new((), plan.with_bound(listener))
        .for_receive::<timing::Throughput>()
        .expect("resources");
    let mut reports = Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let facts = Cell::new(Vec::new());
    let mut done = session::Completion::new();
    prepared.enter_with(
        (),
        |worker| {
            worker.lend(|plan, stage| {
                Brand::with(|brand| {
                    let router = H2TaskApp::multiplexed::<wire::Identity, KeyTag<ROUTE>, _>(
                        brand,
                        &server.state,
                        sark_fiber::Config::new(8),
                    );
                    server.state.layout.set(Some(router.layout()));
                    let wire =
                        wire::domain::Local::<wire::Identity, KeyTag<ROUTE>>::new(wire::Identity);
                    plan.tasks(router, 8)
                        .observe(Log(&facts, |cause: &dyn fmt::Debug| {
                            eprintln!("lifecycle diagnostic: {cause:?}");
                        }))
                        .try_install(stage, wire, timers::Disabled, |listener, _wait, stage| {
                            stage
                                .with_quiesce(
                                    &mut done,
                                    Host::new(listener),
                                    Duration::from_secs(5),
                                    |mut app| {
                                        let (mut flood_io, mut flood) =
                                            Peer::try_connect(address, Duration::from_secs(5))
                                                .expect("H2 peer connection");
                                        let (mut probe_io, mut probe) =
                                            Peer::try_connect(address, Duration::from_secs(5))
                                                .expect("H2 peer connection");
                                        for io in [&flood_io, &probe_io] {
                                            io.set_write_timeout(Some(Duration::from_secs(5)))
                                                .expect("peer write bound");
                                        }
                                        for _ in 0..BURST {
                                            let stream = flood
                                                .streams()
                                                .headers()
                                                .request(
                                                    Request::new(b"/ingress-flood")
                                                        .try_headers()
                                                        .expect("fields"),
                                                    Boundary::More,
                                                )
                                                .expect("flood request");
                                            Sender::new(&mut flood)
                                                .try_send(stream, b"flood", Boundary::End)
                                                .expect("H2 peer send");
                                        }
                                        probe
                                            .streams()
                                            .headers()
                                            .request(
                                                Request::new(b"/ingress-probe")
                                                    .try_headers()
                                                    .expect("fields"),
                                                Boundary::End,
                                            )
                                            .expect("probe request");
                                        Peer::new(&mut flood_io, &mut flood)
                                            .try_flush()
                                            .expect("H2 peer flush");
                                        Peer::new(&mut probe_io, &mut probe)
                                            .try_flush()
                                            .expect("H2 peer flush");
                                        // Both connections have queued input before the first drive.
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(
                                                    &mut app,
                                                    &Count {
                                                        facts: &facts,
                                                        event: "response-committed"
                                                    },
                                                    (BURST + 1) as u32,
                                                )
                                                .expect("H2 ingress progress"),
                                            fibers::Wait::Reached
                                        );
                                        drop((flood_io, probe_io));
                                        assert_eq!(
                                            fibers::TEST
                                                .try_wait(
                                                    &mut app,
                                                    &Count {
                                                        facts: &facts,
                                                        event: "session-closed"
                                                    },
                                                    2,
                                                )
                                                .expect("H2 retirement"),
                                            fibers::Wait::Reached
                                        );
                                        let ingress =
                                            server.state.ingress.as_ref().expect("ingress fixture");
                                        assert!(ingress.probe_seen_at.get() < BURST);
                                        assert_eq!(ingress.flood_started.get(), BURST);
                                    },
                                )
                                .expect("runtime");
                        })
                        .expect("install");
                })
            })
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |result| {
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("H2 topology");
        },
    );
    assert_eq!(server.state.observed(), (false, None, Some(DUPLEX_LAYOUT)));
    assert!(
        reports.events().expect("H2 reclaim ownership").is_empty(),
        "retirement: {reports:?}"
    );
}
