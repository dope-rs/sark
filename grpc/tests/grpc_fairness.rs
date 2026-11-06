use dope::core::driver::life::recovery;
pub mod common;

use std::{convert::Infallible, io::Read, net::TcpStream, time::Duration};

use dope::{
    core::{
        driver::route::{FrameworkTag, KeyTag, Tag, kind},
        io::socket::Listener,
    },
    manifold::timing,
    runtime::executor::session,
};
use dope_test::{
    fibers,
    harness::{scenario::process::Deadline, server::Pair},
    reclaim::Reports,
};
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    message::Compression,
    server::{
        Bank, Plan,
        config::{Limits, Output},
        mode,
    },
};
use sark_h2::{
    ClientRole,
    conn::{DataOutcome, config, machine::Connection},
    frame::numbers::ErrorCode,
    server::runtime::modes,
};

use crate::common::{
    codec::Bytes,
    grpc::{
        call::egress::capacity::try_window,
        session::fairness::{
            BURST, CHECK, Client as _, Event, FLOOD, Fairness, HELD, OTHER_FLOOD, OUTPUT_BYTES,
            OutputFairness, OutputTrace, PROBE, Trace, assert_replies,
        },
        settings::Settings,
        wire::Peer,
    },
};

#[test]
fn protocol_failure_stops_before_finish() {
    use std::{cell::Cell, num::NonZero};

    use dope::core::driver::{
        route::{Epoch, indices::SlotIndex},
        settings,
    };
    use dope_test::scope::Driver;
    use o3::buffer::resident;
    use sark_core::manifold::{schedule, server::app};
    use sark_grpc::protocol::section;
    use sark_h2::{ServerRole, conn::Error};

    let config = settings::Config::new(
        settings::QueueLayout::fixed::<32, 32>(),
        settings::Schedule::fixed::<1, 0>(),
        settings::FileSlots::fixed::<0, 1>(),
        settings::Receive::fixed::<2, 4096>(),
        settings::Domain::ONE,
        settings::Progress::Prompt,
    );
    Driver::new(config).with_scope(
        |scope| {
            let (_, mut context, mut scheduler) = scope.split();
            let plan = config::Validated::<ServerRole>::new(config::Config {
                stream_capacity: 1,
                ..Default::default()
            })
            .expect("one stream");
            let budget = context
                .storage
                .allocator()
                .try_budget(plan.inbound_capacity())
                .expect("retained ingress");
            let driver = context.driver_ref();
            let ready = driver
                .ready()
                .make_slot(
                    driver
                        .targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("ready target");
            let bank = Bank::try_new(
                NonZero::<u32>::MIN,
                Limits::default().try_calls(1).expect("one call"),
            )
            .expect("bank");
            let calls = Cell::new(0);
            let mut service =
                mode::Direct.build(common::grpc::call::borrowed::Borrowed(&calls), &bank);
            let mut server =
                Connection::<ServerRole, section::Plan, resident::Shared<'_>>::from_plan(plan)
                    .expect("server");
            server
                .ingress()
                .try_bind(budget)
                .expect("ingress ownership");
            let mut state = app::Application::<
                Connection<ServerRole, section::Plan, resident::Shared<'_>>,
                (),
            >::state(
                &service,
                app::Capacity::new(plan.stream_count(), plan.data_events().raw()),
            )
            .expect("service state");
            let mut client = Connection::<ClientRole>::try_new().expect("client");
            client
                .streams()
                .headers()
                .request(
                    Fairness::try_headers(CHECK).expect("headers"),
                    Boundary::End,
                )
                .expect("request");
            while !client.output().is_empty() {
                let bytes = client.output().first();
                let count = bytes.len();
                server.ingress().ingest(bytes).expect("wire ingress");
                client.transfer().drain(count);
            }
            let active = scheduler.begin();
            let mut turn =
                app::Turn::new((), active.turn().app(), ready.target(), &mut context);
            let mut consumed = 0;
            let mut finished = false;
            let result = app::Application::drive(
                &mut service,
                &mut state,
                &mut server,
                &mut turn,
                schedule::Egress::Unchanged,
                &mut (),
                |_, conn, drain, _| {
                    let _event = drain.next(conn).expect("first queued event");
                    consumed += 1;
                    Err(Error::CriticalDeadline)
                },
                |_, _| {},
                |_, _| finished = true,
                |_, error| panic!("unexpected report: {error:?}"),
            );
            assert!(matches!(
                result,
                Err(app::Error::Connection(Error::CriticalDeadline))
            ));
            assert_eq!(consumed, 1);
            assert!(!finished);
            assert!(server.ingress().has_event(), "next event remains queued");
            assert_eq!(calls.get(), 0);
            app::Application::close(&mut service, &mut state, &mut server, ());
        },
        recovery::Policy::new(
            |event| panic!("unexpected native event: {event:?}"),
            common::recover,
        ),
        |result| {
            result
                .as_ref()
                .map_err(|error| error.cause())
                .expect("driver scope");
        },
    );
}

#[test]
fn event_budget() {
    Deadline::with(Duration::from_secs(30), |_| {
    let listener = Listener::bind(([127, 0, 0, 1], 0).into()).expect("gRPC fairness server");
    let bind = listener.local_addr();
    let flood_io =
        TcpStream::connect_timeout(&bind, Duration::from_secs(2)).expect("connect flood peer");
    let probe_io =
        TcpStream::connect_timeout(&bind, Duration::from_secs(2)).expect("connect probe peer");
    for socket in [&flood_io, &probe_io] {
        socket.set_nodelay(true).expect("TCP_NODELAY");
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("reply deadline");
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .expect("write deadline");
    }
    let mut flood_h2 = Connection::<ClientRole>::try_new().expect("flood H2 client");
    let mut probe_h2 = Connection::<ClientRole>::try_new().expect("probe H2 client");
    let flood = flood_h2
        .streams()
        .headers()
        .request(
            Fairness::try_headers(FLOOD).expect("field fixture"),
            Boundary::More,
        )
        .expect("start flood stream");
    let fbody = Bytes::try_frames(BURST, 0, Compression::Plain, 0).expect("frame flood messages");
    assert_eq!(
        flood_h2.try_send_all(flood, &fbody).expect("gRPC DATA"),
        fbody.len(),
        "gRPC DATA write stalled"
    );
    let probe = probe_h2
        .streams()
        .headers()
        .request(
            Fairness::try_headers(CHECK).expect("field fixture"),
            Boundary::More,
        )
        .expect("start probe stream");
    let mut pbody = Vec::new();
    Bytes::try_frame(Compression::Plain, PROBE, &mut pbody).expect("frame probe message");
    assert_eq!(
        probe_h2.try_send_all(probe, &pbody).expect("gRPC DATA"),
        pbody.len(),
        "gRPC DATA write stalled"
    );
    let trace = Trace::default();
    let settings = Settings::new(bind, Limits::default());
    let bank =
        Bank::try_new(settings.connections.count(), settings.limits).expect("gRPC readiness bank");
    let prepared = settings
        .h2::<KeyTag<0>>()
        .expect("H2 server plan")
        .with_bound(listener)
        .with_plain::<_, modes::Serve<Infallible>>(Plan::new(
            Fairness {
                trace: &trace,
                first: true,
            },
            &bank,
            mode::Direct,
        ))
        .for_install::<timing::Throughput>()
        .expect("server resources");
    let mut reports = Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
    let mut finish = session::Completion::new();
    prepared.with_local(
        |worker| {
            worker.lend(|plan, stage| {
                plan.install(stage, |host, stage| {
                    stage.with_app(&mut finish, host, |mut app| {
                        flood_h2.try_flush(&flood_io).expect("write frames");
                        assert_eq!(
                            fibers::TEST
                                .try_wait(&mut app, &trace, 1)
                                .expect("progress drive"),
                            fibers::Wait::Reached
                        );
                        let events = trace.events.take();
                        let is_first = matches!(events.as_slice(), [Event::First]);
                        trace.events.set(events);
                        assert!(is_first);
                        assert!(
                            0 < trace.flood.get() && trace.flood.get() < BURST,
                            "the first semantic turn drained the flood"
                        );
                        probe_h2.try_flush(&probe_io).expect("write frames");
                        assert_eq!(
                            fibers::TEST
                                .try_wait(&mut app, &trace.finished.handle(), 2)
                                .expect("both replies finished"),
                            fibers::Wait::Reached
                        );
                        let events = trace.events.take();
                        assert!(matches!(events.as_slice(), [Event::First, Event::Probe(seen), Event::Done(total)] if *seen < BURST && *total == BURST), "unfair or repeated events: {events:?}");
                        trace.events.set(events);
                        assert_eq!(trace.flood.get(), BURST, "flood callback count");
                    })
                })
            })
        },
        recovery::Policy::new(|event| reports.push(event), common::recover),
        |result| match result {
            Ok(Ok(Ok(()))) => (),
            Ok(Ok(Err(error))) => panic!("gRPC session: {:?}", error.cause()),
            Ok(Err(error)) => panic!("gRPC install: {error:?}"),
            Err(error) => panic!(
                "gRPC runtime: {:?}",
                error.kind().map(|output| {
                    output.as_ref().map(|session| {
                        session.as_ref().map(|_| ()).map_err(session::Error::cause)
                    })
                })
            ),
        },
    );
    assert!(
        reports.events().expect("retirement owners").is_empty(),
        "retirement: {reports:?}"
    );
    for (mut socket, client, stream, echo) in [
        (&flood_io, &mut flood_h2, flood, false),
        (&probe_io, &mut probe_h2, probe, true),
    ] {
        let mut bodies = [Vec::new()];
        let mut done = [false];
        let mut input = [0; 4096];
        while !done[0] {
            let read = socket.read(&mut input).expect("read reply");
            assert_ne!(read, 0, "reply closed before terminal status");
            client
                .ingress()
                .ingest(&input[..read])
                .expect("reply frames");
            assert_replies(client, &[stream], &mut bodies, &mut done);
        }
        let msgs = Bytes::try_messages(&bodies[0]).expect("reply messages");
        if echo {
            assert_eq!(msgs.len(), 1);
            assert_eq!(msgs[0], PROBE);
        } else {
            assert!(msgs.is_empty());
        }
    }
    })
    .expect("fairness deadline");
}

#[test]
fn output_retries() {
    for competing in [false, true] {
        Deadline::with(Duration::from_secs(30), |_| {
        let listener = Listener::bind(([127, 0, 0, 1], 0).into()).expect("output fairness server");
        let address = listener.local_addr();
        let limits = Limits::default().with_output(
            Output::try_new(2 * BURST + 3, OUTPUT_BYTES)
                .expect("finite floods and probes capacity"),
        );
        let settings = Settings::new(address, limits);
        let trace = OutputTrace::new(competing);
        let bank = Bank::try_new(settings.connections.count(), limits).expect("gRPC bank");
        let prepared = settings
            .h2::<KeyTag<0>>()
            .expect("H2 plan")
            .with_bound(listener)
            .with_plain::<_, modes::Serve<Infallible>>(Plan::new(
                OutputFairness(&trace),
                &bank,
                mode::Direct,
            ))
            .for_install::<timing::Throughput>()
            .expect("server resources");
        let mut reports =
            Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
        let mut finish = session::Completion::new();
        prepared.with_local(
            |worker| {
                worker.lend(|plan, stage| {
                    plan.install(stage, |host, stage| {
                        stage.with_app(&mut finish, host, |mut app| {
                            let mut peer = Peer::try_config(
                                address,
                                config::Config {
                                    local_settings: config::Settings {
                                        initial_window_size: 0,
                                        ..config::Settings::DEFAULT
                                    },
                                    ..config::Config::default()
                                },
                            )
                            .expect("zero-window peer");
                            let mut floods = Vec::new();
                            if competing {
                                for path in [FLOOD, OTHER_FLOOD] {
                                    floods.push(peer.client.streams().headers().request(
                                        Fairness::try_headers(path)
                                            .expect("parked producer headers"),
                                        Boundary::More,
                                    ).expect("parked producer"));
                                }
                            }
                            let held = peer
                                .client
                                .streams()
                                .headers()
                                .request(
                                    Fairness::try_headers(HELD).expect("held headers"),
                                    Boundary::More,
                                )
                                .expect("held response");
                            for _ in 0..if competing { 2 } else { 1 } {
                                peer.client
                                    .streams()
                                    .headers()
                                    .request(
                                        Fairness::try_headers(CHECK).expect("probe headers"),
                                        Boundary::More,
                                    )
                                    .expect("blocked response");
                            }
                            peer.try_flush().expect("held and blocked requests");
                            assert_eq!(
                                fibers::TEST
                                    .try_wait(&mut app, &trace, 1)
                                    .expect("blocked probe"),
                                fibers::Wait::Reached
                            );
                            assert_eq!(trace.blocked.get(), if competing { 2 } else { 1 });
                            assert!(!trace.failed.get());
                            trace.armed.set(true);
                            if competing {
                                let body = Bytes::try_frames(1, 0, Compression::Plain, 0)
                                    .expect("producer wake");
                                for id in floods {
                                    assert!(matches!(
                                        peer.client.streams().try_send(id, &body, Boundary::More).expect("wake producer"),
                                        DataOutcome::Sent(bytes) if bytes.get() == body.len()
                                    ));
                                }
                            } else {
                                peer.client
                                    .streams()
                                    .headers()
                                    .request(
                                        Fairness::try_headers(FLOOD).expect("flood headers"),
                                        Boundary::More,
                                    )
                                    .expect("finite ready output");
                            }
                            let held = peer
                                .client
                                .status()
                                .stream_key(held)
                                .expect("held stream key");
                            peer.client
                                .streams()
                                .try_cancel(held, ErrorCode::Cancel)
                                .expect("release held response");
                            peer.try_flush().expect("ready output and capacity release");
                            try_window(&mut peer.socket, 0)
                                .expect("egress release acknowledgement");
                            assert_eq!(
                                fibers::TEST
                                    .try_wait(&mut app, &trace, 2)
                                    .expect("finite output drive"),
                                fibers::Wait::Reached
                            );
                            assert!(
                                !trace.failed.get(),
                                "the finite flood must fit alongside the probe"
                            );
                            assert!(
                                !trace.unfair.get(),
                                "blocked retry displaced the ready cursor: {:?}",
                                trace.produced.each_ref().map(|count| count.get())
                            );
                            let seen = trace.resumed.get().expect("released probe resumed");
                            assert!(
                                seen < BURST,
                                "released probe waited for all {seen} of {BURST} ready outputs"
                            );
                            drop(peer);
                        })
                    })
                })
            },
            recovery::Policy::new(|event| reports.push(event), common::recover),
            |result| match result {
                Ok(Ok(Ok(()))) => (),
                Ok(Ok(Err(error))) => panic!("gRPC session: {:?}", error.cause()),
                Ok(Err(error)) => panic!("gRPC install: {error:?}"),
                Err(error) => panic!(
                    "gRPC runtime: {:?}",
                    error.kind().map(|output| {
                        output.as_ref().map(|session| {
                            session.as_ref().map(|_| ()).map_err(session::Error::cause)
                        })
                    })
                ),
            },
        );
        assert!(
            reports.events().expect("retirement owners").is_empty(),
            "retirement: {reports:?}"
        );
        })
        .expect("output fairness deadline");
    }
}

#[test]
fn stream_turns() {
    let harness = Pair::bind().expect("gRPC stream fairness harness");
    let bind = harness.addr();
    let mut socket = TcpStream::connect(bind).expect("connect multiplexed peer");
    socket.set_nodelay(true).expect("multiplexed TCP_NODELAY");
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("reply deadline");
    socket
        .set_write_timeout(Some(Duration::from_secs(2)))
        .expect("write deadline");
    let mut client = Connection::<ClientRole>::try_new().expect("multiplexed H2 client");
    let flood = client
        .streams()
        .headers()
        .request(
            Fairness::try_headers(FLOOD).expect("field fixture"),
            Boundary::More,
        )
        .expect("start multiplexed flood");
    let fbody = Bytes::try_frames(BURST, 0, Compression::Plain, 0).expect("frame flood messages");
    assert_eq!(
        client.try_send_all(flood, &fbody).expect("gRPC DATA"),
        fbody.len(),
        "gRPC DATA write stalled"
    );
    let probe = client
        .streams()
        .headers()
        .request(
            Fairness::try_headers(CHECK).expect("field fixture"),
            Boundary::More,
        )
        .expect("start multiplexed probe");
    let mut pbody = Vec::new();
    Bytes::try_frame(Compression::Plain, PROBE, &mut pbody).expect("frame probe message");
    assert_eq!(
        client.try_send_all(probe, &pbody).expect("gRPC DATA"),
        pbody.len(),
        "gRPC DATA write stalled"
    );
    client.try_flush(&socket).expect("write frames");

    harness
        .run(
            move |context, listener| {
                let trace = Trace::default();
                let settings = Settings::new(bind, Limits::default());
                let retired = settings
                    .try_serve_on(
                        Fairness {
                            trace: &trace,
                            first: false,
                        },
                        context,
                        listener,
                        common::recover,
                        |result| result.expect("gRPC runtime"),
                    )
                    .expect("gRPC readiness bank");
                let events = trace.events.take();
                assert!(matches!(events.as_slice(), [Event::Probe(seen), Event::Done(total)] if *seen < BURST && *total == BURST), "unfair or repeated events: {events:?}");
                assert_eq!(trace.flood.get(), BURST, "flood callback count");
                assert!(retired.is_empty(), "retirement: {retired:?}");
                Ok::<(), Infallible>(())
            },
            move |_| {
                let ids = [flood, probe];
                let mut bodies = [Vec::new(), Vec::new()];
                let mut done = [false, false];
                let mut input = [0; 4096];
                while !done.iter().all(|done| *done) {
                    let read = socket.read(&mut input).expect("read replies");
                    assert_ne!(read, 0, "replies closed before terminal statuses");
                    client
                        .ingress()
                        .ingest(&input[..read])
                        .expect("reply frames");
                    assert_replies(&mut client, &ids, &mut bodies, &mut done);
                    client.try_flush(&socket).expect("write frames");
                }
                assert!(bodies[0].is_empty());
                let msgs = Bytes::try_messages(&bodies[1]).expect("probe reply messages");
                assert_eq!(msgs.len(), 1);
                assert_eq!(msgs[0], PROBE);
            },
        )
        .expect("gRPC multiplexed semantic E2E");
}
