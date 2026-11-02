use dope::core::driver::life::recovery;
pub mod common;

use std::{convert::Infallible, io::Read, net::TcpStream, sync::mpsc, time::Duration};

use dope::{
    core::driver::route::{FrameworkTag, KeyTag, Tag, kind},
    manifold::timing,
    runtime::{executor::session, topology},
};
use dope_test::{fibers, harness::server::Pair, reclaim::Reports};
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Code, Status,
    message::Compression,
    server::{Bank, Plan, config::Limits, mode},
};
use sark_h2::{
    ClientRole,
    conn::{event, machine::Connection},
    server::runtime::modes,
};

use crate::common::{
    codec::Bytes,
    grpc::{
        session::fairness::{BURST, CHECK, Client as _, Event, FLOOD, Fairness, PROBE, Trace},
        settings::Settings,
    },
};

#[test]
fn event_budget() {
    let harness = Pair::bind().expect("gRPC fairness harness");
    let bind = harness.addr();
    let (first, started) = mpsc::sync_channel(0);
    let (queued, ready) = mpsc::sync_channel(0);

    harness
        .run(
            move |context, listener| {
                let trace = Trace::default();
                let settings = Settings::new(bind, Limits::default());
                let bank = Bank::try_new(settings.connections.count(), settings.limits)
                    .expect("gRPC readiness bank");
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
                let mut reports =
                    Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                let mut finish = session::Completion::new();
                prepared.enter_with(
                    context,
                    |worker| {
                        worker.lend(|plan, stage| {
                            plan.install(stage, |host, stage| {
                                stage.with_app(&mut finish, host, |mut app| {
                                    assert_eq!(fibers::TEST.try_wait(&mut app, &trace, 1).expect("progress drive"), fibers::Wait::Reached);
                                    let events = trace.events.take();
                                    let is_first = matches!(events.as_slice(), [Event::First]);
                                    trace.events.set(events);
                                    assert!(is_first);
                                    assert!(
                                        0 < trace.flood.get() && trace.flood.get() < BURST,
                                        "the first semantic turn drained the flood"
                                    );
                                    first.send(()).expect("first semantic yield");
                                    ready
                                        .recv_timeout(Duration::from_secs(2))
                                        .expect("probe queued before semantic resume");
                                    app.run().inspect(|_| {
                                        let events = trace.events.take();
                                        let observed = events.clone();
                                        trace.events.set(events);
                                        assert!(matches!(observed.as_slice(), [Event::First, Event::Probe(seen), Event::Done(total)] if *seen < BURST && *total == BURST), "unfair or repeated events: {observed:?}");
                                        assert_eq!(trace.flood.get(), BURST, "flood callback count");
                                    })
                                })
                            })
                            .map_err(topology::Failure::Install)?
                            .map_err(topology::Failure::Run)?
                            .map_err(|cause| topology::Failure::Run(session::Error::Drive(cause)))
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |result| {
                        (match result {
                            Ok(result) => result.map_err(topology::RunError::Failure),
                            Err(error) => Err(topology::RunError::from(error)),
                        }).as_ref().map(|_| ()).map_err(topology::RunError::cause).expect("gRPC runtime")
                    },
                );
                assert!(reports.events().expect("retirement owners").is_empty(), "retirement: {reports:?}");
                Ok::<(), Infallible>(())
            },
            move |address| {
                let flood_io = TcpStream::connect(address).expect("connect flood peer");
                let probe_io = TcpStream::connect(address).expect("connect probe peer");
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
                    .request(Fairness::try_headers(FLOOD).expect("field fixture"), Boundary::More)
                    .expect("start flood stream");
                let fbody = Bytes::try_frames(BURST, 0, Compression::Plain, 0)
                    .expect("frame flood messages");
                assert_eq!(flood_h2.try_send_all(flood, &fbody).expect("gRPC DATA"), fbody.len(), "gRPC DATA write stalled");
                let probe = probe_h2
                    .streams()
                    .headers()
                    .request(Fairness::try_headers(CHECK).expect("field fixture"), Boundary::More)
                    .expect("start probe stream");
                let mut pbody = Vec::new();
                Bytes::try_frame(Compression::Plain, PROBE, &mut pbody)
                    .expect("frame probe message");
                assert_eq!(probe_h2.try_send_all(probe, &pbody).expect("gRPC DATA"), pbody.len(), "gRPC DATA write stalled");

                flood_h2.try_flush(&flood_io).expect("write frames");
                started
                    .recv_timeout(Duration::from_secs(2))
                    .expect("first semantic yield");
                probe_h2.try_flush(&probe_io).expect("write frames");
                queued.send(()).expect("probe wire queued");

                for (mut socket, client, stream, echo) in [
                    (&flood_io, &mut flood_h2, flood, false),
                    (&probe_io, &mut probe_h2, probe, true),
                ] {
                    let mut body = Vec::new();
                    let mut done = false;
                    let mut input = [0; 4096];
                    while !done {
                        let read = socket.read(&mut input).expect("read reply");
                        assert_ne!(read, 0, "reply closed before terminal status");
                        client
                            .ingress()
                            .ingest(&input[..read])
                            .expect("reply frames");
                        while let Some(item) = client.ingress().poll_event() {
                            assert!(
                                !matches!(&item, event::Event::StreamReset { .. }),
                                "reply reset"
                            );
                            match item {
                                event::Event::Data {
                                    stream_id: key,
                                    data,
                                    ..
                                } => {
                                    assert_eq!(key.id(), stream);
                                    data.for_each(|part| body.extend_from_slice(part));
                                }
                                event::Event::Headers {
                                    stream_id: key,
                                    head,
                                    boundary: Boundary::End,
                                    ..
                                } => {
                                    assert_eq!(key.id(), stream);
                                    assert!(!done, "terminal reply repeated");
                                    assert_eq!(
                                        Status::from_fields(head.fields())
                                            .expect("reply status")
                                            .code(),
                                        Code::Ok
                                    );
                                    done = true;
                                }
                                _ => {}
                            }
                        }
                        client.try_flush(socket).expect("write frames");
                    }
                    let msgs = Bytes::try_messages(&body).expect("reply messages");
                    if echo {
                        assert_eq!(msgs.len(), 1);
                        assert_eq!(msgs[0], PROBE);
                    } else {
                        assert!(msgs.is_empty());
                    }
                }
            },
        )
        .expect("gRPC admitted semantic E2E");
}

#[test]
fn stream_turns() {
    let harness = Pair::bind().expect("gRPC stream fairness harness");
    let bind = harness.addr();
    let (queued, ready) = mpsc::sync_channel(0);

    harness
        .run(
            move |context, listener| {
                let trace = Trace::default();
                let settings = Settings::new(bind, Limits::default());
                let bank = Bank::try_new(settings.connections.count(), settings.limits)
                    .expect("gRPC readiness bank");
                let prepared = settings
                    .h2::<KeyTag<0>>()
                    .expect("H2 server plan")
                    .with_bound(listener)
                    .with_plain::<_, modes::Serve<Infallible>>(Plan::new(
                        Fairness {
                            trace: &trace,
                            first: false,
                        },
                        &bank,
                        mode::Direct,
                    ))
                    .for_install::<timing::Throughput>()
                    .expect("server resources");
                let mut reports =
                    Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                let mut finish = session::Completion::new();
                prepared.enter_with(
                    context,
                    |worker| {
                        worker.lend(|plan, stage| {
                            plan.install(stage, |host, stage| {
                                stage.with_app(&mut finish, host, |app| {
                                    ready
                                        .recv_timeout(Duration::from_secs(2))
                                        .expect("both streams queued before drive");
                                    app.run().inspect(|_| {
                                        let events = trace.events.take();
                                        let observed = events.clone();
                                        trace.events.set(events);
                                        assert!(matches!(observed.as_slice(), [Event::Probe(seen), Event::Done(total)] if *seen < BURST && *total == BURST), "unfair or repeated events: {observed:?}");
                                        assert_eq!(trace.flood.get(), BURST, "flood callback count");
                                    })
                                })
                            })
                            .map_err(topology::Failure::Install)?
                            .map_err(topology::Failure::Run)?
                            .map_err(|cause| topology::Failure::Run(session::Error::Drive(cause)))
                        })
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |result| {
                        (match result {
                            Ok(result) => result.map_err(topology::RunError::Failure),
                            Err(error) => Err(topology::RunError::from(error)),
                        }).as_ref().map(|_| ()).map_err(topology::RunError::cause).expect("gRPC runtime")
                    },
                );
                assert!(reports.events().expect("retirement owners").is_empty(), "retirement: {reports:?}");
                Ok::<(), Infallible>(())
            },
            move |address| {
                let mut socket = TcpStream::connect(address).expect("connect multiplexed peer");
                socket.set_nodelay(true).expect("multiplexed TCP_NODELAY");
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("reply deadline");
                socket
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .expect("write deadline");
                let mut client =
                    Connection::<ClientRole>::try_new().expect("multiplexed H2 client");
                let flood = client
                    .streams()
                    .headers()
                    .request(Fairness::try_headers(FLOOD).expect("field fixture"), Boundary::More)
                    .expect("start multiplexed flood");
                let fbody = Bytes::try_frames(BURST, 0, Compression::Plain, 0)
                    .expect("frame flood messages");
                assert_eq!(client.try_send_all(flood, &fbody).expect("gRPC DATA"), fbody.len(), "gRPC DATA write stalled");
                let probe = client
                    .streams()
                    .headers()
                    .request(Fairness::try_headers(CHECK).expect("field fixture"), Boundary::More)
                    .expect("start multiplexed probe");
                let mut pbody = Vec::new();
                Bytes::try_frame(Compression::Plain, PROBE, &mut pbody)
                    .expect("frame probe message");
                assert_eq!(client.try_send_all(probe, &pbody).expect("gRPC DATA"), pbody.len(), "gRPC DATA write stalled");
                client.try_flush(&socket).expect("write frames");
                queued.send(()).expect("both stream wires queued");

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
                    while let Some(item) = client.ingress().poll_event() {
                        assert!(
                            !matches!(&item, event::Event::StreamReset { .. }),
                            "reply reset"
                        );
                        match item {
                            event::Event::Data {
                                stream_id: key,
                                data,
                                ..
                            } => {
                                let index = ids
                                    .iter()
                                    .position(|id| *id == key.id())
                                    .expect("reply stream");
                                data.for_each(|part| bodies[index].extend_from_slice(part));
                            }
                            event::Event::Headers {
                                stream_id: key,
                                head,
                                boundary: Boundary::End,
                                ..
                            } => {
                                let index = ids
                                    .iter()
                                    .position(|id| *id == key.id())
                                    .expect("reply stream");
                                assert!(!done[index], "terminal reply repeated");
                                assert_eq!(
                                    Status::from_fields(head.fields())
                                        .expect("reply status")
                                        .code(),
                                    Code::Ok
                                );
                                done[index] = true;
                            }
                            _ => {}
                        }
                    }
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
