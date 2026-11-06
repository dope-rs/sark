pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    fmt,
    io::{Read as _, Write as _},
    time::Duration,
};

use common::h2::recover;
use common::h2::{
    apps::H2TaskApp,
    events::{Credit, CreditLog, Log},
    facts::State,
    peer::{Peer, Sender},
    server::{CONFIG, Request, Server},
};
use dope::{
    core::driver::{
        life::recovery::Policy,
        route::{FrameworkTag, KeyTag, Tag, kind},
        schedule, storage,
    },
    manifold::{app::Host, network::listener::handler::Application, timing},
    net::wire,
    runtime::{executor::session, topology},
};
use dope_fiber::task::storage::scope::Brand;
use dope_test::{alloc::Tracker, harness::server::Pair, reclaim::Reports};
use sark_core::{life::mode::Observed, manifold::stream::Boundary};
use sark_h2::{
    conn::{self, event::Rejected},
    frame::numbers,
    hpack::decode::limit::Admission,
    server::task::timers,
    stream::Id,
};
use sark_http::field::metadata::block::Validation;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn construction_keeps_allocation_owner() {
    for (remaining, expected) in [(2, "Book(Exhausted)"), (1, "Tasks(Exhausted)")] {
        Pair::bind().expect("listener").run(
            |context, listener| {
                let server = Server::new(CONFIG, 1, State::default());
                let plan = server.try_plan(listener.local_addr()).expect("plan").with_bound(listener);
                let prepared = topology::Plan::new((), plan)
                    .for_receive::<timing::Throughput>().expect("resources");
                let events = Cell::new(Vec::new());
                let mut causes = Vec::new();
                let mut done = session::Completion::new();
                prepared.enter_with(
                    context,
                    |worker| worker.lend(|plan, stage| Brand::with(|brand| {
                        let factory = H2TaskApp::multiplexed::<wire::Identity, KeyTag<0>, _>(
                            brand, &server.state, sark_fiber::Config::new(1),
                        );
                        plan.tasks(factory, 1).observe(Log(&events, |cause: &dyn fmt::Debug| {
                            causes.push(format!("{cause:?}"));
                        })).try_install(
                            stage, wire::domain::Local::<wire::Identity, _>::new(wire::Identity),
                            timers::Disabled,
                            |listener, _wait, stage| {
                                let seed = stage.driver().driver_ref().random_seed().map(|words| words);
                                stage.with_quiesce(
                                    &mut done, Host::new(listener), Duration::from_secs(2),
                                    |mut app| {
                                        let (_, allocations) = app.inspect(Host::manifold, |listener| {
                                            // SAFETY: the installed listener structurally pins its application.
                                            let handler = unsafe { listener.map_unchecked(|listener| listener.handler()) };
                                            Tracker::count(|| Application::<KeyTag<0>>::connection(handler, &seed)
                                                .map(drop).expect("connection"))
                                        });
                                        Tracker::fail_after(allocations - remaining, || app.run().map(|_| ()))
                                    },
                                ).expect("runtime").expect("drive");
                            },
                        ).expect("install");
                    })),
                    Policy::new(|_| {}, recover),
                    |result| { result.as_ref().map_err(|error| error.cause()).expect("retirement"); },
                );
                assert_eq!(causes, [expected]);
                Ok::<_, Infallible>(())
            },
            |address| {
                let mut socket = Peer::try_open(address, Duration::from_secs(3)).expect("peer");
                assert_eq!(socket.read(&mut [0]).expect("rejected connection"), 0);
            },
        ).expect("pair");
    }
}

#[test]
fn stream_failure_isolated() {
    use std::rc::Rc;

    use common::route::body::{
        Trace,
        stream::{Failure, StreamFailureDispatch},
    };
    use sark_h2::{conn::event::Event, server::runtime::app::Failure as Report};
    use sark_http::field::{self, Entry, Pseudo, Value};

    Pair::bind()
        .expect("listener")
        .run(
            |context, listener| {
                let plan = Server::new(CONFIG, 2, State::default())
                    .try_plan(listener.local_addr())
                    .expect("plan")
                    .with_bound(listener);
                let prepared = topology::Plan::new((), plan)
                    .for_receive::<timing::Throughput>()
                    .expect("resources");
                let mut reports =
                    Reports::<1>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                let cause = String::from("read-failed");
                let trace = Rc::new(Trace::default());
                let state = Failure {
                    cause: &cause,
                    trace: Rc::clone(&trace),
                };
                let mut diagnostics = Vec::new();
                let mut done = session::Completion::new();
                prepared.enter_with(
                    context,
                    |worker| {
                        worker.lend(|plan, stage| {
                            Brand::with(|brand| {
                                let router = StreamFailureDispatch::multiplexed::<
                                    wire::Identity,
                                    KeyTag<0>,
                                    _,
                                >(
                                    brand, &state, sark_fiber::Config::new(2)
                                );
                                plan.tasks(router, 2)
                                    .report(
                                        |failure: Report<'_, '_, '_, wire::Identity, KeyTag<0>, Infallible, sark_h2::server::task::Build>| {
                                            let Report::Response { id, stream, cause } = failure
                                            else {
                                                panic!("unexpected failure")
                                            };
                                            assert_eq!(id.index().get(), 0);
                                            assert_eq!(stream.id(), Id::FIRST_CLIENT);
                                            diagnostics.push(format!("{cause:?}"));
                                        },
                                    )
                                    .try_install(
                                        stage,
                                        wire::domain::Local::<wire::Identity, _>::new(
                                            wire::Identity,
                                        ),
                                        timers::Disabled,
                                        |listener, _wait, stage| {
                                            stage
                                                .run_quiesce(
                                                    &mut done,
                                                    Host::new(listener),
                                                    Duration::from_secs(2),
                                                )
                                                .expect("runtime")
                                                .expect("drive")
                                        },
                                    )
                                    .expect("install");
                            })
                        })
                    },
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("retirement");
                    },
                );
                assert!(reports.events().expect("reclaimed resources").is_empty());
                assert_eq!(diagnostics, [format!("{cause:?}")]);
                assert_eq!(trace.polls.get(), 4);
                assert_eq!(trace.drops.get(), 2);
                assert!(!trace.moved.get());
                Ok::<_, Infallible>(())
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("peer");
                let streams = [b"/later".as_slice(), b"/next"].map(|path| {
                    let mut fields = Request::new(path).try_headers().expect("fields");
                    fields[0] =
                        Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"GET")));
                    client
                        .streams()
                        .headers()
                        .request(fields, Boundary::End)
                        .expect("request")
                });
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("flush");
                let mut bodies = [Vec::new(), Vec::new()];
                let mut headers = [false; 2];
                let mut done = [false; 2];
                let mut input = [0; 4096];
                while !done.iter().all(|done| *done) {
                    let read = socket.read(&mut input).expect("responses");
                    assert_ne!(read, 0, "connection remains open");
                    client.ingress().ingest(&input[..read]).expect("ingress");
                    while let Some(event) = client.ingress().poll_event() {
                        match event {
                            Event::Headers {
                                stream_id,
                                head,
                                boundary,
                                ..
                            } => {
                                let slot = streams
                                    .iter()
                                    .position(|id| *id == stream_id)
                                    .expect("stream");
                                headers[slot] = head
                                    .fields()
                                    .iter()
                                    .any(|field| field.name == b":status" && field.value == b"200");
                                if boundary == Boundary::End {
                                    assert_eq!(slot, 1, "failed stream cannot end successfully");
                                    done[slot] = true;
                                }
                            }
                            Event::Data {
                                stream_id,
                                data,
                                boundary,
                            } => {
                                let slot = streams
                                    .iter()
                                    .position(|id| *id == stream_id)
                                    .expect("stream");
                                data.for_each(|part| bodies[slot].extend_from_slice(part));
                                if boundary == Boundary::End {
                                    assert_eq!(slot, 1, "failed stream cannot end successfully");
                                    done[slot] = true;
                                }
                            }
                            Event::StreamReset {
                                stream_id, error, ..
                            } => {
                                assert_eq!(stream_id, streams[0]);
                                assert_eq!(error, numbers::ErrorCode::InternalError);
                                assert!(!done[0], "one reset");
                                done[0] = true;
                            }
                            Event::GoAway { .. } => panic!("sibling must survive"),
                            _ => {}
                        }
                    }
                    Peer::new(&mut socket, &mut client)
                        .try_flush()
                        .expect("flush control");
                }
                assert_eq!(headers, [true; 2]);
                assert_eq!(bodies, [b"before-error".to_vec(), b"next".to_vec()]);
                let mut fields = Request::new(b"/later").try_headers().expect("HEAD fields");
                fields[0] =
                    Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"HEAD")));
                let head = client
                    .streams()
                    .headers()
                    .request(fields, Boundary::End)
                    .expect("HEAD");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("flush HEAD");
                let (headers, body) = Peer::new(&mut socket, &mut client)
                    .try_response(head)
                    .expect("reused task slot");
                assert!(
                    headers
                        .iter()
                        .any(|field| field.name == b":status" && field.value == b"200")
                );
                assert!(body.is_empty());
            },
        )
        .expect("pair");
}

#[test]
fn response_header_cause_once() {
    use common::h2::{facts::GuardProbe, server::CONNECT, tls::Rpc};
    use sark_core::life::audit;
    use sark_grpc::server::{Bank, config::Limits, mode};
    use sark_h2::{frame, server::{runtime::app::Failure, task}};

    for mixed in [false, true] {
        Pair::bind().expect("listener").run(
            |context, listener| {
                let server = Server::new(CONFIG, 2, State {
                    guard: Some(GuardProbe::default()),
                    ..State::try_body().expect("response owner")
                });
                let plan = server.try_plan(listener.local_addr()).expect("plan").with_bound(listener)
                    .with_streams(std::num::NonZero::new(2).expect("streams"));
                let bank = Bank::try_new(plan.connections().count(), Limits::default().try_calls(2).expect("calls"))
                    .expect("gRPC bank");
                let prepared = topology::Plan::new(storage::Value::new(bank), plan)
                    .for_install::<timing::Throughput>().expect("resources");
                let mut reports = Reports::<1>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                let events = Cell::new(Vec::new());
                let credit = Cell::new(Credit::default());
                let mut diagnostics = Vec::new();
                let mut done = session::Completion::new();
                prepared.enter_with(
                    context,
                    |worker| worker.lend(|plan, stage| Brand::with(|brand| {
                        let router = H2TaskApp::multiplexed::<wire::Identity, KeyTag<0>, _>(
                            brand, &server.state, sark_fiber::Config::new(2),
                        );
                        let wire = wire::domain::Local::<wire::Identity, KeyTag<0>>::new(wire::Identity);
                        if mixed {
                            let bank = stage.storage();
                            plan.tasks((), 2).report(
                                |failure: Failure<'_, '_, '_, wire::Identity, KeyTag<0>,
                                    sark_grpc::server::runtime::run::Failure,
                                    task::Build<sark_grpc::server::runtime::state::Error>>| {
                                    let Failure::Response { id, stream, cause } = failure else {
                                        panic!("unexpected failure")
                                    };
                                    assert_eq!(id.index().get(), 0);
                                    assert_eq!(stream.id().into_u32(), 1 + 2 * diagnostics.len() as u32);
                                    diagnostics.push(format!("{cause:?}"));
                                },
                            ).prepare_in::<wire::Identity, _, _, _, _, _, _>(stage, wire, |prepared, _wait, stage| {
                                let listener = prepared.build_with(
                                    router, task::timers::Disabled, mode::Direct.build(Rpc, bank), &mut stage.driver(),
                                ).expect("mixed listener");
                                stage.run_quiesce(&mut done, Host::new(listener), Duration::from_secs(2))
                                    .expect("runtime").expect("drive");
                            }).expect("prepare");
                        } else {
                            plan.tasks(router, 2).observe(CreditLog {
                                log: Log(&events, |cause: &dyn fmt::Debug| diagnostics.push(format!("{cause:?}"))),
                                state: &credit,
                            }).report(|_: Failure<'_, '_, '_, wire::Identity, KeyTag<0>, Infallible, task::Build>| {
                                panic!("native observer owns response diagnostics")
                            }).try_install(stage, wire, timers::Disabled, |listener, _wait, stage| {
                                stage.run_quiesce(&mut done, Host::new(listener), Duration::from_secs(2))
                                    .expect("runtime").expect("drive");
                            }).expect("install");
                        }
                    })),
                    Policy::new(|event| reports.push(event), recover),
                    |result| { result.as_ref().map_err(|error| error.cause()).expect("retirement"); },
                );
                assert!(reports.events().expect("reclaimed resources").is_empty());
                assert_eq!(server.state.stream.polls.get(), 0);
                assert_eq!(server.state.stream.drops.get(), 1);
                assert!(server.state.body.take().is_none());
                let guard = server.state.guard.as_ref().expect("guard");
                assert_eq!(guard.checks.get(), 1);
                assert_eq!(guard.handlers.get(), 0);
                assert_eq!(server.state.header.get(), Some(*b"retained-after-pseudo-fields"));
                if !mixed {
                    let events = events.into_inner();
                    for stream in events.iter().filter_map(|&(kind, stream)| (kind == "exchange-rejected").then_some(stream)) {
                        assert!(events.iter().filter_map(|&(kind, id)| {
                            (id == stream && matches!(kind, "response-committed" | "terminal" | "exchange-retired"))
                                .then_some(kind)
                        }).eq(["terminal", "exchange-retired"]));
                    }
                    assert_eq!(events.iter().filter(|event| event.0 == "exchange-rejected").count(), 6);
                    assert!(credit.into_inner().audit.expect("final audit").is_zero(
                        audit::Capability::RESPONSE_STAGING_OWNER_BYTES
                            .union(audit::Capability::TRANSPORT_RESPONSE_OWNER_BYTES),
                    ));
                }
                assert_eq!(diagnostics,
                    ["HeaderEncode(List { used: 0, name: 7, value: 3, limit: 1 })"; 6]);
                Ok::<_, Infallible>(())
            },
            |address| {
                let (mut socket, mut client) = Peer::try_connect(address, Duration::from_secs(3)).expect("peer");
                Peer::new(&mut socket, &mut client).try_flush().expect("preface");
                let mut setting = Vec::new();
                frame::controls::Settings::new(frame::Ack::Required, &[0, 6, 0, 0, 0, 1]).expect("header limit")
                    .encode(&mut setting).expect("setting frame");
                socket.write_all(&setting).expect("restrict response headers");
                for path in [Some(b"/quick".as_slice()), Some(b"/stream"), Some(b"/unbounded"), Some(b"/missing"), Some(b"/guarded/body"), None] {
                    let stream = match path {
                        Some(path) => client.streams().headers()
                            .request(Request::new(path).try_headers().expect("fields"), Boundary::End),
                        None => client.streams().headers().request(CONNECT, Boundary::More),
                    }.expect("request");
                    Peer::new(&mut socket, &mut client).try_flush().expect("flush");
                    assert_eq!(Peer::new(&mut socket, &mut client).try_reset(stream).expect("response reset"),
                        numbers::ErrorCode::EnhanceYourCalm);
                }
                setting.clear();
                frame::controls::Settings::new(frame::Ack::Required, &[0, 6, 0, 1, 0, 0]).expect("restore header limit")
                    .encode(&mut setting).expect("setting frame");
                socket.write_all(&setting).expect("restore response headers");
                let next = client.streams().headers()
                    .request(Request::new(b"/quick").try_headers().expect("fields"), Boundary::End).expect("follow-up");
                Peer::new(&mut socket, &mut client).try_flush().expect("flush follow-up");
                assert_eq!(Peer::new(&mut socket, &mut client).try_response(next).expect("surviving connection").1, b"quick");
            },
        ).expect("pair");
    }
}

#[test]
fn preserves_preface_cause() {
    Pair::bind()
        .expect("H2 harness")
        .run(
            |context, listener| {
                let events = Cell::new(Vec::new());
                let mut causes = Vec::new();
                Server::new(CONFIG, 2, State::default())
                    .try_run(
                        context,
                        listener,
                        Observed::new(Log(&events, |cause: &dyn fmt::Debug| {
                            causes.push(format!("{cause:?}"));
                        })),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .expect("H2 setup");
                assert_eq!(
                    causes,
                    ["BadPreface { offset: 0, expected: 80, actual: 88 }"],
                );
                Ok::<_, Infallible>(())
            },
            |address| {
                let mut socket =
                    Peer::try_open(address, Duration::from_secs(3)).expect("H2 peer connection");
                socket
                    .write_all(b"XRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
                    .expect("invalid preface");
                socket.read_to_end(&mut Vec::new()).expect("H2 close");
            },
        )
        .expect("H2 diagnostic runtime");
}

#[test]
fn keeps_queued_rejection() {
    let harness = Pair::bind().expect("H2 harness");
    let (mut socket, mut client) =
        Peer::try_connect(harness.addr(), Duration::from_secs(3)).expect("H2 peer connection");
    let stream = client
        .streams()
        .headers()
        .request(
            Request::new(b"/blocked")
                .try_headers()
                .expect("valid request fields"),
            Boundary::More,
        )
        .expect("start H2 request");
    Peer::new(&mut socket, &mut client)
        .try_faults(stream)
        .expect("queue rejected trailers and fatal headers");
    harness
        .run(
            |context, listener| {
                let events = Cell::new(Vec::new());
                let mut causes = Vec::new();
                Server::new(CONFIG, 2, State::default())
                    .try_run(
                        context,
                        listener,
                        Observed::new(Log(&events, |cause: &dyn fmt::Debug| {
                            causes.push(format!("{cause:?}"));
                        })),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .expect("H2 setup");
                let mut expected = [
                    format!(
                        "{:?}",
                        Rejected {
                            admit: Admission::Admitted,
                            sealed: Validation::Malformed,
                            finished: Validation::Malformed,
                        }
                    ),
                    format!(
                        "{:?}",
                        conn::Error::ParseError(numbers::ParseError::Stream {
                            kind: numbers::Type::Headers,
                            actual: Id::CONNECTION,
                        })
                    ),
                ];
                causes.sort_unstable();
                expected.sort_unstable();
                assert_eq!(causes, expected);
                Ok::<_, Infallible>(())
            },
            |_| {
                socket.read_to_end(&mut Vec::new()).expect("H2 close");
            },
        )
        .expect("H2 diagnostic runtime");
}

#[test]
fn json_failure_precedes_next_data() {
    let harness = Pair::bind().expect("H2 harness");
    let (mut socket, mut client) =
        Peer::try_connect(harness.addr(), Duration::from_secs(3)).expect("H2 peer connection");
    let stream = client
        .streams()
        .headers()
        .request(
            Request::new(b"/large")
                .try_headers()
                .expect("valid request fields"),
            Boundary::More,
        )
        .expect("start JSON request");
    // More DATA than one admitted turn can consume, all queued before the
    // server starts. The first invalid byte must fail before the next push.
    for index in 0..2 * schedule::LIMIT {
        Sender::new(&mut client)
            .try_send(
                stream,
                b"!",
                if index + 1 == 2 * schedule::LIMIT {
                    Boundary::End
                } else {
                    Boundary::More
                },
            )
            .expect("queue JSON DATA");
    }
    Peer::new(&mut socket, &mut client)
        .try_flush()
        .expect("flush queued JSON DATA");
    harness
        .run(
            |context, listener| {
                let events = Cell::new(Vec::new());
                let credit = Cell::new(Credit::default());
                let mut causes = Vec::new();
                Server::new(CONFIG, 2, State::default())
                    .try_run(
                        context,
                        listener,
                        Observed::new(CreditLog {
                            log: Log(&events, |cause: &dyn fmt::Debug| {
                                causes.push(format!("{cause:?}"));
                            }),
                            state: &credit,
                        }),
                        recover,
                        |result| result.expect("H2 execution"),
                    )
                    .expect("H2 setup");
                let credit = credit.into_inner();
                assert_eq!(causes, ["Build(R0002(Parse(Invalid)))"]);
                assert_eq!(credit.reserved, (1, 1));
                assert_eq!(credit.released, credit.reserved);
                assert_eq!(credit.closing, Some((0, 0)));
                assert!(!credit.invalid);
                Ok::<_, Infallible>(())
            },
            |_| {
                assert_eq!(
                    Peer::new(&mut socket, &mut client)
                        .try_reset(stream)
                        .expect("JSON request reset"),
                    numbers::ErrorCode::ProtocolError,
                );
                let followup = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/quick")
                            .try_headers()
                            .expect("valid follow-up fields"),
                        Boundary::End,
                    )
                    .expect("start follow-up request");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("flush follow-up request");
                let (_, bodies) = Peer::new(&mut socket, &mut client)
                    .try_statuses(&[followup], b"200")
                    .expect("follow-up response after JSON reset");
                assert_eq!(bodies, [b"quick".to_vec()]);
            },
        )
        .expect("H2 JSON cleanup runtime");
}
