pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io::{Read, Write},
    net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream},
    time::Duration,
};

use common::{
    h2::{
        events::Log,
        tls::{self, HEADERS, RESPONSE, Shared, State},
    },
    server::{self, recover},
};
use dope::{
    core::{
        driver::{
            life::recovery::Policy,
            route::{FrameworkTag, KeyTag, Tag, kind},
            schedule, storage,
        },
        io::socket::Listener,
    },
    manifold::{
        app::{Host, receive::limits::Budget},
        timing,
    },
    net::wire,
    runtime::{executor::session, topology},
};
use dope_fiber::task::storage::scope::Brand;
use dope_test::{alloc::Tracker, harness::server::Pair, reclaim::Reports};
use dope_tls::{
    rustls::{self, ClientConnection, StreamOwned},
    tls::{Tls, endpoint, roles},
};
use o3::buffer::hold::limits::Bound;
use sark_core::{
    deadline,
    manifold::stream::{Boundary, Section},
};
use sark_grpc::{
    Code as GrpcCode, Status,
    message::{Compression, Frame},
    metadata::Metadata,
    protocol::request,
    server::{
        Bank,
        config::{Limits, Output},
        mode,
    },
};
use sark_h1::server::{date, run};
use sark_h2::{
    ClientRole,
    conn::{DataOutcome, config, event::Event, machine::Connection},
    frame::{self, layout, payload},
    server::runtime::app::Failure,
};
use sark_http::field::{self, Entry, Lower, Value};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn shares_driver_storage_with_h1() {
    for (phase, reject_open, half_close) in [
        (Some(deadline::Phase::Handler), false, false),
        (None, true, false),
        (Some(deadline::Phase::Total), false, true),
        (None, false, true),
    ] {
        let pair = Pair::bind().expect("H1 listener");
        let h2 = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("H2 listener");
        let h2_address = h2.local_addr();
        pair.run(
            move |context, h1| {
                let h1 = server::Plan::new(h1.local_addr(), Duration::from_secs(5))
                    .build()
                    .gated_scoped()
                    .with_bound(h1);
                let baseline = h1.config();
                let h2 = tls::plan(h2).expect("H2 plan").with_config(config::Config {
                    stream_capacity: 1,
                    event_capacity: if phase == Some(deadline::Phase::Total) { 1 } else { schedule::LIMIT + 4 },
                    ..Default::default()
                });
                let owner =
                    wire::domain::Owner::<Tls<roles::Server>, KeyTag<2>>::try_new(h2.connections())
                        .expect("TLS owner");
                let storage = storage::With::new(
                    storage::Value::new(owner),
                    date::Clock::new().expect("H1 Date clock"),
                );
                let prepared = topology::Plan::new(storage, (h1, h2))
                    .with_install(baseline)
                    .expect("combined resource claims");
                let mut reports =
                    Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                let calls = Cell::new(0);
                let opens = Cell::new(0);
                let terminals = Cell::new(0);
                let facts = Cell::new(Vec::new());
                let wire = endpoint::Config::from(
                    tls::server_config(reject_open.then_some(1)).expect("TLS server config"),
                );
                let mut finish = session::Completion::new();
                prepared.enter_with(
                    context,
                    |worker| {
                        worker.lend(|(h1, h2), stage| {
                            let wire = stage.storage().0.bind(wire).expect("TLS binding");
                            Brand::with(|h1_brand| {
                                Brand::with(|h2_brand| {
                                    h1.prepare_in(
                                        stage,
                                        wire::domain::Local::<wire::Identity, _>::new(wire::Identity),
                                        |h1, wait, stage| {
                                            let tasks = h2.tasks((), 1);
                                            let tasks = if phase == Some(deadline::Phase::Total) {
                                                tasks.with_age(sark_core::life::session::Config::try_new(
                                                    Duration::from_secs(1), Duration::from_secs(4),
                                                ).expect("graceful lifetime before Total"))
                                            } else { tasks };
                                            tasks.observe(Log(&facts, |cause: &dyn std::fmt::Debug| {
                                                assert_eq!(phase, Some(deadline::Phase::Handler));
                                                assert_eq!(format!("{cause:?}"), "CriticalDeadline");
                                                let facts = facts.take();
                                                let last = facts.iter().rev().find(|event| event.0 == "head-admitted")
                                                    .expect("final request admitted behind its partial trailer").1;
                                                assert_eq!(facts.iter().filter(|event| event.0 == "head-admitted").count(), 2);
                                                assert!(!facts.iter().any(|event| event.1 == last && event.0 == "input-finished"));
                                                terminals.set(terminals.get() + 1);
                                            })).report(
                                                |failure: Failure<'_, '_, '_, Tls<roles::Server>, KeyTag<2>, Infallible, sark_h2::server::task::Build>| {
                                                    match failure {
                                                        Failure::Open(cause) => {
                                                            let cause = cause.into_cause();
                                                            assert!(matches!(
                                                                cause,
                                                                dope_tls::Error::Tls(rustls::Error::BadMaxFragmentSize)
                                                            ), "{cause:?}");
                                                            opens.set(opens.get() + 1);
                                                        }
                                                        Failure::Wire { id, cause } => {
                                                            assert_eq!(id.index().get(), 0);
                                                            let (cause, closing) = cause.into_parts();
                                                            assert!(matches!(
                                                                &cause,
                                                                dope_tls::Error::Io(error)
                                                                    if error.kind() == std::io::ErrorKind::UnexpectedEof
                                                            ), "{cause:?}");
                                                            assert!(closing.is_none(), "{closing:?}");
                                                            terminals.set(terminals.get() + 1);
                                                        }
                                                        _ => panic!("unexpected H2 task wire failure"),
                                                    }
                                                },
                                            ).prepare_in::<Tls<roles::Server>, _, _, _, _, _, _>(
                                                stage, wire, |h2, _wait, stage| {
                                                    let state = std::pin::pin!(State(Cell::new(0), half_close && phase.is_none()));
                                                    let state = state.as_ref().get_ref();
                                                    let h1_app = Shared::new::<wire::Identity, KeyTag<0>>(
                                                        h1_brand, state,
                                                        sark_fiber::Config::new(server::MAX_CONNECTIONS)
                                                            .with_budget(Budget::new(Bound::new(4096), 0)),
                                                    );
                                                    let root = h1.build(h1_app, &mut stage.driver())
                                                        .expect("late H1 listener");
                                                    let h2_app = Shared::multiplexed::<Tls<roles::Server>, KeyTag<2>, _>(
                                                        h2_brand, state, sark_fiber::Config::new(1),
                                                    );
                                                    if let Some(phase) = phase {
                                                        let timer = deadline::config::Config::try_new(
                                                            phase, Duration::from_secs(2),
                                                        ).expect("H2 handler deadline").try_with(
                                                            deadline::Phase::Head, Duration::from_secs(2),
                                                        ).expect("partial HTTP head deadline");
                                                        let listener = h2.build(h2_app, timer, &mut stage.driver())
                                                            .expect("timed TLS listener");
                                                        run::session::<Infallible, _>(stage.with_app(
                                                            &mut finish,
                                                            server::accept::Listeners::new(root, Host::new(listener)),
                                                            |app| run::run(wait, app),
                                                        )).as_ref().map_err(run::Failure::cause).expect("timed TLS session");
                                                    } else {
                                                        let listener = h2.build(
                                                            h2_app, sark_h2::server::task::timers::Disabled,
                                                            &mut stage.driver(),
                                                        ).expect("TLS listener");
                                                        run::session::<Infallible, _>(stage.with_app(
                                                            &mut finish,
                                                            server::accept::Listeners::new(root, Host::new(listener)),
                                                            |app| run::run(wait, app),
                                                        )).as_ref().map_err(run::Failure::cause).expect("TLS session");
                                                    }
                                                    calls.set(state.0.get());
                                                },
                                            ).expect("TLS listener preparation");
                                        },
                                    ).expect("H1 listener preparation");
                                })
                            });
                        })
                    },
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("combined topology");
                    },
                );
                assert_eq!(
                    calls.get(),
                    if reject_open || phase == Some(deadline::Phase::Total) { 1 } else { 2 },
                    "exact shared route calls"
                );
                assert_eq!(opens.get(), usize::from(reject_open));
                assert_eq!(terminals.get(), usize::from(!reject_open && phase != Some(deadline::Phase::Total)));
                if half_close {
                    let facts = facts.take();
                    let old = facts.iter().find(|event| event.0 == "exchange-opened")
                        .expect("duplex admission").1;
                    let input = facts.iter().position(|event| event.1 == old && event.0 == "input-finished")
                        .expect("duplex input finish before retirement");
                    let cause = if phase.is_some() { "exchange-deadline-expired" } else { "exchange-rejected" };
                    let retired = facts.iter().position(|event| event.1 == old && event.0 == cause)
                        .expect("exact duplex retirement cause");
                    let next = facts.iter().position(|event| if phase.is_some() {
                        event.0 == "session-closing"
                    } else { event.1 != old && event.0 == "exchange-opened" })
                        .expect("progress after exact retirement");
                    assert!(input < retired && retired < next, "{facts:?}");
                    if phase.is_none() {
                        assert!(!facts.iter().any(|event| event.0 == "exchange-deadline-expired"));
                    }
                }
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "{reports:?}"
                );
                Ok::<_, Infallible>(())
            },
            |h1_address| {
                let mut h1 = TcpStream::connect(h1_address).expect("H1 connect");
                h1.set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("H1 deadline");
                h1.write_all(
                    b"GET /shared HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .expect("H1 request");
                let mut response = Vec::new();
                h1.read_to_end(&mut response).expect("H1 response");
                assert!(response.starts_with(b"HTTP/1.1 200 "));
                assert!(
                    response
                        .windows(8)
                        .any(|part| part.eq_ignore_ascii_case(b"\r\ndate: "))
                );
                assert!(response.ends_with(RESPONSE));

                let mut socket = TcpStream::connect(h2_address).expect("TLS connect");
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("TLS read deadline");
                socket
                    .set_write_timeout(Some(Duration::from_secs(3)))
                    .expect("TLS write deadline");
                if reject_open {
                    let mut bytes = [0; 1];
                    assert_eq!(socket.read(&mut bytes).expect("rejected TLS open"), 0);
                    return;
                }
                let connection = ClientConnection::new(
                    tls::client_config().expect("TLS client config"),
                    Ipv4Addr::LOCALHOST.into(),
                )
                .expect("TLS client");
                let mut wire = StreamOwned::new(connection, socket);
                let mut client = Connection::<ClientRole>::try_new().expect("H2 client");
                let mut batch = Vec::new();
                if phase == Some(deadline::Phase::Total) {
                    common::h2::peer::flush(&mut client, &mut batch).expect("connection preface");
                    payload::Ping { ack: frame::Ack::Required, opaque: *b"half-end" }
                        .encode(&mut batch).expect("initial head fence");
                }
                let mut stream = client
                    .streams()
                    .headers()
                    .request(
                        if half_close { common::h2::server::CONNECT.as_slice() } else { HEADERS.as_slice() },
                        if half_close && phase.is_none() { Boundary::More } else { Boundary::End },
                    )
                    .expect("H2 request");
                let initial = stream;
                if phase == Some(deadline::Phase::Total) {
                    let encoded = client.output().first();
                    let head = layout::Header::parse(encoded).expect("initial CONNECT frame");
                    assert_eq!(head.kind, frame::numbers::Type::Headers);
                    assert_eq!(encoded.len(), frame::HEADER_LEN + head.length.into_usize());
                    layout::Headers::new(stream, Boundary::End, frame::Block::More, None, &encoded[frame::HEADER_LEN..])
                        .expect("fragmented initial CONNECT").encode(&mut batch).expect("initial head bytes");
                    let len = encoded.len();
                    client.transfer().drain(len);
                    // The ACK is flushed after this record's partial initial HEAD is observed.
                    assert!(batch.len() < 16 << 10);
                    assert_eq!(wire.write(&batch).expect("one CONNECT plaintext record"), batch.len());
                    wire.flush().expect("publish fragmented CONNECT");
                } else {
                    common::h2::peer::flush(&mut client, &mut wire).expect("TLS request");
                }
                assert_eq!(wire.conn.alpn_protocol(), Some(b"h2".as_slice()));
                let mut input = [0; 4096];
                let mut body = Vec::new();
                let mut status = None;
                let mut ended = false;
                let mut closing = None;
                'response: while !ended || phase.is_some() {
                    let read = wire.read(&mut input).unwrap_or_else(|cause| panic!(
                        "TLS response {phase:?}/{half_close}: {cause}; ended={ended}, closing={closing:?}, status={status:?}"
                    ));
                    if read == 0 {
                        assert_eq!(phase, Some(deadline::Phase::Total));
                        assert!(ended, "duplex output FIN");
                        assert_eq!(closing, Some(initial));
                        break;
                    }
                    client.ingress().ingest(&input[..read]).expect("H2 ingress");
                    while let Some(event) = client.ingress().poll_event() {
                        match event {
                            Event::Headers {
                                stream_id,
                                head,
                                boundary,
                                ..
                            } => {
                                assert_eq!(stream_id, stream);
                                status = head
                                    .fields()
                                    .iter()
                                    .find(|field| field.name == b":status")
                                    .map(|field| field.value.to_vec());
                                ended |= matches!(boundary, Boundary::End);
                            }
                            Event::Data {
                                stream_id,
                                data,
                                boundary,
                            } => {
                                assert_eq!(stream_id, stream);
                                data.for_each(|bytes| body.extend_from_slice(bytes));
                                ended |= matches!(boundary, Boundary::End);
                            }
                            Event::GoAway { last_stream_id, error, .. } => {
                                if phase == Some(deadline::Phase::Total) {
                                    assert_eq!(last_stream_id, initial);
                                    assert_eq!(error, frame::numbers::ErrorCode::NoError);
                                    closing = Some(last_stream_id);
                                } else {
                                    assert_eq!(Some(last_stream_id), closing);
                                    assert_eq!(error, frame::numbers::ErrorCode::InternalError);
                                    break 'response;
                                }
                            }
                            Event::Ping { ack: frame::Ack::Complete, opaque } if half_close => {
                                assert_eq!(opaque, *b"half-end");
                                let mut batch = Vec::new();
                                if phase == Some(deadline::Phase::Total) {
                                    payload::Continuation::new(initial, frame::Block::End, &[])
                                        .expect("initial head continuation").encode(&mut batch).expect("continuation bytes");
                                } else {
                                    stream = client.streams().headers().request(HEADERS, Boundary::End)
                                        .expect("successor after failure fence");
                                }
                                // The pending SETTINGS ACK follows CONT and fills the sole
                                // event slot, driving the initial FIN before Header is cleared.
                                common::h2::peer::flush(&mut client, &mut batch).expect("fenced H2 output");
                                assert!(batch.len() < 16 << 10);
                                assert_eq!(wire.write(&batch).expect("one fenced plaintext record"), batch.len());
                                wire.flush().expect("publish fenced output");
                                status = None;
                                ended = false;
                            }
                            event => assert!(
                                matches!(
                                    event,
                                    Event::PrefaceComplete
                                        | Event::SettingsApplied
                                        | Event::SettingsAck
                                ),
                                "unexpected H2 event"
                            ),
                        }
                    }
                    if half_close && phase.is_none() && ended && stream == initial {
                        assert_eq!(status.as_deref(), Some(b"200".as_slice()));
                        assert!(body.is_empty(), "output FIN before input EOF");
                        assert!(matches!(client.streams().try_send(initial, &[], Boundary::End)
                            .expect("legal tunnel EOF"), DataOutcome::Ended));
                        client.control().ping(*b"half-end").expect("failure settlement fence");
                        common::h2::peer::flush(&mut client, &mut wire).expect("EOF and fence");
                        ended = false;
                    }
                    // Keep the Total case silent after its initial CONTINUATION.
                    if !half_close {
                        common::h2::peer::flush(&mut client, &mut wire).expect("H2 acknowledgments");
                    }
                    if phase == Some(deadline::Phase::Handler) && ended && closing.is_none() {
                        let mut batch = Vec::new();
                        for _ in 0..schedule::LIMIT {
                            payload::Ping { ack: frame::Ack::Complete, opaque: [0; 8] }
                                .encode(&mut batch).expect("trailer quota prefix");
                        }
                        let stream = client.streams().headers().request(HEADERS, Boundary::More)
                            .expect("request before its partial trailer");
                        common::h2::peer::flush(&mut client, &mut batch).expect("append initial head");
                        layout::Headers::new(stream, Boundary::End, frame::Block::More, None, &[0])
                            .expect("partial trailer").encode(&mut batch).expect("trailer bytes");
                        // One TLS record queues the initial HEAD behind LIMIT control
                        // events while its trailing HEAD already awaits CONTINUATION.
                        assert!(batch.len() < 16 << 10);
                        assert_eq!(wire.write(&batch).expect("one trailer plaintext record"), batch.len());
                        wire.flush().expect("publish initial head and partial trailer");
                        closing = Some(stream);
                    }
                }
                assert_eq!(status.as_deref(), Some(b"200".as_slice()));
                assert_eq!(body, if phase == Some(deadline::Phase::Total) { &[][..] } else { RESPONSE });
                if phase.is_none() {
                    wire.sock.shutdown(Shutdown::Write).expect("truncate TLS");
                }
                wire.sock.read_to_end(&mut Vec::new()).expect("TLS retirement");
            },
        )
        .expect("H1 and TLS H2 on one driver");
    }
}

#[test]
fn shares_tls_connection_with_grpc() {
    for reject in [true, false] {
        mixed_grpc(reject, false);
    }
}

#[test]
fn releases_or_rejects_header_pressure() {
    mixed_grpc(false, true);
}

fn mixed_grpc(reject: bool, pressure: bool) {
    let pair = Pair::bind().expect("mixed H2 listener");
    pair.check(
        |context, listener| {
            let mut plan = tls::plan(listener)
                .expect("mixed H2 plan")
                .with_streams(std::num::NonZero::new(2).expect("two streams"));
            if pressure {
                plan = plan.with_config(config::Config {
                    stream_capacity: 2,
                    payload_capacity: config::capacity::Payload::new(64, 1)
                        .expect("one retained request head"),
                    ..Default::default()
                });
            }
            let owner = wire::domain::Owner::<Tls<roles::Server>, KeyTag<2>>::try_new(
                plan.connections(),
            ).expect("mixed TLS owner");
            let bank = Bank::try_new(
                plan.connections().count(),
                Limits::default().try_calls(2).expect("two calls").with_output(
                    Output::try_new(1, sark_grpc::message::HEADER_LEN)
                        .expect("one message output budget"),
                ),
            ).expect("gRPC readiness bank");
            let prepared = topology::Plan::new(storage::Value::new((owner, bank)), plan)
                .for_install::<timing::Throughput>().expect("mixed resource claims");
            let mut reports = Reports::new(
                prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE],
            );
            let mut finish = session::Completion::new();
            let terminal = Cell::new(None);
            prepared.enter_with(
                context,
                |worker| worker.lend(|plan, stage| {
                    let (owner, bank) = stage.storage();
                    let wire = owner.bind(endpoint::Config::from(
                        tls::server_config(None).expect("mixed TLS config"),
                    )).expect("mixed TLS binding");
                    plan.tasks((), 1).report(
                        |failure: Failure<
                            '_, '_, '_, Tls<roles::Server>, KeyTag<2>,
                            sark_grpc::server::runtime::run::Failure,
                            sark_h2::server::task::Build<sark_grpc::server::runtime::state::Error>,
                        >| match failure {
                            Failure::Wire { cause, .. } => panic!("mixed wire: {:?}", cause.into_parts().0),
                            Failure::Construction(cause) => {
                                assert!(matches!(cause, sark_h2::server::task::Build::Deadlines(
                                    o3::depot::heap::Error::Allocation { capacity, error: o3::alloc::Exhausted, .. }
                                ) if capacity.raw() == 2), "{cause:?}");
                                assert!(terminal.replace(Some(true)).is_none());
                            }
                            Failure::Drive {
                                cause: sark_h2::server::app::Error::Connection(
                                    sark_h2::conn::Error::CriticalDeadline,
                                ),
                                ..
                            } if !pressure => assert!(terminal.replace(Some(false)).is_none()),
                            Failure::Drive {
                                cause: sark_h2::server::app::Error::Connection(
                                    sark_h2::conn::Error::HeaderDecodeFull { used: 1, limit: 1 },
                                ),
                                ..
                            } if pressure => assert!(terminal.replace(Some(false)).is_none()),
                            Failure::Drive { cause, .. } | Failure::Diagnostic { cause, .. } => {
                                panic!("mixed drive: {cause:?}")
                            }
                            Failure::Output { cause, .. } => panic!("mixed output: {cause:?}"),
                            Failure::Response { cause, .. } => panic!("mixed response: {cause:?}"),
                            Failure::Open(cause) => panic!("mixed TLS open: {:?}", cause.into_cause()),
                        },
                    ).prepare_in::<Tls<roles::Server>, _, _, _, _, _, _>(
                        stage, wire, |prepared, _wait, stage| {
                            let state = std::pin::pin!(State(Cell::new(0), false));
                            let state = state.as_ref().get_ref();
                            Brand::with(|brand| {
                                let factory = Shared::multiplexed::<Tls<roles::Server>, KeyTag<2>, _>(
                                    brand, state, sark_fiber::Config::new(1),
                                );
                                let listener = prepared.build_with(
                                    factory,
                                    tls::Deadlines(deadline::config::Config::try_new(
                                        deadline::Phase::Handler, Duration::from_secs(2),
                                    ).expect("HTTP handler deadline").try_with(
                                        deadline::Phase::Head, Duration::from_secs(2),
                                    ).expect("partial HTTP head deadline"), reject),
                                    mode::Direct.build(tls::Rpc, bank), &mut stage.driver(),
                                ).expect("mixed TLS listener");
                                stage.run_quiesce(
                                    &mut finish, Host::new(listener), Duration::from_secs(2),
                                ).expect("mixed session").expect("mixed drive");
                            });
                            assert_eq!(state.0.get(), if reject || pressure { 0 } else { 5 }, "HTTP siblings and follow-up");
                        },
                    ).expect("mixed TLS preparation");
                }),
                Policy::new(|event| reports.push(event), recover),
                |result| {
                    result.as_ref().map_err(|error| error.cause()).expect("mixed topology");
                },
            );
            assert!(reports.events().expect("mixed retirement").is_empty(), "{reports:?}");
            Ok::<_, Infallible>(terminal.get())
        },
        |address| {
            let mut socket = TcpStream::connect(address).expect("mixed TLS connect");
            socket.set_read_timeout(Some(Duration::from_secs(3))).expect("mixed read deadline");
            socket.set_write_timeout(Some(Duration::from_secs(3))).expect("mixed write deadline");
            if reject {
                assert_eq!(socket.read(&mut [0]).expect("construction rejection"), 0);
                return;
            }
            let connection = ClientConnection::new(
                tls::client_config().expect("mixed client config"), Ipv4Addr::LOCALHOST.into(),
            ).expect("mixed TLS client");
            let mut wire = StreamOwned::new(connection, socket);
            let mut client = Connection::<ClientRole>::with_config(config::Config {
                local_settings: config::Settings {
                    initial_window_size: 4,
                    ..Default::default()
                },
                ..Default::default()
            }).expect("mixed H2 client");
            let frame = Frame::try_header(Compression::Plain, 0).expect("empty protobuf frame");
            let stream = if pressure {
                let mut fields = HEADERS.to_vec();
                fields.push(Entry::new(
                    field::literal!(Lower::try_bytes(b"x-duplex-header")),
                    field::literal!(Value::try_bytes(b"retained")),
                ));
                let http = client.streams().headers().request_key(fields, Boundary::End)
                    .expect("header released by a parked task");
                let mut batch = Vec::new();
                common::h2::peer::flush(&mut client, &mut batch).expect("first request");
                let metadata = Metadata::new();
                let fields = request::Fields::new(b"/benchmark.BenchmarkService/Missing", Some(b"localhost"), &metadata)
                    .expect("immediate gRPC response").with_scheme(request::Scheme::Https);
                let rpc = client.streams().headers().request(fields, Boundary::End)
                    .expect("request behind retained header");
                let mut encoded = Vec::new();
                common::h2::peer::flush(&mut client, &mut encoded).expect("second request");
                let header = layout::Header::parse(&encoded).expect("request frame");
                assert_eq!(encoded.len(), frame::HEADER_LEN + header.length.into_usize());
                layout::Headers::new(rpc, Boundary::End, frame::Block::End, Some(layout::Priority {
                    sharing: layout::Sharing::Shared,
                    dependency: sark_h2::stream::Id::CONNECTION,
                    weight: 0,
                }), &encoded[frame::HEADER_LEN..]).expect("retained decoder path")
                    .encode(&mut batch).expect("priority request");
                assert!(batch.len() < 16 << 10);
                assert_eq!(wire.write(&batch).expect("one plaintext record"), batch.len());
                wire.flush().expect("publish retained requests");
                let mut input = [0; 4096];
                'resumed: loop {
                    let read = wire.read(&mut input).expect("response after releasing header");
                    assert_ne!(read, 0, "resumed request");
                    client.ingress().ingest(&input[..read]).expect("resumed ingress");
                    while let Some(event) = client.ingress().poll_event() {
                        match event {
                            Event::Headers { stream_id, head, boundary: Boundary::End, .. } => {
                                assert_eq!(stream_id.id(), rpc);
                                assert_eq!(Status::from_fields(head.fields()).expect("resumed status").code(),
                                    GrpcCode::Unimplemented);
                                break 'resumed;
                            }
                            Event::GoAway { error, .. } => panic!("releasable header: {error:?}"),
                            _ => {}
                        }
                    }
                }
                client.streams().try_cancel(http, frame::numbers::ErrorCode::Cancel)
                    .expect("retire parked HTTP task");
                let mut metadata = Metadata::new();
                metadata.push(b"x-request", b"request-only").expect("retained metadata");
                let fields = request::Fields::new(tls::UNARY, Some(b"localhost"), &metadata)
                    .expect("gRPC request fields").with_scheme(request::Scheme::Https);
                let first = client.streams().headers().request(fields, Boundary::More)
                    .expect("buffered gRPC head");
                assert!(matches!(client.streams().try_send(first, &frame, Boundary::More)
                    .expect("gRPC message"), DataOutcome::Sent(bytes) if bytes.get() == frame.len()));
                client.streams().headers().trail(first, [Entry::new(
                    field::literal!(Lower::try_bytes(b"x-request-tail")),
                    field::literal!(Value::try_bytes(b"tail-only")),
                )]).expect("trailer needs a second header owner");
                common::h2::peer::flush(&mut client, &mut wire).expect("header pressure batch");
                first
            } else {
            for (path, count, messages, expected) in [
                (Some(tls::UNARY), b"0".as_slice(), 1, GrpcCode::Ok),
                (Some(tls::STREAM), b"3".as_slice(), 3, GrpcCode::Ok),
                (Some(tls::STREAM), b"0".as_slice(), 0, GrpcCode::Ok),
                (Some(b"/benchmark.BenchmarkService/Missing".as_slice()), b"0".as_slice(), 0, GrpcCode::Unimplemented),
                (None, b"0".as_slice(), 0, GrpcCode::Ok),
            ] {
                let mut late = expected == GrpcCode::Unimplemented;
                let http = client.streams().headers().request(
                    HEADERS, if late { Boundary::More } else { Boundary::End },
                )
                    .expect("HTTP sibling");
                let rpc = path.map(|path| {
                    let mut metadata = Metadata::new();
                    metadata.push(b"x-count", count).expect("stream count");
                    metadata.push(b"x-request", b"request-only").expect("request metadata");
                    let fields = request::Fields::new(path, Some(b"localhost"), &metadata)
                        .expect("gRPC request fields").with_scheme(request::Scheme::Https);
                    let stream = client.streams().headers().request(fields, Boundary::More)
                        .expect("gRPC sibling");
                    if !late {
                        assert!(matches!(
                            client.streams().try_send(stream, &frame, Boundary::More).expect("gRPC message"),
                            DataOutcome::Sent(bytes) if bytes.get() == frame.len()
                        ));
                        client.streams().headers().trail(stream, [Entry::new(
                            field::literal!(Lower::try_bytes(b"x-request-tail")),
                            field::literal!(Value::try_bytes(b"tail-only")),
                        )]).expect("gRPC request trailers");
                    }
                    stream
                });
                common::h2::peer::flush(&mut client, &mut wire).expect("mixed requests");
                assert_eq!(wire.conn.alpn_protocol(), Some(b"h2".as_slice()));
                let mut bodies = [Vec::new(), Vec::new()];
                let mut heads = [false; 2];
                let mut ended = [false; 2];
                let mut status = None;
                let mut input = [0; 4096];
                while !ended[0] || (rpc.is_some() && !ended[1]) {
                    let read = wire.read(&mut input).expect("mixed TLS response");
                    assert_ne!(read, 0, "complete mixed responses");
                    client.ingress().ingest(&input[..read]).expect("mixed H2 ingress");
                    let index = |id| {
                        if id == http { 0 } else {
                            assert_eq!(Some(id), rpc);
                            1
                        }
                    };
                    while let Some(event) = client.ingress().poll_event() {
                        match event {
                            Event::Headers { stream_id, head, boundary, section } => {
                                let index = index(stream_id.id());
                                match section {
                                    Section::Initial => {
                                        assert!(!std::mem::replace(&mut heads[index], true));
                                        assert_eq!(head.fields().iter().find(|field| field.name == b":status")
                                            .map(|field| field.value), Some(b"200".as_slice()));
                                        assert_eq!(head.fields().iter().any(|field|
                                            field.name == b"content-type" && field.value == b"application/grpc+proto"
                                        ), index == 1);
                                        assert!(!head.fields().iter().any(|field| field.name == b"grpc-status"));
                                    }
                                    Section::Trailing => {
                                        assert_eq!(index, 1, "gRPC trailers stay on their stream");
                                        assert!(heads[index] && !ended[index]);
                                        assert_eq!(boundary, Boundary::End);
                                        let code = Status::from_fields(head.fields()).expect("gRPC status").code();
                                        assert!(status.replace(code).is_none(), "one terminal status");
                                    }
                                }
                                ended[index] |= boundary == Boundary::End;
                            }
                            Event::Data { stream_id, data, boundary } => {
                                let index = index(stream_id.id());
                                if index == 1 {
                                    assert_eq!(boundary, Boundary::More, "status ends gRPC");
                                }
                                assert!(heads[index] && !ended[index]);
                                data.for_each(|bytes| bodies[index].extend_from_slice(bytes));
                                ended[index] |= boundary == Boundary::End;
                            }
                            Event::Ping { ack: frame::Ack::Complete, opaque } => {
                                assert_eq!(&opaque, b"old-tail");
                                assert!(matches!(
                                    client.streams().try_send(http, &[], Boundary::End)
                                        .expect("finish held HTTP input"),
                                    DataOutcome::Ended
                                ));
                            }
                            Event::StreamReset { stream_id, error, validation, .. } => panic!(
                                "mixed reset {path:?}: {stream_id:?}/{error:?}/{validation:?}"
                            ),
                            Event::GoAway { last_stream_id, error, .. } => panic!(
                                "mixed GOAWAY {path:?}: {last_stream_id:?}/{error:?}"
                            ),
                            event => assert!(matches!(event,
                                Event::PrefaceComplete | Event::SettingsApplied | Event::SettingsAck
                            ), "unexpected mixed H2 event"),
                        }
                    }
                    common::h2::peer::flush(&mut client, &mut wire).expect("mixed H2 acknowledgments");
                    if late && ended[1] {
                        late = false;
                        assert!(!heads[0], "HTTP input holds the other physical slot");
                        assert!(matches!(
                            client.streams().try_send(rpc.expect("finished gRPC"), &[], Boundary::End)
                                .expect("late gRPC input end"),
                            DataOutcome::Ended
                        ));
                        client.control().ping(*b"old-tail").expect("late input fence");
                        common::h2::peer::flush(&mut client, &mut wire).expect("old input tail and fence");
                    }
                }
                assert_eq!(bodies[0], RESPONSE);
                assert_eq!(bodies[1], frame.repeat(messages), "exact lazy response count");
                assert_eq!(status, rpc.map(|_| expected));
            }
            let mut batch = Vec::new();
            let stream = client.streams().headers().request(HEADERS, Boundary::End)
                .expect("request with incomplete initial head");
            layout::Headers::new(stream, Boundary::End, frame::Block::More, None, &[0])
                .expect("partial head").encode(&mut batch).expect("head bytes");
            assert_eq!(wire.write(&batch).expect("partial head plaintext record"), batch.len());
            wire.flush().expect("publish partial head");
            stream
            };
            let mut input = [0; 4096];
            'terminal: loop {
                let read = wire.read(&mut input).expect("terminal H2 response");
                assert_ne!(read, 0, "terminal GOAWAY");
                client.ingress().ingest(&input[..read]).expect("deadline ingress");
                while let Some(event) = client.ingress().poll_event() {
                    match event {
                        Event::GoAway { last_stream_id, error, .. } => {
                            if !pressure { assert_eq!(last_stream_id, stream); }
                            assert_eq!(error, if pressure {
                                frame::numbers::ErrorCode::EnhanceYourCalm
                            } else {
                                frame::numbers::ErrorCode::InternalError
                            });
                            break 'terminal;
                        }
                        Event::PrefaceComplete | Event::SettingsApplied | Event::SettingsAck if pressure => {}
                        _ => panic!("unexpected partial head response"),
                    }
                }
            }
            wire.sock.read_to_end(&mut Vec::new()).expect("mixed TLS retirement");
        },
        |terminal| assert_eq!(terminal.expect("mixed TLS server"), Some(reject)),
    ).expect("HTTP and gRPC share one TLS H2 connection");
}
