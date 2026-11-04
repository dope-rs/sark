pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io::{Read, Write},
    net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream},
    time::Duration,
};

use common::{
    h2::tls::{self, HEADERS, RESPONSE, Shared, State},
    server::{self, recover},
};
use dope::{
    core::{
        driver::{
            life::recovery::Policy,
            route::{FrameworkTag, KeyTag, Tag, kind, table::conn::Capacity},
            settings, storage,
        },
        io::socket::{self, Listener},
    },
    manifold::{app::Host, network::listener::config},
    net::{tcp, wire},
    runtime::{executor::session, topology},
};
use dope_fiber::task::storage::scope::Brand;
use dope_test::{harness::server::Pair, reclaim::Reports};
use dope_tls::{
    rustls::{self, ClientConnection, StreamOwned},
    tls::{Tls, endpoint, roles},
};
use sark_core::{deadline, manifold::stream::Boundary};
use sark_h1::server::{date, run};
use sark_h2::{
    ClientRole,
    conn::{event::Event, machine::Connection},
    server::runtime::{Plan, app::Failure},
};

#[test]
fn shares_driver_storage_with_h1() {
    for (timed, reject_open) in [(false, false), (true, false), (false, true)] {
        let pair = Pair::bind().expect("H1 listener");
        let h2 = Listener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("H2 listener");
        let h2_address = h2.local_addr();
        pair.check(
            move |context, h1| {
                let h1 = server::Plan::new(h1.local_addr(), Duration::from_secs(5))
                    .build()
                    .with_bound(h1);
                let baseline = h1.config();
                let h2 = Plan::<KeyTag<2>>::new(
                    config::Prepared {
                        bind: h2.local_addr(),
                        connections: Capacity::fixed::<1>(),
                        per_ip: None,
                        direct_flights: Default::default(),
                        backlog: socket::Backlog::fixed::<1>(),
                        stream: tcp::StreamConfig::default(),
                        transport: Default::default(),
                        egress: Default::default(),
                    },
                    settings::Receive::fixed::<64, { 32 << 10 }>(),
                    common::h2::server::CONFIG,
                )
                .expect("H2 plan")
                .with_bound(h2);
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
                let state = State(Cell::new(0));
                let opens = Cell::new(0);
                let terminals = Cell::new(0);
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
                                    let h1_app = Shared::new::<wire::Identity, KeyTag<0>>(
                                        h1_brand,
                                        &state,
                                        sark_fiber::Config::new(server::MAX_CONNECTIONS),
                                    );
                                    let h2_app =
                                        Shared::multiplexed::<Tls<roles::Server>, KeyTag<2>, _>(
                                            h2_brand,
                                            &state,
                                            sark_fiber::Config::new(1),
                                        );
                                    h1.install(
                                        stage,
                                        wire::domain::Local::<wire::Identity, _>::new(
                                            wire::Identity,
                                        ),
                                        h1_app,
                                        |root, wait, stage| {
                                            let tasks = h2.tasks(h2_app, 1).report(
                                                |failure: Failure<'_, '_, Tls<roles::Server>, KeyTag<2>>| {
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
                                            );
                                            if timed {
                                                let timer = deadline::config::Config::try_new(
                                                    deadline::Phase::Handler,
                                                    Duration::from_secs(2),
                                                )
                                                .expect("H2 handler deadline");
                                                tasks
                                                    .try_due(
                                                        stage,
                                                        wire,
                                                        timer,
                                                        |listener, stage| {
                                                            run::Failure::<Infallible, _>::session(
                                                                stage.with_app(
                                                                    &mut finish,
                                                                    server::accept::Listeners::new(root, Host::new(listener)),
                                                                    |app| wait.run(app),
                                                                ),
                                                            )
                                                            .as_ref()
                                                            .map_err(run::Failure::cause)
                                                            .expect("timed TLS session");
                                                        },
                                                    )
                                                    .expect("timed TLS listener");
                                            } else {
                                                tasks
                                                    .try_install(stage, wire, |listener, stage| {
                                                        run::Failure::<Infallible, _>::session(
                                                            stage.with_app(
                                                                &mut finish,
                                                                server::accept::Listeners::new(root, Host::new(listener)),
                                                                |app| wait.run(app),
                                                            ),
                                                        )
                                                        .as_ref()
                                                        .map_err(run::Failure::cause)
                                                        .expect("TLS session");
                                                    })
                                                    .expect("TLS listener");
                                            }
                                        },
                                    )
                                    .expect("H1 listener installation");
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
                    state.0.get(),
                    if reject_open { 1 } else { 2 },
                    "both protocols execute the same async route"
                );
                assert_eq!(opens.get(), usize::from(reject_open));
                assert_eq!(terminals.get(), usize::from(!reject_open));
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
                let stream = client
                    .streams()
                    .headers()
                    .request(HEADERS, Boundary::End)
                    .expect("H2 request");
                tls::flush(&mut client, &mut wire).expect("TLS request");
                assert_eq!(wire.conn.alpn_protocol(), Some(b"h2".as_slice()));
                let mut input = [0; 4096];
                let mut body = Vec::new();
                let mut status = None;
                let mut ended = false;
                while !ended {
                    let read = wire.read(&mut input).expect("TLS response");
                    assert_ne!(read, 0, "complete H2 response");
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
                    tls::flush(&mut client, &mut wire).expect("H2 acknowledgments");
                }
                assert_eq!(status.as_deref(), Some(b"200".as_slice()));
                assert_eq!(body, RESPONSE);
                wire.sock.shutdown(Shutdown::Write).expect("truncate TLS");
                wire.sock.read_to_end(&mut Vec::new()).expect("TLS retirement");
            },
            |result| result.expect("combined server"),
        )
        .expect("H1 and TLS H2 on one driver");
    }
}
