use sark_h2::server::app::Error;
pub mod common;

use std::{
    cell::Cell,
    convert::Infallible,
    io::{Read, Write},
    net::Ipv4Addr,
    sync::Arc,
};

use common::{
    body::headers,
    tls::{self, Output as _},
};
use dope::{
    core::{
        driver::{
            life::recovery,
            route::{FrameworkTag, KeyTag, Tag, kind, table::conn::Capacity},
            settings,
        },
        io::socket::{self, option::Switch},
    },
    manifold::{network::listener::config, timing},
    net::{tcp, wire::domain},
    runtime::{
        executor::session,
        topology::{self, ops},
    },
};
use dope_test::{harness::server::Pair, reclaim::Reports};
use dope_tls::{
    config::raw::Identity,
    tls::{
        Tls,
        endpoint::{self, layout::Staged},
        roles,
    },
};
use o3::alloc;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{self, DataOutcome, event::Event, machine::Connection},
    frame::{Ack, layout::Data, numbers::ErrorCode, payload::Ping},
    hpack::encode::fields,
    server::{
        self,
        runtime::{Build, Plan, app, modes},
        service,
    },
};
use sark_http::payload::{
    fixed::Body,
    response::{statics, status::Code},
};

#[test]
fn retains_across_records() {
    for reject in [false, true] {
        Pair::bind()
            .expect("H2 TLS listener")
            .check(
                |context, listener| {
                    let done = Cell::new(0);
                    let reported = Cell::new(0);
                    let plan = Plan::<KeyTag<0>>::new(
                        config::Prepared {
                            bind: listener.local_addr(),
                            connections: Capacity::fixed::<1>(),
                            per_ip: None,
                            direct_flights: Default::default(),
                            backlog: socket::Backlog::fixed::<1>(),
                            stream: tcp::StreamConfig {
                                no_delay: Some(Switch::On),
                                ..Default::default()
                            },
                            transport: Default::default(),
                            egress: Default::default(),
                        },
                        settings::Receive::fixed::<128, 1024>(),
                        server::Config {
                            max_request_body_bytes: tls::BODY_LEN,
                            max_request_body_chunks: tls::RECORDS + 1,
                            max_connection_body_bytes: tls::BODY_LEN,
                            max_connection_body_chunks: tls::RECORDS + 1,
                            max_outbound_bytes: 4096,
                        },
                    )
                    .expect("H2 TLS setup")
                    .with_bound(listener);
                    let owner = domain::Owner::<_, KeyTag<0>>::try_new(plan.connections())
                        .expect("H2 TLS setup");
                    let wire = endpoint::Config::from(Arc::new(
                        Identity::from_seed(&tls::SEED)
                            .expect("TLS server identity")
                            .server()
                            .expect("TLS server config"),
                    ))
                    .with_staged(Staged::fixed::<64>());
                    let handler = service::FromFn::new(|request: service::Request<'_, '_, '_>| {
                        assert_eq!(request.body_len(), tls::BODY_LEN);
                        assert!(
                            request
                                .body()
                                .segments()
                                .flatten()
                                .all(|byte| *byte == b'x')
                        );
                        done.set(done.get() + 1);
                        statics::Direct::new(Code::OK, [], Body::new(tls::RESPONSE))
                    });
                    let prepared = plan
                        .with_direct::<Tls<roles::Server>, _, _, modes::Sync>(
                            &owner,
                            (
                                service::Route::new(handler),
                                app::Report::new(
                                    |failure: app::Failure<
                                        '_,
                                        '_,
                                        '_,
                                        Tls<roles::Server>,
                                        KeyTag<0>,
                                        Infallible,
                                        Build<alloc::Error>,
                                    >| {
                                        assert!(matches!(
                                            failure,
                                            app::Failure::Diagnostic {
                                                cause: Error::Connection(
                                                    conn::Error::HeaderEncode(
                                                        fields::Error::List {
                                                            used: 0,
                                                            name: 7,
                                                            value: 3,
                                                            limit: 1,
                                                        }
                                                    )
                                                ),
                                                ..
                                            }
                                        ));
                                        reported.set(reported.get() + 1);
                                    },
                                ),
                            ),
                            wire,
                        )
                        .for_install::<timing::Throughput>()
                        .expect("H2 TLS setup");
                    let mut reports =
                        Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                    let mut finish = session::Completion::new();
                    let _done = prepared.enter(
                        context,
                        ops::Run(&mut finish),
                        recovery::Policy::new(
                            |event| reports.push(event),
                            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                                panic!("native H2 TLS failure: {:?}", failure.cause())
                            },
                        ),
                        |result| {
                            let result = match result {
                                Ok(result) => result.map_err(topology::RunError::Failure),
                                Err(error) => Err(topology::RunError::from(error)),
                            };
                            result
                                .as_ref()
                                .map_err(topology::RunError::cause)
                                .expect("H2 TLS shutdown");
                            result.ok().expect("checked H2 TLS shutdown")
                        },
                    );
                    assert!(
                        reports
                            .events()
                            .expect("H2 TLS reclaim ownership")
                            .is_empty(),
                        "unexpected H2 TLS retirement: {reports:?}"
                    );
                    assert_eq!(reported.get(), usize::from(reject), "H2 TLS diagnostic");
                    Ok::<_, Infallible>(done.get())
                },
                |address| {
                    let key = Identity::from_seed(&tls::SEED)
                        .expect("TLS peer identity")
                        .public_key();
                    let mut wire = tls::Peer::connect(
                        address,
                        Arc::new(Identity::client(key).expect("TLS peer key")),
                        Ipv4Addr::LOCALHOST.into(),
                    )
                    .expect("TLS handshake");
                    let mut config = conn::config::Config::default();
                    config.local_settings.max_header_list_size = reject.then_some(1);
                    let mut client =
                        Connection::<ClientRole>::with_config(config).expect("H2 client");
                    let stream = client
                        .streams()
                        .headers()
                        .request(
                            headers(b"/retained", b"33280").expect("H2 request fields"),
                            Boundary::More,
                        )
                        .expect("H2 request stream");
                    client.flush(&mut wire).expect("H2 request headers");
                    let mut record = Vec::with_capacity(16384);
                    Data::new(stream, Boundary::More, &tls::CHUNK)
                        .expect("DATA record stream")
                        .encode(&mut record)
                        .expect("DATA record encoding");
                    let record = tls::padded(record).expect("DATA record padding");
                    for _ in 0..tls::RECORDS {
                        wire.write_all(&record).expect("one 16KiB TLS DATA record");
                        wire.flush().expect("flush TLS DATA record");
                    }
                    let mut ping = Vec::with_capacity(16384);
                    Ping {
                        ack: Ack::Required,
                        opaque: tls::PING,
                    }
                    .encode(&mut ping)
                    .expect("PING record encoding");
                    wire.write_all(&tls::padded(ping).expect("PING record padding"))
                        .expect("16KiB TLS control record");
                    wire.flush().expect("flush TLS control record");
                    let mut input = [0; 16384];
                    let mut acked = false;
                    while !acked {
                        let read = wire.read(&mut input).expect("TLS control response");
                        assert_ne!(read, 0, "TLS control response ended");
                        client
                            .ingress()
                            .ingest(&input[..read])
                            .expect("H2 control response");
                        while let Some(event) = client.ingress().poll_event() {
                            match event {
                                Event::Ping {
                                    ack: Ack::Complete,
                                    opaque,
                                } => {
                                    assert_eq!(opaque, tls::PING);
                                    acked = true;
                                }
                                event => assert!(
                                    matches!(
                                        event,
                                        Event::PrefaceComplete
                                            | Event::SettingsApplied
                                            | Event::SettingsAck
                                    ),
                                    "unexpected event before END_STREAM"
                                ),
                            }
                        }
                        client.flush(&mut wire).expect("H2 control acknowledgments");
                    }
                    assert_eq!(
                        client
                            .streams()
                            .try_send(stream, &[], Boundary::End)
                            .expect("H2 peer send"),
                        DataOutcome::Ended,
                    );
                    client
                        .flush(&mut wire)
                        .expect("separate TLS END_STREAM record");

                    let mut status = None;
                    let mut body = Vec::new();
                    let mut ended = false;
                    while !ended {
                        let read = wire.read(&mut input).expect("TLS response");
                        assert_ne!(read, 0, "TLS response ended");
                        client
                            .ingress()
                            .ingest(&input[..read])
                            .expect("H2 response");
                        while let Some(event) = client.ingress().poll_event() {
                            match event {
                                Event::Headers {
                                    stream_id: id,
                                    head,
                                    boundary,
                                    ..
                                } => {
                                    assert_eq!(id, stream);
                                    assert!(status.is_none(), "one response head");
                                    status = head
                                        .fields()
                                        .iter()
                                        .find(|field| field.name == b":status")
                                        .map(|field| field.value.to_vec());
                                    ended |= matches!(boundary, Boundary::End);
                                }
                                Event::Data {
                                    stream_id: id,
                                    data,
                                    boundary,
                                } => {
                                    assert_eq!(id, stream);
                                    data.for_each(|bytes| body.extend_from_slice(bytes));
                                    ended |= matches!(boundary, Boundary::End);
                                }
                                Event::StreamReset {
                                    stream_id: id,
                                    error,
                                    ..
                                } => {
                                    assert!(reject, "unexpected response reset");
                                    assert_eq!(id, stream);
                                    assert_eq!(error, ErrorCode::EnhanceYourCalm);
                                    ended = true;
                                }
                                event => assert!(
                                    matches!(
                                        event,
                                        Event::PrefaceComplete
                                            | Event::SettingsApplied
                                            | Event::SettingsAck
                                    ),
                                    "unexpected response event"
                                ),
                            }
                        }
                        client
                            .flush(&mut wire)
                            .expect("H2 response acknowledgments");
                    }
                    if reject {
                        assert!(status.is_none());
                        assert!(body.is_empty());
                    } else {
                        assert_eq!(status.as_deref(), Some(b"200".as_slice()));
                        assert_eq!(body, tls::RESPONSE);
                    }
                    assert!(wire.close().expect("authenticated TLS close").is_empty());
                },
                |result| assert_eq!(result.expect("TLS server completion"), 1),
            )
            .expect("TLS retained H2 body runtime");
    }
}
