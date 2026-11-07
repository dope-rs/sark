pub mod common;

use std::{cell::Cell, convert::Infallible};

use common::server::h3::{
    proof::{Dispatch, request::Spec},
    sessions::Scenario,
};
use dope::core::driver::life::recovery::Recovery;
use sark_core::{life::session::Disabled, manifold::stream::Boundary};
use sark_h3::{
    conn::options,
    frame,
    manifold::server::{Config, deadline},
    qpack::encoder::Encoder,
};
use sark_http::field::{Entry, Lower, Pseudo, Value, literal};

#[test]
fn reports_rejected_ingress() {
    let limit = Config::default().protocol.ingress.field_section_bytes;
    let field = Entry::new(
        literal!(Lower::try_bytes(b"accept")),
        literal!(Value::try_bytes(b"*/*")),
    );
    let mut block = Vec::new();
    Encoder::new()
        .encode(0, vec![field; limit / 41 + 1], &mut block)
        .expect("static oversized field section");
    let mut wire = Vec::new();
    frame::Payload::encode(frame::TYPE_HEADERS, &block, &mut wire).expect("HEADERS frame");
    let expected = format!(
        "H3(Qpack(FieldCapacity {{ available: {}, required: 32 }}))",
        limit % 41
    );

    let mut deferred = [0x21, 0].repeat(33);
    deferred.push(frame::TYPE_HEADERS.get() as u8);
    for (wire, boundary, expected) in [
        (wire.as_slice(), Boundary::More, expected.as_str()),
        (wire.as_slice(), Boundary::End, expected.as_str()),
        (
            deferred.as_slice(),
            Boundary::End,
            "H3(Incomplete { stream_id: 0, buffered: 1, cause: VarInt(Underflow { actual: 0, required: 1 }) })",
        ),
    ] {
        let cause = Cell::new(None);
        let mut request = Spec::new(b"/", b"");
        request.head.wire = Some(wire);
        request.data.boundary = boundary;
        request.expect.input_finished = false;
        request.expect.connection_closed = true;
        Scenario {
            name: "invalid-ingress-closes",
            requests: vec![request],
            expected_bodies: vec![None],
            task_capacity: 1,
            cancellation: None,
            must_finish_before_last: None,
            linked: 0,
        }
        .try_run_with(
            common::server::h3::sessions::plain,
            Default::default(),
            None,
            Disabled,
            deadline::Disabled,
            |error: Recovery<'_, '_>| -> Infallible { panic!("{:?}", error.cause()) },
            |error| assert!(cause.replace(Some(format!("{error:?}"))).is_none()),
            sark_core::life::mode::Observed::new,
            |outcome| outcome.expect("invalid ingress closes without waiting for input"),
        );
        assert_eq!(cause.take().as_deref(), Some(expected));
    }
}

#[test]
fn rejected_head_reconciles_pending_parser_credit() {
    let length = (Config::default().max_request_body_bytes + 1).to_string();
    let fields = [
        Entry::pseudo(Pseudo::Method, literal!(Value::try_bytes(b"POST"))),
        Entry::pseudo(Pseudo::Scheme, literal!(Value::try_bytes(b"https"))),
        Entry::pseudo(
            Pseudo::Authority,
            literal!(Value::try_bytes(b"example.com")),
        ),
        Entry::pseudo(Pseudo::Path, literal!(Value::try_bytes(b"/body"))),
        Entry::new(
            literal!(Lower::try_bytes(b"content-length")),
            Value::try_bytes(length.as_bytes()).expect("decimal length"),
        ),
    ];
    let mut block = Vec::new();
    Encoder::new()
        .encode(0, fields, &mut block)
        .expect("request head");
    for unknown in [0, 100] {
        let mut wire = Vec::new();
        frame::Payload::encode(frame::TYPE_HEADERS, &block, &mut wire).expect("HEADERS frame");
        wire.extend_from_slice(&[0x21, 0].repeat(unknown));
        let mut rejected = Spec::new(b"/body", b"x");
        rejected.head.wire = Some(&wire);
        rejected.data.boundary = Boundary::More;
        rejected.expect.error = Some(frame::ErrorCode::ExcessiveLoad);
        rejected.expect.input_finished = false;
        let mut followup = Spec::new(b"/body", b"x");
        followup.dispatch = Dispatch::AfterFailure;
        let mut protocol = options::Config::default();
        protocol.qpack.decoder.blocked_streams = 1;
        let proof = Scenario {
            name: "reject-with-pending-parser-credit",
            requests: vec![rejected, followup],
            expected_bodies: vec![None, Some(b"x")],
            task_capacity: 1,
            cancellation: None,
            must_finish_before_last: None,
            linked: 1,
        }
        .try_run_with(
            common::server::h3::sessions::plain,
            protocol,
            None,
            Disabled,
            deadline::Disabled,
            |error: Recovery<'_, '_>| -> Infallible { panic!("{:?}", error.cause()) },
            |error| panic!("{unknown} unknown frames: {error:?}"),
            sark_core::life::mode::Observed::new,
            |outcome| outcome.unwrap_or_else(|error| panic!("{unknown} unknown frames: {error:?}")),
        );
        assert!(proof.cancel.followup);
    }
}

#[test]
fn rejected_request_drains_inflight_data_before_reusing_capacity() {
    use std::{
        net::SocketAddr,
        rc::Rc,
        time::{Duration, Instant},
    };

    use common::server::h3::{
        Client,
        apps::SrvApp,
        native::Native,
        observer::{Probe, StoragePlan, config_with_streams},
        proof::{DIALER, LISTENER, Proof, Shared},
    };
    use dope::{
        core::driver::{
            life::{creation::Attempt, recovery},
            route::KeyTag,
            settings::{
                self,
                claim::{Claim, Needs},
                demands,
            },
            storage,
        },
        net::wire,
        runtime::{
            control,
            executor::{Executor, session},
        },
    };
    use dope_fiber::task::storage::scope::Brand;
    use dope_quic::{
        config,
        conn::{life::tls::clients::Authority, stream::access::bindings},
        runtime::{
            embedded::{self, client, link::Link, plan},
            routing,
        },
    };
    use sark_core::life::{Event, audit::Capability, mode::Observed};
    use sark_h3::manifold::{Phase, server::startup::embedded::Embedded};
    use sark_http::payload::compress::dynamic::hash;

    for (path, status, expected_body, expected_error, stop_before_fin) in [
        (
            &b"/body"[..],
            &b"200"[..],
            None,
            Some(frame::ErrorCode::ExcessiveLoad),
            false,
        ),
        (b"/guarded", b"401", Some(&b"denied"[..]), None, false),
        (b"/missing", b"404", Some(&b""[..]), None, false),
        (b"/guarded", b"401", Some(&b"denied"[..]), None, true),
        (b"/missing", b"404", Some(&b""[..]), None, true),
    ] {
        let client_addr = SocketAddr::from(([127, 0, 0, 1], 4000));
        let server_addr = SocketAddr::from(([127, 0, 0, 1], 443));
        let identity = config::raw::Identity::from_seed(&[0x71; 32]).expect("identity");
        let mut client_tls =
            config::raw::Identity::client(identity.public_key()).expect("client TLS");
        let mut tls = identity.server().expect("server TLS");
        tls.alpn_protocols = vec![b"h3".to_vec()];
        client_tls.alpn_protocols = vec![b"h3".to_vec()];
        let auth = Authority::try_new(client_tls, server_addr.ip().into()).expect("authority");
        let plan = plan::Plan::try_new(embedded::Config {
            max_connections: 1,
            outgoing_capacity: 8,
            outgoing_bytes_capacity: 16_384,
            link_capacity: 2,
            max_flight_resident_bytes: 4096,
        })
        .expect("embedded plan");
        let client = client::Plan::<DIALER>::new(plan);
        let mut config = Config {
            max_request_body_bytes: usize::from(expected_error.is_none()),
            ..Config::default()
        };
        config.protocol.streams.capacity = 1;
        let server = Embedded::<LISTENER>::try_new(
            tls,
            config_with_streams(2, None).expect("server streams"),
            plan,
            config,
        )
        .expect("server plan");
        let forward = plan.link::<19, DIALER, LISTENER>(client_addr, server_addr);
        let reverse = plan.link::<20, LISTENER, DIALER>(server_addr, client_addr);
        let config = Needs::from_tree(&(&client, &server, &forward, &reverse))
            .expect("claims")
            .include(
                Claim::new(settings::Routes::EMPTY).schedule(demands::Schedule::fixed::<1, 1>()),
            )
            .expect("task claims")
            .apply_to(settings::Config::default())
            .expect("driver config");
        let mut rejected = Spec::new(path, b"x");
        rejected.data.split = Some(0);
        rejected.data.defer_tail = true;
        rejected.data.boundary = if stop_before_fin {
            Boundary::More
        } else {
            Boundary::End
        };
        rejected.expect.status = status;
        rejected.expect.error = expected_error;
        rejected.expect.input_finished = false;
        let mut quick = Spec::new(b"/async/quick", b"");
        quick.dispatch = Dispatch::AfterFailure;
        let proof = Rc::new(Shared::new(Proof::new(
            vec![rejected, quick],
            Instant::now() + Duration::from_secs(5),
            None,
        )));
        let storage = StoragePlan {
            cancel_dropped: Rc::clone(&proof.read().expect("proof").cancel.dropped),
            errors: Rc::clone(&proof.failures),
            stream: Rc::clone(&proof.read().expect("proof").stream),
        };
        let delivered = Cell::new(false);
        let mut create = Attempt::new();
        Executor::new(&mut create, config)
        .unwrap_or_else(|error| panic!("driver: {:?}", error.kind()))
        .with_plan(storage::Scope::new((
            forward.try_arena().expect("forward arena"), reverse.try_arena().expect("reverse arena"),
        )))
        .run(|executor| {
            executor.scope(|mut scope| {
                let state = storage::Direct::build(storage);
                Brand::with(|brand| {
                    let router = SrvApp::multiplexed::<wire::Identity, KeyTag<LISTENER>, _>(
                        brand, &state, sark_fiber::Config::new(1),
                    );
                    let (forward, reverse) = *scope.storage();
                    let runtime = {
                        let mut driver = scope.driver();
                        let seed = hash::Seed::new(driver.driver_ref().random_seed());
                        let server = server.try_bind(router, Observed::new(Probe(
                            Rc::clone(&proof), |cause: &dyn std::fmt::Debug| panic!("H3: {cause:?}"),
                        )), &mut driver).expect("server bind");
                        let client = client.bind(Client::new(seed, Rc::clone(&proof)), &mut driver)
                            .expect("client bind");
                        let forward = Link::try_new(forward, &mut driver).expect("forward bind");
                        let reverse = Link::try_new(reverse, &mut driver).expect("reverse bind");
                        let mut auth = Some(auth);
                        let mut phase = 0;
                        let proof = Rc::clone(&proof);
                        let delivered = &delivered;
                        Native { client, server, forward, reverse,
                            run: move |mut client, mut server, mut forward, mut reverse, now| {
                                if let Some(auth) = auth.take() {
                                    client.connect(now, server_addr, &auth,
                                        config_with_streams(2, None).expect("client streams"), routing::Plain)
                                        .expect("connect");
                                }
                                assert!(client.relay(&mut server, &mut forward, now).is_ok(), "forward relay");
                                // Deliver STOP after late DATA; FIN cases also wait for server EOF.
                                if phase == 0 || phase == 3 || (phase == 2 && stop_before_fin) {
                                    assert!(server.relay(&mut client, &mut reverse, now).is_ok(), "reverse relay");
                                }
                                let request = proof.read().expect("proof").replies.streams[0];
                                let Some((_, stream)) = request else { return control::Flow::More; };
                                if phase == 0 { phase = 1; return control::Flow::More; }
                                let client_id = client.sole().expect("same client connection");
                                let server_id = server.sole().expect("same server connection");
                                if phase == 1 && proof.read().expect("proof").trace.events.iter().any(
                                    |event| matches!(event, Event::ExchangeRetired(exchange) if exchange.stream() == stream),
                                ) {
                                    server.with(now, server_id, |mut selected| {
                                        let (_, state, quic) = selected.split();
                                        assert!(matches!(state.phase(quic, stream).expect("request phase"), Phase::Missing), "retired response retained its request slot");
                                        let recv = quic.streams().view().resolve_recv(stream).expect("receive identity");
                                        assert!(!quic.streams().view().is_recv_eof(recv), "request FIN was not sent");
                                    }).expect("server scope");
                                    client.with(now, client_id, |mut selected| {
                                        let (_, link, quic) = selected.split();
                                        let send = quic.streams().view().resolve_send(stream).expect("send identity");
                                        assert!(quic.streams().view().stopped(send).is_none());
                                        let (id, bytes, boundary) = proof.write().expect("proof").tail(0)
                                            .expect("tail").expect("unsent tail");
                                        link.h3.send().data(id, bytes, boundary).expect("late DATA");
                                        link.h3.egress().settle_scope(quic).expect("late DATA queue");
                                    }).expect("client scope");
                                    phase = 2;
                                } else if phase == 2 {
                                    let eof = server.with(now, server_id, |mut selected| {
                                        let (_, state, quic) = selected.split();
                                        assert!(!quic.view().lifecycle().is_closing(), "late DATA closed the connection");
                                        let recv = quic.streams().view().resolve_recv(stream).expect("receive identity");
                                        if !quic.streams().view().is_recv_eof(recv) { return false; }
                                        assert!(matches!(state.phase(quic, stream).expect("request phase"), Phase::Missing));
                                        if let Some(handle) = quic.streams().resident().handle(stream) {
                                            assert!(matches!(quic.streams().binding().try_release(handle), Ok(bindings::Release::Vacant)));
                                        }
                                        true
                                    }).expect("server scope");
                                    if eof {
                                        delivered.set(true);
                                        client.with(now, client_id, |mut selected| {
                                            let (_, link, quic) = selected.split();
                                            link.try_send(&proof, quic, Dispatch::AfterFailure).expect("capacity reused");
                                        }).expect("followup scope");
                                        phase = 3;
                                    }
                                }
                                control::Flow::More
                            },
                        }
                    };
                    match scope.with_app(&mut session::Completion::new(), runtime, |mut app| {
                        proof.failures.try_wait(&mut app)
                    }) {
                        Ok(Ok(_)) => assert!(delivered.get(), "late DATA and input termination actually consumed"),
                        Ok(Err(error)) => panic!("execution: {error:?}"),
                        Err(error) => panic!("teardown: {:?}", error.cause()),
                    }
                });
            }, recovery::Policy::new(
                |event| panic!("unexpected retirement: {event:?}"),
                |error: Recovery<'_, '_>| -> Infallible { panic!("{:?}", error.cause()) },
            )).as_ref().map_err(|error| error.cause()).expect("driver retirement");
        });
        let proof = Rc::into_inner(proof)
            .expect("released fixture")
            .into_inner();
        assert!(proof.network.failures.values.take().is_empty());
        assert!(proof.replies.status_ok[1]);
        if expected_error.is_some() || stop_before_fin {
            assert!(proof.replies.error_ok[0]);
        }
        if let Some(body) = expected_body {
            assert!(proof.replies.status_ok[0]);
            assert_eq!(proof.replies.bodies[0], body);
        }
        assert_eq!(proof.replies.bodies[1], b"quick");
        assert!(proof.replies.finished.iter().all(|finished| *finished));
        for (_, stream) in proof.replies.streams.into_iter().flatten() {
            assert_eq!(
                proof
                    .trace
                    .events
                    .iter()
                    .filter(|event| matches!(
                        event, Event::ExchangeRetired(exchange) if exchange.stream() == stream,
                    ))
                    .count(),
                1
            );
        }
        let required = Capability::REQUEST_BOOK_ENTRIES
            .union(Capability::PRE_ADMISSION_BYTES)
            .union(Capability::H3_RESPONSE_OWNER_BYTES);
        assert!(!proof.trace.audits.is_empty());
        assert!(
            proof
                .trace
                .audits
                .iter()
                .all(|(_, snapshot)| snapshot.is_zero(required))
        );
    }
}
