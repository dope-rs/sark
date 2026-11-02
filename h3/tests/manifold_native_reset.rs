pub mod common;

use common::conn::{
    bridge::Pair,
    native::{CLIENT, Native, Receiver, SERVER},
};
use dope::{
    core::driver::{
        life::{creation::Attempt, recovery},
        settings::{self, claim::Needs},
        storage,
    },
    runtime::{
        control,
        executor::{Executor, session},
        shutdown,
    },
};
use dope_quic::{
    config,
    conn::{life::tls::clients::Authority, stream::access::bindings},
    errors::Code,
    runtime::{
        embedded::{self, client, link::Link, plan, server},
        routing,
    },
    wire::stream::Direction,
};
use sark_h3::{
    conn::{
        event::{Event, Origin, Role},
        io::Step,
    },
    manifold::Session,
};

const CODE: Code = Code::from_u16(42);
use std::{cell::Cell, convert::Infallible};

#[test]
fn first_terminal_survives_native_callback() {
    let identity = config::raw::Identity::from_seed(&[0x71; 32]).expect("identity");
    let auth = Authority::try_new(
        config::raw::Identity::client(identity.public_key()).expect("client TLS"),
        SERVER.ip().into(),
    )
    .expect("client authority");
    let plan = plan::Plan::try_new(embedded::Config {
        max_connections: 1,
        outgoing_capacity: 8,
        outgoing_bytes_capacity: 16_384,
        link_capacity: 2,
        max_flight_resident_bytes: 4096,
    })
    .expect("embedded plan");
    let client = client::Plan::<1>::new(plan);
    let server = server::Plan::<2>::new(
        identity.server().expect("server TLS"),
        Pair::try_config().expect("QUIC options"),
        plan,
    )
    .expect("server plan");
    let forward = plan.link::<3, 1, 2>(CLIENT, SERVER);
    let reverse = plan.link::<4, 2, 1>(SERVER, CLIENT);
    let cfg = Needs::from_tree(&(&client, &server, &forward, &reverse))
        .expect("claims")
        .apply_to(settings::Config::default())
        .expect("driver config");
    let (source, stop) = shutdown::Pair::new().expect("shutdown pair").split();
    let done = Cell::new(false);
    let mut create = Attempt::new();
    let executor = Executor::new(&mut create, cfg)
        .unwrap_or_else(|error| panic!("native driver: {:?}", error.kind()));
    executor
        .shutdown(source)
        .expect("shutdown source")
        .with_plan(storage::Scope::new((
            forward.try_arena().expect("forward arena"),
            reverse.try_arena().expect("reverse arena"),
        )))
        .run(|executor| {
            executor
                .scope(
                    |mut scope| {
                        let (forward, reverse) = *scope.storage();
                        let app = {
                            let mut driver = scope.driver();
                            let mut auth = Some(auth);
                            let mut phase = 0;
                            let mut original = None;
                            let mut stop = Some(stop);
                            let done = &done;
                            Native::new(
                                client
                                    .bind(
                                        Receiver::new(Session::new().expect("client H3")),
                                        &mut driver,
                                    )
                                    .expect("client bind"),
                                server
                                    .bind(
                                        Receiver::new(
                                            Session::with_role(Role::Server).expect("server H3"),
                                        ),
                                        &mut driver,
                                    )
                                    .expect("server bind"),
                                Link::try_new(forward, &mut driver).expect("forward bind"),
                                Link::try_new(reverse, &mut driver).expect("reverse bind"),
                                move |mut client, mut server, mut forward, mut reverse, now| {
                                    if phase == 4 {
                                        return control::Flow::Idle;
                                    }
                                    if let Some(auth) = auth.take() {
                                        client
                                            .connect(
                                                now,
                                                SERVER,
                                                &auth,
                                                Pair::try_config().expect("QUIC options"),
                                                routing::Plain,
                                            )
                                            .expect("native connect");
                                    }
                                    // Observe Stopped before its automatic RESET can reach the peer.
                                    if phase != 2 {
                                        assert!(
                                            client.relay(&mut server, &mut forward, now).is_ok(),
                                            "client relay"
                                        );
                                    }
                                    assert!(
                                        server.relay(&mut client, &mut reverse, now).is_ok(),
                                        "server relay"
                                    );
                                    assert!(!client.handler().certificate_failed);
                                    assert!(
                                        client.handler().failure.is_none(),
                                        "{:?}",
                                        client.handler().failure
                                    );
                                    assert!(!server.handler().generation_changed);
                                    assert!(!server.handler().certificate_failed);
                                    assert!(
                                        server.handler().failure.is_none(),
                                        "{:?}",
                                        server.handler().failure
                                    );
                                    if !client.handler().ready || !server.handler().ready {
                                        return control::Flow::More;
                                    }
                                    let client_id = client.sole().expect("native client");
                                    let server_id = server.sole().expect("native server");
                                    match phase {
                                        0 => {
                                            client
                                                .with(now, client_id, |mut selected| {
                                                    let (_, _, quic) = selected.split();
                                                    let mut streams = quic.streams();
                                                    let handle = streams
                                                        .send()
                                                        .open_handle(Direction::Uni)
                                                        .expect("peer uni");
                                                    original = Some(handle.local());
                                                    assert!(matches!(
                                                        streams.binding().try_release(handle),
                                                        Ok(bindings::Release::Vacant)
                                                    ));
                                                    let send = streams
                                                        .view()
                                                        .resolve_send(handle.id())
                                                        .expect("peer send");
                                                    streams
                                                        .send()
                                                        .send(send, &[0x21])
                                                        .expect("first STREAM");
                                                })
                                                .expect("client scope");
                                            phase = 1;
                                        }
                                        1 => {
                                            let Some((id, key)) = server.handler().first else {
                                                return control::Flow::More;
                                            };
                                            let key = key.expect("native Readable generation");
                                            server
                                                .with(now, server_id, |mut selected| {
                                                    let (_, _, quic) = selected.split();
                                                    let handle = quic
                                                        .streams()
                                                        .resident()
                                                        .handle_key(key)
                                                        .expect("first STREAM resident");
                                                    assert!(matches!(
                                                        quic.streams()
                                                            .binding()
                                                            .try_release(handle),
                                                        Ok(bindings::Release::Vacant)
                                                    ));
                                                    let mut streams = quic.streams();
                                                    let recv = streams
                                                        .view()
                                                        .resolve_recv(id)
                                                        .expect("peer receiver");
                                                    streams
                                                        .receive()
                                                        .stop(recv, CODE)
                                                        .expect("peer STOP_SENDING");
                                                })
                                                .expect("server pre-admission stop");
                                            phase = 2;
                                        }
                                        2 => {
                                            let Some((id, code, key)) = client.handler().terminal
                                            else {
                                                return control::Flow::More;
                                            };
                                            let original = original.expect("original local uni");
                                            assert_eq!(id, original.id());
                                            assert_eq!(key, Some(original.key()));
                                            assert_eq!(code, CODE);
                                            client
                                                .with(now, client_id, |mut selected| {
                                                    let (_, _, quic) = selected.split();
                                                    assert!(quic.view().lifecycle().is_ready());
                                                    let streams = quic.streams();
                                                    let send = streams.view().resolve_send(id).expect("stopped sender");
                                                    assert_eq!(streams.view().stopped(send), Some(CODE));
                                                })
                                                .expect("client stopped scope");
                                            phase = 3;
                                        }
                                        3 => {
                                            let original = original.expect("original local uni");
                                            let pending = client
                                                .with(now, client_id, |mut selected| {
                                                    let (_, _, quic) = selected.split();
                                                    assert!(quic.view().lifecycle().is_ready());
                                                    let streams = quic.streams();
                                                    let send = streams.view().resolve_send(original.id()).expect("stopped sender");
                                                    streams.view().stopped(send)
                                                })
                                                .expect("client ACK scope");
                                            if let Some(code) = pending {
                                                assert_eq!(code, CODE);
                                                return control::Flow::More;
                                            }
                                            let Some((id, code, key)) = server.handler().terminal
                                            else {
                                                return control::Flow::More;
                                            };
                                            assert_eq!(Some((id, key)), server.handler().first);
                                            assert_eq!(code, CODE);
                                            let key = key.expect("native Reset generation");
                                            server
                                                .with(now, server_id, |mut selected| {
                                                    let (_, h3, quic) = selected.split();
                                                    assert!(quic.view().lifecycle().is_ready());
                                                    assert!(
                                                        quic.streams()
                                                            .resident()
                                                            .handle_key(key)
                                                            .is_some()
                                                    );
                                                    let event = h3
                                                    .ingress()
                                                    .consume(quic, |event| {
                                                        matches!(
                                                        event,
                                                        Event::Reset {
                                                            stream_id,
                                                            error_code,
                                                            origin: Origin::Transport,
                                                            ..
                                                        } if *stream_id == id && *error_code == CODE
                                                    )
                                                    })
                                                    .expect("later-scope reset");
                                                    assert!(matches!(event, Step::Item(true)));
                                                    assert!(matches!(
                                                        h3.ingress()
                                                            .consume(quic, Event::completion)
                                                            .expect("empty after reset"),
                                                        Step::Idle
                                                    ));
                                                    assert!(
                                                        quic.streams()
                                                            .resident()
                                                            .handle_key(key)
                                                            .is_none()
                                                    );
                                                })
                                                .expect("server consume scope");
                                            client
                                                .with(now, client_id, |mut selected| {
                                                    let (_, h3, quic) = selected.split();
                                                    assert!(quic.streams().resident().handle_key(original.key()).is_some());
                                                    let event = h3.ingress().consume(quic, |event| matches!(event,
                                                        Event::Stopped { stream_id, error_code, origin: Origin::Transport }
                                                        if *stream_id == original.id() && *error_code == CODE
                                                    )).expect("later-scope stopped");
                                                    assert!(matches!(event, Step::Item(true)));
                                                    assert!(matches!(h3.ingress().consume(quic, Event::completion).expect("empty after stopped"), Step::Idle));
                                                    assert!(quic.streams().resident().handle_key(original.key()).is_none());
                                                })
                                                .expect("client consume scope");
                                            assert_eq!(server.handler().terminals, 1);
                                            assert_eq!(client.handler().terminals, 1);
                                            done.set(true);
                                            stop.take()
                                                .expect("shutdown owner")
                                                .fire()
                                                .expect("shutdown");
                                            phase = 4;
                                        }
                                        _ => return control::Flow::Idle,
                                    }
                                    control::Flow::More
                                },
                            )
                        };
                        match scope.with_app(&mut session::Completion::new(), app, |app| {
                            app.run_for(dope_test::harness::scenario::TIMEOUT)
                        }) {
                            Ok(Ok(_)) => {
                                assert!(done.get(), "native terminals delivered and retired")
                            }
                            Ok(Err(error)) => panic!("embedded execution: {error:?}"),
                            Err(error) => panic!("embedded teardown: {:?}", error.cause()),
                        }
                    },
                    recovery::Policy::new(
                        |event| assert!(matches!(event, dope::core::io::Reclaimed::Shutdown)),
                        |error: recovery::Recovery<'_, '_>| -> Infallible {
                            panic!("native recovery: {:?}", error.cause())
                        },
                    ),
                )
                .as_ref()
                .map_err(|error| error.cause())
                .expect("driver retirement");
        });
}
