pub mod common;
use std::{io::Write, net::TcpListener, rc::Rc, str, time::Duration};

use common::{
    app::{
        self,
        saturate::{FRAME_PAYLOAD, SATURATING_PAYLOAD, Saturating},
    },
    client::topology,
    peer::Peer as _,
};
use dope::core::{
    driver::{life::recovery, settings},
    io::Reclaimed,
};
use dope_test::harness::{
    observe::Trace,
    scenario::run::{Entry, Task as _},
    signals::{Gate, Progress as _},
};
use sark_ws::{
    client,
    client::{channel, mode},
    frame,
};

#[test]
fn terminal_frames_keep_diagnostics() {
    for (wire, expected) in common::protocol::TERMINAL {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind peer");
        let address = listener.local_addr().expect("peer address");
        let peer = Entry(move || {
            let (mut stream, _) = listener.accept().expect("accept client");
            let request = app::Upgrade::new(&stream).try_head().expect("upgrade head");
            let accept = app::Head::new(&request).try_accept().expect("upgrade key");
            app::Upgrade::new(&stream)
                .try_respond(&accept)
                .expect("upgrade response");
            stream.write_all(wire).expect("malformed frame");
            std::io::copy(&mut stream, &mut std::io::sink()).expect("client terminates");
        });
        peer.with(|| {
            let seen = common::client::Probe::new(
                address,
                client::Config::new("localhost", "/").expect("configuration"),
            )
            .run(
                common::client::Goal::Close,
                recovery::Policy::new(|_| {}, common::recover),
            );
            assert!(seen.is_open());
            assert!(matches!(
                seen.close(),
                Some(client::CloseReason::Decode(client::Fault::Frame(actual)))
                    if *actual == expected
            ));
        })
        .expect("peer");
    }
}

#[test]
fn outbound_retains_layout() {
    use common::app::{self, Closing, saturate::FRAGMENTS};

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind peer");
    let address = listener.local_addr().expect("peer address");
    let peer = Entry(move || {
        let (mut stream, _) = listener.accept().expect("accept client");
        let request = app::Upgrade::new(&stream)
            .try_head()
            .expect("WebSocket operation");
        let accept = app::Head::new(&request)
            .try_accept()
            .expect("WebSocket operation");
        app::Upgrade::new(&stream)
            .try_respond(&accept)
            .expect("WebSocket operation");
        let mut frames = Vec::new();
        for (index, payload) in FRAGMENTS.chunks(126).enumerate() {
            let (fin, opcode, actual) = stream.try_client(&mut frames).expect("client frame");
            assert_eq!((fin, opcode), (index == 2, if index == 0 { 2 } else { 0 }));
            assert_eq!(actual, payload);
        }
        assert_eq!(
            stream.try_client(&mut frames).expect("client frame"),
            (true, 2, Vec::new())
        );
        stream.write_all(b"\x88\x00").expect("close peer");
        assert_eq!(
            stream.try_client(&mut frames).expect("client frame"),
            (true, 8, Vec::new())
        );
    });
    peer.with(|| {
        let opened = Gate::new();
        let closed = Gate::new();
        let messages = Gate::new();
        let config = client::Config::new("localhost", "/");
        let config = config.expect("WebSocket configuration");
        let config = config.message(FRAGMENTS.len() * 2).outbound(126);
        topology::Plan::new(address, config, 1, 1)
            .enter(
                |domain| {
                    domain.run(
                        channel::App::<_, mode::Egress>::new(Closing {
                            open: &|_| {},
                            close: &|closed| {
                                assert!(matches!(
                                    closed,
                                    client::Closed {
                                        reason: client::CloseReason::RemoteClose { code: None, .. },
                                        transmit: None,
                                    }
                                ))
                            },
                            opened: &opened,
                            closed: &closed,
                            messages: &messages,
                        }),
                        |mut app| {
                            app.wait()
                                .expect("WebSocket operation")
                                .as_ref()
                                .map_err(|cause| cause.cause())
                                .expect("WebSocket wait");
                            let handle = app.client();
                            let driver = handle.try_driver().expect("first egress driver");
                            let alias = handle;
                            assert!(matches!(alias.try_driver(), Err(client::send::Claimed)));
                            drop(driver);
                            drop(alias.try_driver().expect("released egress driver"));
                            for data in [FRAGMENTS.as_slice(), &[]] {
                                app.drive(handle.send_binary(data))
                                    .expect("send driver")
                                    .expect("WebSocket operation");
                            }
                            assert!(
                                app.try_until(&closed, 1).expect("close driver")
                                    == dope_test::fibers::Wait::Reached
                            );
                        },
                    )
                },
                recovery::Policy::new(|_| {}, common::recover),
                |result| result.expect("WebSocket execution"),
            )
            .as_ref()
            .map_err(|cause| cause.kind())
            .expect("WebSocket operation");
        assert_eq!(messages.hits(), 0);
    })
    .expect("peer");
}

#[test]
fn controls_wait_capacity() {
    let mut retired = Vec::new();
    {
        let bound = TcpListener::bind("127.0.0.1:0").expect("bind raw WebSocket peer");
        let addr = bound.local_addr().expect("raw peer address");
        let peer = Entry(move || {
            let (mut stream, _) = bound.accept().expect("accept WebSocket client");
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("read timeout");
            let (req, mut buf) = stream
                .try_head(&mut Vec::new())
                .expect("read client handshake");
            let accept = app::Head::new(&req)
                .try_accept()
                .expect("client WebSocket key");
            let head = format!(
                "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n",
                str::from_utf8(&accept).expect("ASCII accept")
            )
            .into_bytes();
            stream.write_all(&head).expect("write handshake");
            let mut frames = Vec::new();
            let ping = frame::Plan::new(0x9, true, 6, false).expect("ping plan");
            frames.resize(ping.wire_len(), 0);
            ping.bind(b"causal", None)
                .expect("ping payload")
                .bind(&mut frames)
                .expect("ping capacity")
                .encode();
            let close = [0x03, 0xe8, b'b', b'y', b'e'];
            let plan = frame::Plan::new(0x8, true, close.len(), false).expect("close plan");
            let start = frames.len();
            frames.resize(start + plan.wire_len(), 0);
            plan.bind(&close, None)
                .expect("close payload")
                .bind(&mut frames[start..])
                .expect("close capacity")
                .encode();
            stream.write_all(&frames).expect("write ping and close");

            let (fin, opcode, data) = stream.try_client(&mut buf).expect("client frame");
            assert!(!fin);
            assert_eq!(opcode, 0x2);
            assert_eq!(data.len(), FRAME_PAYLOAD);
            assert!(data.iter().all(|byte| *byte == 0x5a));
            let (fin, opcode, data) = stream.try_client(&mut buf).expect("client frame");
            assert!(fin);
            assert_eq!(opcode, 0x0);
            assert_eq!(data.len(), FRAME_PAYLOAD);
            assert!(data.iter().all(|byte| *byte == 0x5a));
            assert_eq!(
                stream.try_client(&mut buf).expect("client frame"),
                (true, 0xa, b"causal".to_vec())
            );

            assert_eq!(
                stream.try_client(&mut buf).expect("client frame"),
                (true, 0x8, close.to_vec())
            );
        });

        peer.with(|| {
            let ping = Gate::new();
            let close = Gate::new();
            let reason = Rc::new(Trace::<_, 1>::new());
            let config = client::Config::new("localhost", "/");
            let config = config.expect("WebSocket configuration");
            let config = config.message(SATURATING_PAYLOAD).outbound(FRAME_PAYLOAD);
            topology::Plan::new(addr, config, 1, 1)
                .with_backoff((Duration::from_millis(10), Duration::from_millis(100)))
                .enter(
                    |domain| {
                        domain.run(
                            channel::App::<_, mode::Egress>::new(Saturating {
                                ping: ping.clone(),
                                close: close.clone(),
                                reason: Rc::clone(&reason),
                                open: &|send| {
                                    let data = vec![0x5a; SATURATING_PAYLOAD + 1];
                                    assert!(matches!(send.binary(&data), Err(client::Error::Payload { actual, maximum: SATURATING_PAYLOAD }) if actual == SATURATING_PAYLOAD + 1));
                                    send.binary(&data[..SATURATING_PAYLOAD])
                                        .expect("WebSocket operation");
                                },
                                message: &|msg| {
                                    if let client::Message::Ping(data) = msg {
                                        assert_eq!(data, b"causal");
                                    }
                                },
                            }),
                            |mut app| {
                                assert!(
                                    app.try_until(&ping, 1).expect("signal driver")
                                        == dope_test::fibers::Wait::Reached,
                                    "WebSocket ping reply deadline"
                                );
                                assert!(
                                    app.try_until(&close, 1).expect("signal driver")
                                        == dope_test::fibers::Wait::Reached,
                                    "WebSocket close reply deadline"
                                );
                                assert_eq!(ping.hits(), 1);
                                assert_eq!(close.hits(), 1);
                            },
                        )
                    },
                    recovery::Policy::new(
                        |event| retired.push(event),
                        common::recover,
                    ),
                    |result| result.expect("WebSocket execution"),
                )
                .as_ref().map_err(|cause| cause.kind()).expect("WebSocket operation");

            assert!(matches!(
                reason.values().expect("close observations").as_ref(),
                [client::Closed {
                    reason: client::CloseReason::RemoteClose {
                        code: Some(1000),
                        reason,
                    },
                    transmit: None,
                }] if reason == "bye"
            ));

        })
        .expect("raw WebSocket peer");
    };
    assert!(
        retired
            .iter()
            .all(|event| matches!(event, Reclaimed::Shutdown)),
        "unexpected reclaimed WebSocket client completion: {retired:?}"
    );
}

#[test]
fn borrowed_handler_retires() {
    use common::app::{
        self,
        borrowed::{Borrowed, Event},
    };

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind borrowed peer");
    let address = listener.local_addr().expect("borrowed peer address");
    let peer = Entry(move || {
        let (mut stream, _) = listener.accept().expect("accept borrowed client");
        let request = app::Upgrade::new(&stream)
            .try_head()
            .expect("WebSocket operation");
        app::Upgrade::new(&stream)
            .try_respond(
                &app::Head::new(&request)
                    .try_accept()
                    .expect("WebSocket operation"),
            )
            .expect("WebSocket operation");
        stream
            .write_all(b"\x81\x04loan\x88\x02\x03\xe8")
            .expect("send borrowed events");
        std::io::copy(&mut stream, &mut std::io::sink()).expect("borrowed client terminates");
    });

    peer.with(|| {
        let events = Trace::new();
        let closed = Gate::new();
        let mut retired = Vec::new();
        let config = client::Config::new("localhost", "/");
        let config = config.expect("WebSocket configuration");
        let config = config.frame(4);
        topology::Plan::new(address, config, 1, 1)
            .with_receive(settings::Receive::fixed::<1024, { 64 * 1024 }>())
            .enter(
                |domain| {
                    domain.run(
                        channel::App::<_, mode::Egress>::new(Borrowed {
                            events: &events,
                            closed: &closed,
                            message: &|message| {
                                assert!(matches!(message, client::Message::Text("loan")))
                            },
                        }),
                        |mut app| {
                            assert!(
                                app.try_until(&closed, 1).expect("borrowed close driver")
                                    == dope_test::fibers::Wait::Reached
                            );
                        },
                    )
                },
                recovery::Policy::new(|event| retired.push(event), common::recover),
                |result| result.expect("WebSocket execution"),
            )
            .as_ref()
            .map_err(|cause| cause.kind())
            .expect("WebSocket operation");

        assert!(matches!(
            events.values().expect("borrowed observations").as_ref(),
            [Event::Opened, Event::Message, Event::Closed(client::Closed { reason: client::CloseReason::RemoteClose { code: Some(1000), reason }, transmit: None }), Event::Dropped] if reason.is_empty()
        ));
        assert!(
            retired
                .iter()
                .all(|event| matches!(event, Reclaimed::Shutdown))
        );
    })
    .expect("borrowed peer");
}
