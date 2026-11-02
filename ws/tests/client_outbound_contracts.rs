pub mod common;

use std::{
    io::{Read as _, Write as _},
    net::TcpListener,
    sync::mpsc,
    thread,
    time::Duration,
};

use common::{
    app::{self, Closing},
    client::topology,
    peer::Peer as _,
};
use dope::core::driver::life::recovery;
use dope_test::harness::signals::{Gate, Progress as _};
use sark_ws::{
    client::{self, channel, mode},
    inbound::Close,
};

#[test]
fn close_seals_generation() {
    for direct in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind peer");
        let address = listener.local_addr().expect("peer address");
        let (release, wait) = mpsc::channel();
        let peer = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept client");
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .expect("read timeout");
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
            let (fin, opcode, payload) = stream.try_client(&mut frames).expect("client close");
            assert_eq!((fin, opcode, payload.len()), (true, 8, 0));
            wait.recv_timeout(Duration::from_secs(3))
                .expect("send gate checked");
            stream
                .write_all(b"\x89\x04ping")
                .expect("ping closing client");
            assert_eq!(
                stream.try_client(&mut frames).expect("closing pong"),
                (true, 0xa, b"ping".to_vec())
            );
            stream.write_all(b"\x88\x00").expect("acknowledge close");
            stream.read_to_end(&mut frames).expect("closed transport");
            assert!(
                frames.is_empty(),
                "no data or duplicate close follows close"
            );
            if !direct {
                let (mut stream, _) = listener.accept().expect("accept next generation");
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("read timeout");
                let request = app::Upgrade::new(&stream)
                    .try_head()
                    .expect("WebSocket operation");
                let accept = app::Head::new(&request)
                    .try_accept()
                    .expect("WebSocket operation");
                app::Upgrade::new(&stream)
                    .try_respond(&accept)
                    .expect("WebSocket operation");
                let (opcode, payload) = stream.try_next(&mut frames).expect("next generation data");
                assert_eq!((opcode, payload.len()), (1, 4));
                stream
                    .write_all(b"\x88\x00")
                    .expect("finish next generation");
                stream
                    .read_to_end(&mut frames)
                    .expect("retire next generation");
            }
        });
        let opened = Gate::new();
        let closed = Gate::new();
        let messages = Gate::new();
        let config = client::Config::new("localhost", "/");
        let config = config.expect("WebSocket configuration");
        topology::Plan::new(address, config, 1, 1)
            .enter(
                |domain| {
                    domain.run(
                        channel::App::<_, mode::Egress>::new(Closing {
                            open: &|send| {
                                if direct {
                                    send.close(Close::parse(&[]).expect("empty close"))
                                        .expect("WebSocket operation");
                                    assert!(matches!(
                                        send.binary(b"late"),
                                        Err(client::Error::Closing)
                                    ));
                                }
                            },
                            close: &|reason| {
                                assert!(matches!(
                                    reason,
                                    client::CloseReason::RemoteClose { code: None, .. }
                                ))
                            },
                            opened: &opened,
                            closed: &closed,
                            messages: &messages,
                        }),
                        |mut app| {
                            assert!(
                                app.try_until(&opened, 1).expect("open driver")
                                    == dope_test::fibers::Wait::Reached
                            );
                            let handle = app.client();
                            if !direct {
                                app.drive(handle.close(Close::parse(&[]).expect("empty close")))
                                    .expect("close driver")
                                    .expect("WebSocket operation");
                            }
                            assert!(matches!(
                                app.drive(handle.send_text("late")).expect("text driver"),
                                Err(client::Error::Closing)
                            ));
                            release.send(()).expect("release close acknowledgement");
                            assert!(
                                app.try_until(&closed, 1).expect("close driver")
                                    == dope_test::fibers::Wait::Reached
                            );
                            if !direct {
                                app.wait(|result| {
                                    result
                                        .as_ref()
                                        .map_err(|cause| cause.cause())
                                        .expect("WebSocket wait");
                                })
                                .expect("WebSocket operation");
                                app.drive(handle.send_text("next"))
                                    .expect("next generation driver")
                                    .expect("WebSocket operation");
                                assert!(
                                    app.try_until(&closed, 2).expect("next close driver")
                                        == dope_test::fibers::Wait::Reached
                                );
                            }
                        },
                    )
                },
                recovery::Policy::new(|_| {}, common::recover),
                |result| result.expect("WebSocket execution"),
            )
            .as_ref()
            .map_err(|cause| cause.kind())
            .expect("WebSocket operation");
        assert_eq!(messages.hits(), 1);
        peer.join().expect("peer");
    }
}

#[test]
fn close_rejection_open() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind peer");
    let address = listener.local_addr().expect("peer address");
    let peer = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept client");
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .expect("read timeout");
        let request = app::Upgrade::new(&stream)
            .try_head()
            .expect("WebSocket operation");
        let accept = app::Head::new(&request)
            .try_accept()
            .expect("WebSocket operation");
        app::Upgrade::new(&stream)
            .try_respond(&accept)
            .expect("WebSocket operation");
        stream.read_to_end(&mut Vec::new()).expect("retire peer");
    });
    let config = client::Config::new("localhost", "/");
    let config = config.expect("WebSocket configuration");
    let config = config.message(1).outbound(1);
    let closed = Gate::new();
    let opened = Gate::new();
    let messages = Gate::new();
    topology::Plan::new(address, config, 1, 1)
        .enter(
            |domain| {
                domain.run(
                    channel::App::<_, mode::Egress>::new(Closing {
                        open: &|send| {
                            loop {
                                match send.binary(b"x") {
                                    Err(client::Error::Admission(_)) => break,
                                    result => {
                                        result.expect("send until admission rejection");
                                    }
                                }
                            }
                            assert!(matches!(
                                send.close(Close::parse(&[]).expect("empty close")),
                                Err(client::Error::Admission(_))
                            ));
                        },
                        close: &|_| {},
                        opened: &opened,
                        closed: &closed,
                        messages: &messages,
                    }),
                    |mut app| {
                        app.wait(|result| {
                            result
                                .as_ref()
                                .map_err(|cause| cause.cause())
                                .expect("WebSocket wait");
                        })
                        .expect("WebSocket operation");
                    },
                )
            },
            recovery::Policy::new(|_| {}, common::recover),
            |result| result.expect("WebSocket execution"),
        )
        .as_ref()
        .map_err(|cause| cause.kind())
        .expect("WebSocket operation");
    assert_eq!(closed.hits(), 1);
    assert_eq!(opened.hits(), 1);
    assert_eq!(messages.hits(), 0);
    peer.join().expect("peer");
}
