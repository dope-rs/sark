pub mod common;

use std::{
    io::{Read as _, Write as _},
    net::TcpListener,
    pin::{Pin, pin},
    task::Poll,
};

use common::{
    app::{self, Closing},
    client::topology,
    peer::Peer as _,
};
use dope::core::driver::life::recovery;
use dope_fiber::{
    abi::{Task, poll_fn},
    context,
};
use dope_test::harness::{
    scenario::run::{Entry, Task as _},
    signals::{Gate, Progress as _},
};
use o3::base::cell::local::Access;
use sark_ws::{
    client::{self, channel, mode},
    inbound::Close,
};

#[test]
fn close_seals_generation() {
    for direct in [false, true] {
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
            let (fin, opcode, payload) = stream.try_client(&mut frames).expect("client close");
            assert_eq!((fin, opcode, payload.len()), (true, 8, 0));
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
        peer.with(|| {
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
                                close: &|closed| {
                                    assert!(matches!(
                                        closed,
                                        client::Closed {
                                            reason: client::CloseReason::RemoteClose {
                                                code: None,
                                                ..
                                            },
                                            transmit: None,
                                        }
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
                                    let mut close = pin!(Task::new(
                                        handle.close(Close::parse(&[]).expect("empty close"))
                                    ));
                                    let mut send = handle.try_driver().expect("exclusive sender");
                                    app.drive(poll_fn(
                                        |mut cx: Pin<&mut context::Context<'_, '_>>| {
                                            match cx.as_mut().poll(close.as_mut()) {
                                                Poll::Pending => return Poll::Pending,
                                                Poll::Ready(result) => {
                                                    result.expect("WebSocket operation")
                                                }
                                            }
                                            let wake = cx.as_ref().completion();
                                            assert!(matches!(
                                                send.poll_text(
                                                    Access::new(cx.as_mut().region_token()),
                                                    "late",
                                                    wake
                                                ),
                                                Poll::Ready(Err(client::Error::Closing))
                                            ));
                                            Poll::Ready(())
                                        },
                                    ))
                                    .expect("close driver");
                                }
                                assert!(
                                    app.try_until(&closed, 1).expect("close driver")
                                        == dope_test::fibers::Wait::Reached
                                );
                                if !direct {
                                    app.wait()
                                        .expect("WebSocket operation")
                                        .as_ref()
                                        .map_err(|cause| cause.cause())
                                        .expect("WebSocket wait");
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
                .expect("WebSocket operation");
            assert_eq!(messages.hits(), 1);
        })
        .expect("peer");
    }
}

#[test]
fn close_rejection_open() {
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
        stream.read_to_end(&mut Vec::new()).expect("retire peer");
    });
    peer.with(|| {
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
                            app.wait()
                                .expect("WebSocket operation")
                                .as_ref()
                                .map_err(|cause| cause.cause())
                                .expect("WebSocket wait");
                        },
                    )
                },
                recovery::Policy::new(|_| {}, common::recover),
                |result| result.expect("WebSocket execution"),
            )
            .expect("WebSocket operation");
        assert_eq!(closed.hits(), 1);
        assert_eq!(opened.hits(), 1);
        assert_eq!(messages.hits(), 0);
    })
    .expect("peer");
}
