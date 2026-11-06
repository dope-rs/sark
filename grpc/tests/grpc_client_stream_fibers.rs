use dope::core::driver::life::recovery;
pub mod common;

use std::convert::Infallible;

use common::{
    codec::Bytes,
    grpc::{
        State,
        call::{calls, egress::streams},
        clients,
        settings::Settings,
    },
};
use dope::core::io::Reclaimed;
use dope_test::{fibers, harness::server::Pair};
use sark_grpc::{
    Code, Reason,
    client::{
        manifold::{
            Error,
            api::{Duplex as _, Recv as _, Request as _, Session as _, Write as _},
        },
        receive::Decoded,
    },
    details::Diagnostic,
    encode::Prost,
    metadata::Metadata,
    server::{config::Limits, service::handler::output::Duplex},
};

#[test]
fn admission_and_retirement() {
    let pair = Pair::bind().expect("gRPC server");
    let server = Settings::new(pair.addr(), Limits::default());
    pair.run(
        move |context, listener| {
            let handler = State::new(
                Duplex::new(streams::Echo, Bytes),
                None,
            );
            let retired = server.try_serve_on(handler, context, listener, common::recover, |result| result.expect("gRPC runtime")).expect("gRPC server runtime");
            assert!(retired.is_empty(), "unexpected server retirement: {retired:?}");
            Ok::<_, Infallible>(())
        },
        |address| {
            let limits = calls::Settings::new(Some(1), Some(1), Some(8), Some(1))
                .try_build().expect("one call and one output owner");
            clients::Plan::new(address, limits, limits.calls()).expect("gRPC client plan")
                .enter(|mut app, client| {
                    fibers::TEST.try_run(&mut app, client.wait_active())
                        .expect("client drive").expect("active gRPC connection");
                    fibers::TEST.try_run(&mut app, dope_fiber::fiber!('_ => async move {
                        let metadata = Metadata::new();
                        let path = b"/proof/Chat".to_vec();
                        assert!(fibers::TEST.poll_once(
                            client.unary(&path, None, &metadata, Bytes, &path),
                        ).await.is_none());
                        let reply = client.unary(&path, None, &metadata, Bytes, &path)
                            .await.expect("pending cancellation returns the sole completion slot");
                        drop(reply);

                        drop(client.open_request(&path, None, &metadata));
                        let abandoned = client.open_request(&path, None, &metadata)
                            .await.expect("request admission");
                        let error = client.open_request(&path, None, &metadata)
                            .await.err().expect("sole call is occupied");
                        assert!(matches!(error, Error::Protocol(status) if status.code() == Code::ResourceExhausted));
                        drop(abandoned.finish(Bytes));

                        let mut upload = client.open_request(&path, None, &metadata)
                            .await.expect("unpolled finish cancels its request");
                        upload.send(Bytes, &b"upload".to_vec()).await.expect("upload message");
                        let reply = upload.finish(Bytes).await.expect("upload response");
                        assert_eq!(reply.as_slice(), b"daolpu");
                        drop(reply);

                        let abandoned = client.open_bidi(&path, None, &metadata)
                            .await.expect("bidi admission");
                        drop(abandoned.finish());
                        let abandoned = client.open_bidi(&path, None, &metadata)
                            .await.expect("unpolled bidi finish cancels its request");
                        let (send, response) = abandoned.split().expect("split authority");
                        drop(send.finish());
                        drop(response);

                        for split in [false, true] {
                            let mut bidi = client.open_bidi(&path, None, &metadata)
                                .await.expect("previous response owners retired");
                            let first = b"one".to_vec();
                            let second = b"two".to_vec();
                            let mut response = if split {
                                let (mut send, response) = bidi.split().expect("split authority");
                                send.send(Bytes, &first).await.expect("split message");
                                send.finish().await.expect("split half-close");
                                response
                            } else {
                                bidi.send(Bytes, &first).await.expect("first message");
                                assert!(fibers::TEST.poll_once(bidi.send(Bytes, &second)).await.is_none());
                                bidi.send(Bytes, &second).await.expect("send resumes after owner retirement");
                                bidi.finish().await.expect("half-close preserves receive authority")
                            };
                            let mut messages = Vec::new();
                            loop {
                                let event = response.recv(Bytes).await.expect("response event");
                                match &*event {
                                    Decoded::Headers { .. } => {}
                                    Decoded::Message { message } => messages.push(message.clone()),
                                    Decoded::Trailers { status } => {
                                        assert_eq!(status.code(), Code::Ok);
                                        break;
                                    }
                                }
                            }
                            let expected = if split { vec![b"eno".to_vec()] } else { vec![b"eno".to_vec(), b"owt".to_vec()] };
                            assert_eq!(messages, expected);
                        }
                    })).expect("stream fibers complete");
                }, recovery::Policy::new(
                    |event| assert!(matches!(event, Reclaimed::Shutdown), "unexpected client retirement: {event:?}"),
                    common::recover,
                ), |result| result.expect("gRPC client completion"))
                .expect("gRPC client runtime");
        },
    ).expect("manifold stream roundtrip");
}

#[test]
fn tiny_input_preserves_control_progress() {
    for (input_bytes, message_bytes) in [(0, 0), (1, 1), (1, 2)] {
        let pair = Pair::bind().expect("gRPC server");
        let server = Settings::new(pair.addr(), Limits::default());
        pair.run(
            move |context, listener| {
                let handler = State::new(Duplex::new(streams::Echo, Bytes), None);
                let retired = server
                    .try_serve_on(handler, context, listener, common::recover, |result| {
                        result.expect("gRPC runtime")
                    })
                    .expect("gRPC server runtime");
                assert!(retired.is_empty(), "unexpected server retirement: {retired:?}");
                Ok::<_, Infallible>(())
            },
            |address| {
                let limits = calls::Settings::try_ingress(1, 1, 8, 1, input_bytes)
                    .expect("bounded response input")
                    .try_bytes(message_bytes)
                    .expect("bounded protobuf message");
                clients::Plan::new(address, limits, 1)
                    .expect("gRPC client plan")
                    .enter(
                        |mut app, client| {
                            fibers::TEST.try_run(&mut app, dope_fiber::fiber!('_ => async move {
                                client.wait_active().await.expect("active gRPC connection");
                                let path = b"/proof/Tiny";
                                let metadata = Metadata::new();
                                drop(client.unary(path, None, &metadata, Prost::<()>::new(), &())
                                    .await.expect("SETTINGS and empty protobuf response"));
                                let message = vec![b'x'; message_bytes.max(1)];
                                let result = client.unary(path, None, &metadata, Bytes, &message).await;
                                if input_bytes == 1 && message_bytes == 1 {
                                    let reply = result.expect("one-byte response fits");
                                    assert_eq!(reply.as_slice(), message.as_slice());
                                } else {
                                    let Error::Protocol(status) = result.err().expect("payload limit") else {
                                        panic!("expected exact payload error");
                                    };
                                    assert_eq!(status.code(), Code::ResourceExhausted);
                                    if message_bytes == 0 {
                                        assert!(matches!(Diagnostic::from(&status), Diagnostic::Local {
                                            reason: Reason::MessageLarge, ..
                                        }));
                                    } else {
                                        assert!(matches!(Diagnostic::from(&status), Diagnostic::Capacity {
                                            reason: Reason::LiveMessage, error
                                        } if error.into_parts().0.get() == 2 && error.into_parts().1 == 1));
                                    }
                                }
                                drop(client.unary(path, None, &metadata, Prost::<()>::new(), &())
                                    .await.expect("payload release preserves empty protobuf progress"));
                            })).expect("tiny input fibers complete");
                        },
                        recovery::Policy::new(
                            |event| assert!(matches!(event, Reclaimed::Shutdown), "unexpected client retirement: {event:?}"),
                            common::recover,
                        ),
                        |result| result.expect("gRPC client completion"),
                    )
                    .expect("gRPC client runtime");
            },
        )
        .expect("tiny input roundtrip");
    }
}

#[test]
fn rejected_calls_reset() {
    use std::time::Duration;

    use common::grpc::native::Peer;
    use dope::core::io::socket::Listener;
    use dope_fiber::task::done::Once;
    use dope_test::harness::GUARD;
    use sark_core::manifold::schedule::Insert;
    use sark_grpc::client::config;
    use sark_h2::{
        frame::{numbers::ErrorCode, payload::Payload},
        stream::Id,
    };

    #[derive(Clone, Copy, Debug)]
    enum Method {
        Unary,
        Request,
        Bidi,
    }

    for method in [Method::Unary, Method::Request, Method::Bidi] {
        let unary = matches!(method, Method::Unary);
        let listener = Listener::bind(([127, 0, 0, 1], 0).into()).expect("native H2 peer");
        let limits = if unary {
            config::Limits::default()
                .try_bytes(0)
                .expect("empty messages allowed")
        } else {
            config::Limits::default()
                .try_calls(1)
                .expect("one gRPC call")
        };
        clients::Plan::new(listener.local_addr(), limits, 2).expect("two H2 slots")
            .enter_peer(listener, |mut app, client, listener| {
                app.drive_for(GUARD, Once::new(client.wait_active()))
                    .expect("client drive").expect("active connection");
                let mut peer = app.drive_for(GUARD, Once::new(Peer::accept(listener, &[
                            0, 0, 0, 4, 0, 0, 0, 0, 0,
                            0, 0, 0, 4, 1, 0, 0, 0, 0,
                ]))).expect("server SETTINGS and acknowledgment");
                let metadata = Metadata::new();
                let retained = if unary {
                    None
                } else {
                    let request = app.drive_for(GUARD, Once::new(
                        client.open_request(b"/proof/Held", None, &metadata),
                    )).expect("first request drive").expect("retained first request");
                    while !app.drive_for(GUARD, Once::new(peer.next(|frame| {
                        matches!(frame, Payload::Headers(head) if head.stream_id() == Id::FIRST_CLIENT)
                    }))).expect("first request on wire") {}
                    Some(request)
                };
                let message = vec![b'x'];
                let error = match method {
                    Method::Unary => app.drive_for(GUARD, Once::new(
                        client.unary(b"/proof/Reject", None, &metadata, Bytes, &message),
                    )).expect("rejected unary drive").err().expect("message exceeds zero-byte limit"),
                    Method::Request => app.drive_for(GUARD, Once::new(
                        client.open_request(b"/proof/Reject", None, &metadata),
                    )).expect("rejected request drive").err().expect("gRPC slot excluded"),
                    Method::Bidi => app.drive_for(GUARD, Once::new(
                        client.open_bidi(b"/proof/Reject", None, &metadata),
                    )).expect("rejected bidi drive").err().expect("gRPC slot excluded"),
                };
                if unary {
                    assert!(matches!(error, Error::Protocol(status) if matches!(
                        Diagnostic::from(&status),
                        Diagnostic::Local { code: Code::ResourceExhausted, reason: Reason::MessageLarge }
                    )));
                } else {
                    assert!(matches!(error, Error::Protocol(status) if status.code() == Code::Internal && matches!(
                        Diagnostic::from(&status),
                        Diagnostic::Insert {
                            cause: Insert::Excluded { stream, value: (), index: 1, capacity: 1 },
                            reset: Ok(()),
                        } if stream.id().into_u32() == 3
                    )));
                }
                let reset = app.drive_for(Duration::from_millis(200), Once::new(
                    dope_fiber::fiber!('_ => async move {
                        loop {
                            if let Some(reset) = peer.next(|frame| {
                                if let Payload::RstStream(reset) = frame {
                                    Some((reset.stream_id(), reset.error()))
                                } else {
                                    None
                                }
                            }).await {
                                break reset;
                            }
                        }
                    }),
                )).expect("rejection flushes reset without another gRPC operation");
                assert_eq!(reset.0.into_u32(), if unary { 1 } else { 3 });
                assert_eq!(reset.1, if unary { ErrorCode::Cancel } else { ErrorCode::RefusedStream });
                drop(retained);
            }, recovery::Policy::new(
                |event| assert!(matches!(event, Reclaimed::Shutdown), "unexpected client retirement: {event:?}"),
                common::recover,
            ), |result| result.expect("gRPC client completion"))
            .expect("gRPC client runtime");
    }
}

#[test]
fn sender_notification_survives_cancel() {
    use std::{cell::Cell, task::Poll};

    use common::grpc::native::{CancelSender, Peer, Sender, ZERO_WINDOW};
    use dope::core::io::socket::Listener;
    use dope_fiber::{
        abi,
        task::{batch, done::Once},
    };
    use dope_test::harness::GUARD;
    use sark_h2::frame::payload::Payload;

    for sender in [Sender::Unary, Sender::Stream, Sender::Borrowed] {
        let opening = !matches!(sender, Sender::Borrowed);
        let listener = Listener::bind(([127, 0, 0, 1], 0).into()).expect("native H2 peer");
        let limits =
            calls::Settings::new(Some(if opening { 2 } else { 3 }), Some(3), Some(8), Some(1))
                .try_build()
                .expect("one output owner and distinct call slots");
        clients::Plan::with_waiters(listener.local_addr(), limits, limits.calls(), 2)
            .expect("two sender registrations")
            .enter_peer(listener, |mut app, client, listener| {
                app.drive_for(GUARD, Once::new(client.wait_active()))
                    .expect("client drive").expect("active connection");
                let mut peer = app.drive_for(GUARD, Once::new(Peer::accept(listener, ZERO_WINDOW)))
                    .expect("zero-window SETTINGS handshake");
                let waiting = Cell::new(0);
                let waiting = &waiting;
                let calls = dope_fiber::fiber!('_ => async move {
                    let root = abi::poll_fn(|context| Poll::Ready(context.as_ref().root_waker())).await;
                            let metadata = Metadata::new();
                            let message = vec![b'x'];
                            let mut held = client.open_request(b"/proof/Held", None, &metadata)
                                .await.expect("retained request");
                            let register = || {
                                let count = waiting.get() + 1;
                                waiting.set(count);
                                if count == 2 {
                                    root.wake();
                                }
                            };
                            let mut pool = batch::Pool::<2>::acquire().await.expect("separate sender tasks");
                            if matches!(sender, Sender::Unary) {
                                held.send(Bytes, &message).await.expect("occupy sole output owner");
                                let mut results = batch::Join::from_fn(&mut pool, |index| {
                                    CancelSender::new(
                                        client.unary(b"/proof/Unary", None, &metadata, Bytes, &message),
                                        index.get() == 0,
                                        register,
                                    )
                                }).await.expect("notified unary senders");
                                assert!(results.next().expect("first sender").is_none());
                                let error = results.next().expect("second sender").expect("surviving notification")
                                    .err().expect("peer canceled the admitted unary");
                                assert!(matches!(error, Error::Protocol(status) if status.code() == Code::Cancelled));
                            } else if matches!(sender, Sender::Stream) {
                                held.send(Bytes, &message).await.expect("occupy sole output owner");
                                let mut results = batch::Join::from_fn(&mut pool, |index| {
                                    CancelSender::new(
                                        client.stream(b"/proof/Stream", None, &metadata, Bytes, &message),
                                        index.get() == 0,
                                        register,
                                    )
                                }).await.expect("notified stream senders");
                                assert!(results.next().expect("first sender").is_none());
                                let mut response = results.next().expect("second sender").expect("surviving notification")
                                    .expect("second stream admitted after waiter cancellation");
                                let error = response.recv(Bytes).await.err()
                                    .expect("peer canceled the admitted stream");
                                assert!(matches!(error, Error::Protocol(status) if status.code() == Code::Cancelled));
                            } else {
                                let mut first = client.open_request(b"/proof/First", None, &metadata)
                                    .await.expect("first retained stream");
                                let mut second = client.open_request(b"/proof/Second", None, &metadata)
                                    .await.expect("second retained stream");
                                held.send(Bytes, &message).await.expect("occupy sole output owner");
                                let mut results = batch::Join::from_map(&mut pool, [(true, &mut first), (false, &mut second)], |(cancel, stream)| {
                                    CancelSender::new(stream.send(Bytes, &message), cancel, register)
                                }).await.expect("notified borrowed senders");
                                assert!(results.next().expect("first sender").is_none());
                                results.next().expect("second sender").expect("surviving notification")
                                    .expect("second send admitted while both streams remain owned");
                            }
                            assert_eq!(waiting.get(), 2);
                            drop(held);
                    });
                    let release = dope_fiber::fiber!('_ => async move {
                        let mut opened = 0;
                        while opened != if opening { 1 } else { 3 } {
                            opened += usize::from(peer.next(|frame| {
                                assert!(!matches!(frame, Payload::Data(_)), "zero window retains the output owner");
                                matches!(frame, Payload::Headers(_))
                            }).await);
                        }
                        // Wake only the race root: Join still requires each sender's own readiness.
                        abi::poll_fn(|_| if waiting.get() == 2 { Poll::Ready(()) } else { Poll::Pending }).await;
                        peer.write(&[0, 0, 4, 8, 0, 0, 0, 0, 1, 0, 0, 0, 6]).await;
                        if opening {
                            loop {
                                if let Some(id) = peer.next(|frame| match frame {
                                    Payload::Headers(head) => Some(head.stream_id()),
                                    _ => None,
                                }).await {
                                    assert_eq!(id.into_u32(), 3, "canceled waiter never opened a call");
                                    peer.write(&[0, 0, 4, 3, 0, 0, 0, 0, 3, 0, 0, 0, 8]).await;
                                    break;
                                }
                            }
                        }
                        abi::Pending::<()>::new().await
                    });
                let result = app.drive_for(GUARD, Once::new(abi::race::Race::new(calls, release)));
                assert!(matches!(result.expect("canceled notification must wake the surviving sender"), abi::race::Either::Left(())));
            }, recovery::Policy::new(
                |event| assert!(matches!(event, Reclaimed::Shutdown), "unexpected client retirement: {event:?}"),
                common::recover,
            ), |result| result.expect("gRPC client completion"))
            .expect("gRPC client runtime");
    }
}

#[test]
fn queued_cancellations_release_before_refill() {
    use common::grpc::native::{CANCELLATION_RESPONSES, DEFAULT_SETTINGS, Peer};
    use dope::core::io::socket::Listener;
    use dope_fiber::task::done::Once;
    use dope_test::harness::GUARD;
    use sark_grpc::client::config::{Input, Limits};
    use sark_h2::frame::{Ack, payload::Payload};

    let listener = Listener::bind(([127, 0, 0, 1], 0).into()).expect("native H2 peer");
    let limits = Limits::default()
        .try_calls(3)
        .expect("three calls")
        .try_bytes(2)
        .expect("two-byte messages")
        .try_input(Input::try_new(3, 8, 2, 16, 2).expect("two retained response bytes"))
        .expect("bounded input");
    clients::Plan::new(listener.local_addr(), limits, 3).expect("client plan")
        .enter_peer(listener, |mut app, client, listener| {
            app.drive_for(GUARD, Once::new(dope_fiber::fiber!('_ => async move {
                client.wait_active().await.expect("active connection");
                let mut peer = Peer::accept(listener, DEFAULT_SETTINGS).await;
                let metadata = Metadata::new();
                let request = Vec::new();
                let queued = client.stream(b"/proof/Queued", None, &metadata, Bytes, &request)
                    .await.expect("streaming response claim");
                let mut codec = Bytes;
                let held = client.poll_unary(b"/proof/Held", None, &metadata, &mut codec, &request)
                    .await.expect("unfinished unary claim");
                let mut waiting = client.poll_unary(b"/proof/Waiting", None, &metadata, &mut codec, &request)
                    .await.expect("pending unary claim");
                loop {
                    if peer.next(|frame| matches!(frame,
                        Payload::Headers(head) if head.stream_id().into_u32() == 5,
                    )).await {
                        break;
                    }
                }
                peer.write(CANCELLATION_RESPONSES).await;
                loop {
                    if peer.next(|frame| matches!(frame,
                        Payload::Ping(ping) if ping.ack == Ack::Complete,
                    )).await {
                        break;
                    }
                }
                assert!(fibers::TEST.poll_once(waiting.recv()).await.is_none(), "queued input can be released");
                drop(queued);
                drop(held);
                let response = waiting.recv().await.expect("queued cancellations release both bytes before refill")
                    .into_payload().expect("successful response");
                assert_eq!(response.contiguous(), Some(b"ok".as_slice()));
            }))).expect("cancellation batch completes");
        }, recovery::Policy::new(
            |event| assert!(matches!(event, Reclaimed::Shutdown), "unexpected client retirement: {event:?}"),
            common::recover,
        ), |result| result.expect("gRPC client completion"))
        .expect("gRPC client runtime");
}

#[test]
fn split_cancellation_preserves_reused_claim() {
    use common::grpc::native::{
        Peer, SPLIT_RETAINED_RESPONSE, SPLIT_REUSED_RESPONSE, UNIMPLEMENTED_RESPONSE, ZERO_WINDOW,
    };
    use dope::core::io::socket::Listener;
    use dope_fiber::task::done::Once;
    use dope_test::harness::GUARD;
    use sark_grpc::client::config::{Input, Limits, Output};
    use sark_h2::frame::{numbers::ErrorCode, payload::Payload};

    for response_first in [false, true] {
        let listener = Listener::bind(([127, 0, 0, 1], 0).into()).expect("native H2 peer");
        let limits = Limits::try_new(
            2,
            1,
            Input::try_new(1, 4, 1, 8, 1).expect("two streaming response claims"),
            Output::try_new(1).expect("one request owner"),
        )
        .expect("bounded client");
        clients::Plan::new(listener.local_addr(), limits, 2).expect("client plan")
            .enter_peer(listener, |mut app, client, listener| {
                app.drive_for(GUARD, Once::new(dope_fiber::fiber!('_ => async move {
                    client.wait_active().await.expect("active connection");
                    let mut peer = Peer::accept(listener, ZERO_WINDOW).await;
                    let metadata = Metadata::new();
                    let retained = client.open_bidi(b"/proof/Retained", None, &metadata)
                        .await.expect("first response claim").finish().await.expect("closed request half");
                    let old = client.open_bidi(b"/proof/Old", None, &metadata)
                        .await.expect("second response claim");
                    let (mut send, mut response) = old.split().expect("split authority");
                    let message = calls::SMALL_RESPONSE.to_vec();
                    send.send(Bytes, &message).await.expect("zero window retains sole request owner");
                    while !peer.next(|frame| matches!(frame,
                        Payload::Headers(head) if head.stream_id().into_u32() == 3,
                    )).await {}
                    peer.write(SPLIT_RETAINED_RESPONSE).await;
                    peer.write(UNIMPLEMENTED_RESPONSE).await;
                    loop {
                        let event = response.recv(Bytes).await.expect("old terminal response");
                        if let Decoded::Trailers { status } = &*event {
                            assert_eq!(status.code(), Code::Unimplemented);
                            break;
                        }
                    }
                    let mut live = client.open_bidi(b"/proof/Reused", None, &metadata)
                        .await.expect("terminal consumption frees the only response claim");
                    assert!(fibers::TEST.poll_once(live.send(Bytes, &message)).await.is_none());
                    if response_first {
                        drop(response);
                        drop(send);
                    } else {
                        drop(send);
                        drop(response);
                    }
                    live.send(Bytes, &message).await.expect("old split cancellation releases request owner");
                    while !peer.next(|frame| {
                        if let Payload::RstStream(frame) = frame {
                            assert_eq!(frame.stream_id().into_u32(), 3);
                            assert_eq!(frame.error(), ErrorCode::Cancel);
                            true
                        } else {
                            false
                        }
                    }).await {}
                    peer.write(SPLIT_REUSED_RESPONSE).await;
                    loop {
                        let event = live.recv(Bytes).await.expect("reused response claim stays alive");
                        if let Decoded::Trailers { status } = &*event {
                            assert_eq!(status.code(), Code::Ok);
                            break;
                        }
                    }
                    drop(retained);
                }))).expect("both split drop orders preserve the reused call");
            }, recovery::Policy::new(
                |event| assert!(matches!(event, Reclaimed::Shutdown), "unexpected client retirement: {event:?}"),
                common::recover,
            ), |result| result.expect("gRPC client completion"))
            .expect("gRPC client runtime");
    }
}
