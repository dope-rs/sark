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
                .enter(|mut scope| {
                    let client = scope.handle;
                    fibers::TEST.try_run(&mut scope.app, client.wait_active())
                        .expect("client drive").expect("active gRPC connection");
                    fibers::TEST.try_run(&mut scope.app, dope_fiber::fiber!('_ => async move {
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
                        |mut scope| {
                            let client = scope.handle;
                            fibers::TEST.try_run(&mut scope.app, dope_fiber::fiber!('_ => async move {
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
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };

    use dope_test::harness::{GUARD, peer::TcpScript};
    use sark_core::manifold::schedule::Insert;
    use sark_grpc::{Reason, client::config, details::Diagnostic};
    use sark_h2::{
        frame::{
            layout::Header,
            numbers::{Flags, Type},
        },
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
        let (progress, observed) = mpsc::sync_channel(1);
        let (release, proceed) = mpsc::sync_channel(1);
        TcpScript::with(
        move |stream| {
            let mut preface = [0; 24];
            stream.read_exact(&mut preface).expect("client preface");
            assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
            let reset = loop {
                let mut bytes = [0; 9];
                let received = stream.read_exact(&mut bytes);
                if matches!(received, Err(dope_test::harness::peer::Error::TimedOut)) {
                    break false;
                }
                received.expect("client frame");
                let header = Header::parse(&bytes).expect("client frame header");
                let mut payload = vec![0; header.length.into_usize()];
                stream.read_exact(&mut payload).expect("client frame payload");
                let settled = header.kind == Type::Settings && header.flags.contains(Flags::ACK);
                let opened = !unary && header.kind == Type::Headers && header.stream_id == Id::FIRST_CLIENT;
                if header.kind == Type::Settings && !settled {
                    stream.write_all(&[
                        0, 0, 0, 4, 0, 0, 0, 0, 0,
                        0, 0, 0, 4, 1, 0, 0, 0, 0,
                    ]).expect("server SETTINGS and client SETTINGS acknowledgment");
                }
                if settled || opened {
                    progress.send(None).expect("client progress observer");
                    if unary || opened {
                        proceed.recv_timeout(GUARD).expect("rejection completed");
                        stream.read_wait(Some(Duration::from_millis(200)))
                            .expect("reset deadline");
                    }
                }
                if header.kind == Type::RstStream {
                    assert_eq!(header.stream_id.into_u32(), if unary { 1 } else { 3 });
                    assert_eq!(payload, [0, 0, 0, if unary { 8 } else { 7 }]);
                    break true;
                }
            };
            progress.send(Some(reset)).expect("reset observer");
            proceed.recv_timeout(GUARD).expect("client runtime retired");
        },
        |peer| {
            let limits = if unary {
                config::Limits::default().try_bytes(0).expect("empty messages allowed")
            } else {
                config::Limits::default().try_calls(1).expect("one gRPC call")
            };
            let reset = clients::Plan::new(peer.addr(), limits, 2).expect("two H2 slots")
                .enter(|mut scope| {
                    let client = scope.handle;
                    fibers::TEST.try_run(&mut scope.app, client.wait_active())
                        .expect("client drive").expect("active connection");
                    let metadata = Metadata::new();
                    let mut retained = None;
                    let started = Instant::now();
                    loop {
                        match observed.try_recv() {
                            Ok(settled) => {
                                assert_eq!(settled, None);
                                if !unary && retained.is_none() {
                                    retained = Some(fibers::TEST.try_run(
                                        &mut scope.app,
                                        client.open_request(b"/proof/Held", None, &metadata),
                                    ).expect("first request drive").expect("retained first request"));
                                    continue;
                                }
                                break;
                            }
                            Err(error) => {
                                assert_eq!(error, mpsc::TryRecvError::Empty, "client progress observer");
                                assert!(started.elapsed() < GUARD, "client progress deadline");
                                fibers::TEST.try_pause(&mut scope.app, Duration::from_millis(10))
                                    .expect("drive SETTINGS and first request");
                            }
                        }
                    }
                    fibers::TEST.try_pause(&mut scope.app, Duration::from_millis(100))
                        .expect("retire prior I/O before rejection");
                    let message = vec![b'x'];
                    let error = match method {
                        Method::Unary => fibers::TEST.try_run(
                            &mut scope.app,
                            client.unary(b"/proof/Reject", None, &metadata, Bytes, &message),
                        ).expect("rejected unary drive").err().expect("message exceeds zero-byte limit"),
                        Method::Request => fibers::TEST.try_run(
                            &mut scope.app,
                            client.open_request(b"/proof/Reject", None, &metadata),
                        ).expect("rejected request drive").err().expect("gRPC slot excluded"),
                        Method::Bidi => fibers::TEST.try_run(
                            &mut scope.app,
                            client.open_bidi(b"/proof/Reject", None, &metadata),
                        ).expect("rejected bidi drive").err().expect("gRPC slot excluded"),
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
                    release.send(()).expect("observe reset after rejection");
                    fibers::TEST.try_pause(&mut scope.app, Duration::from_millis(500))
                        .expect("drive idle client");
                    let reset = observed.recv_timeout(GUARD).expect("reset observation").expect("reset phase");
                    drop(retained);
                    reset
                }, recovery::Policy::new(
                    |event| assert!(matches!(event, Reclaimed::Shutdown), "unexpected client retirement: {event:?}"),
                    common::recover,
                ), |result| result.expect("gRPC client completion"))
                .expect("gRPC client runtime");
            release.send(()).expect("release scripted peer after client retirement");
            peer.finish().expect("scripted H2 peer");
            assert!(reset, "rejected {method:?} must flush its reset without another gRPC operation");
        },
    ).expect("scripted H2 server");
    }
}

#[test]
fn sender_notification_survives_cancel() {
    use std::{
        cell::Cell,
        time::{Duration, Instant},
    };

    use common::grpc::native::{CancelSender, Sender};
    use dope::core::platform::wake;
    use dope_fiber::task::batch;
    use dope_test::harness::{GUARD, peer::TcpScript};
    use sark_h2::frame::{
        layout::Header,
        numbers::{Flags, Type},
    };

    for sender in [Sender::Unary, Sender::Stream, Sender::Borrowed] {
        let opening = !matches!(sender, Sender::Borrowed);
        let (settled, handshake) = wake::Ends::blocking().expect("handshake signal").split();
        let (release, queued) = wake::Ends::blocking()
            .expect("sender admission signal")
            .split();
        let (finished, finish) = wake::Ends::blocking()
            .expect("peer lifetime signal")
            .split();
        TcpScript::with(
            move |stream| {
                let mut preface = [0; 24];
                stream.read_exact(&mut preface).expect("client preface");
                assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
                // Keep every request owner resident until both senders are queued.
                stream.write_all(&[
                    0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0,
                    0, 0, 0, 4, 1, 0, 0, 0, 0,
                ]).expect("zero stream window and SETTINGS acknowledgment");
                let mut opened = 0;
                while opened != if opening { 1 } else { 3 } {
                    let mut bytes = [0; 9];
                    stream.read_exact(&mut bytes).expect("initial frame");
                    let header = Header::parse(&bytes).expect("initial frame header");
                    let mut payload = vec![0; header.length.into_usize()];
                    stream.read_exact(&mut payload).expect("initial frame payload");
                    if header.kind == Type::Settings && header.flags.contains(Flags::ACK) {
                        handshake.notify().expect("zero window installed");
                    }
                    if header.kind == Type::Headers {
                        opened += 1;
                    }
                    assert_ne!(header.kind, Type::Data, "zero window retains the output owner");
                }
                release.wait_timeout(GUARD).expect("both senders registered");
                // Only stream 1 becomes writable; this frame requires no reply.
                stream.write_all(&[0, 0, 4, 8, 0, 0, 0, 0, 1, 0, 0, 0, 6])
                    .expect("release exactly one six-byte request owner");
                if opening {
                    loop {
                        let mut bytes = [0; 9];
                        stream.read_exact(&mut bytes).expect("surviving call frame");
                        let header = Header::parse(&bytes).expect("surviving call header");
                        let mut payload = vec![0; header.length.into_usize()];
                        stream.read_exact(&mut payload).expect("surviving call payload");
                        if header.kind == Type::Headers {
                            assert_eq!(header.stream_id.into_u32(), 3, "canceled waiter never opened a call");
                            stream.write_all(&[0, 0, 4, 3, 0, 0, 0, 0, 3, 0, 0, 0, 8])
                                .expect("complete the admitted survivor with CANCEL");
                            break;
                        }
                    }
                }
                finished.wait_timeout(GUARD).expect("client runtime retired");
            },
            |peer| {
                let limits = calls::Settings::new(Some(if opening { 2 } else { 3 }), Some(3), Some(8), Some(1))
                    .try_build().expect("one output owner and distinct call slots");
                clients::Plan::with_waiters(peer.addr(), limits, limits.calls(), 2).expect("two sender registrations")
                    .enter(|mut scope| {
                        let client = scope.handle;
                        fibers::TEST.try_run(&mut scope.app, client.wait_active())
                            .expect("client drive").expect("active connection");
                        let started = Instant::now();
                        loop {
                            match settled.wait_timeout(Duration::ZERO) {
                                Ok(()) => break,
                                Err(wake::WaitError::Timeout { .. }) => {
                                    assert!(started.elapsed() < GUARD, "zero-window handshake deadline");
                                    fibers::TEST.try_pause(&mut scope.app, Duration::from_millis(10))
                                        .expect("drive zero-window handshake");
                                }
                                Err(error) => panic!("handshake signal: {error:?}"),
                            }
                        }
                        let cancelled = Cell::new(false);
                        let observed = &cancelled;
                        let queued = &queued;
                        let result = fibers::TEST.try_run(&mut scope.app, dope_fiber::fiber!('_ => async move {
                            let metadata = Metadata::new();
                            let message = vec![b'x'];
                            let mut held = client.open_request(b"/proof/Held", None, &metadata)
                                .await.expect("retained request");
                            let waiting = Cell::new(0);
                            let register = || {
                                let count = waiting.get() + 1;
                                waiting.set(count);
                                if count == 2 {
                                    queued.notify().expect("release after both registrations");
                                }
                            };
                            let mut pool = batch::Pool::<2>::acquire().await.expect("separate sender tasks");
                            if matches!(sender, Sender::Unary) {
                                held.send(Bytes, &message).await.expect("occupy sole output owner");
                                let mut results = batch::Join::from_fn(&mut pool, |index| {
                                    CancelSender::new(
                                        client.unary(b"/proof/Unary", None, &metadata, Bytes, &message),
                                        index.get() == 0,
                                        observed,
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
                                        observed,
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
                                    CancelSender::new(stream.send(Bytes, &message), cancel, observed, register)
                                }).await.expect("notified borrowed senders");
                                assert!(results.next().expect("first sender").is_none());
                                results.next().expect("second sender").expect("surviving notification")
                                    .expect("second send admitted while both streams remain owned");
                            }
                            assert_eq!(waiting.get(), 2);
                            drop(held);
                        }));
                        assert!(cancelled.get(), "first sender was notified and canceled before admission");
                        result.expect("canceled notification must wake the surviving sender");
                    }, recovery::Policy::new(
                        |event| assert!(matches!(event, Reclaimed::Shutdown), "unexpected client retirement: {event:?}"),
                        common::recover,
                    ), |result| result.expect("gRPC client completion"))
                    .expect("gRPC client runtime");
                finish.notify().expect("release peer after client retirement");
                peer.finish().expect("scripted H2 peer");
            },
        ).expect("scripted H2 server");
    }
}
