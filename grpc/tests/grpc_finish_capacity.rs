pub mod common;

use std::{
    cell::Cell,
    pin::Pin,
    sync::mpsc,
    task::Poll,
    time::{Duration, Instant},
};

use common::{
    codec::Bytes,
    grpc::{call::calls, clients},
};
use dope::core::{driver::life::recovery, io::Reclaimed};
use dope_fiber::{abi, context, task::batch};
use dope_test::{
    fibers,
    harness::{GUARD, peer::TcpScript},
};
use sark_core::manifold::client::domain;
use sark_grpc::{
    client::manifold::{
        Error,
        api::{Duplex as _, Request as _, Session as _},
    },
    details::Diagnostic,
    metadata::Metadata,
};
use sark_h2::{
    ServerRole,
    conn::{self, config, event::Event, machine::Connection},
    frame::numbers::ErrorCode,
};

#[test]
fn finish_wakes_sender() {
    let (progress, observed) = mpsc::sync_channel(1);
    let (release, proceed) = mpsc::sync_channel(1);
    TcpScript::with(
        move |stream| {
            let mut server = Connection::<ServerRole>::with_config(config::Config {
                local_settings: config::Settings {
                    initial_window_size: 0,
                    ..config::Settings::DEFAULT
                },
                ..config::Config::default()
            }).expect("zero request window");
            let mut first = None;
            let mut reset = false;
            loop {
                while !server.output().is_empty() {
                    let bytes = server.output().first();
                    stream.write_all(bytes).expect("server frames");
                    let sent = bytes.len();
                    server.transfer().drain(sent);
                }
                if reset {
                    progress.send(()).expect("reset observer");
                    proceed.recv_timeout(GUARD).expect("client retired");
                    return;
                }
                let mut bytes = [0; 4096];
                let count = stream.read(&mut bytes).expect("client frames");
                assert_ne!(count, 0, "client remains connected");
                server.ingress().ingest(&bytes[..count]).expect("valid client frames");
                while let Some(event) = server.ingress().poll_event() {
                    match event {
                        Event::SettingsAck => progress.send(()).expect("settings observer"),
                        Event::Headers { stream_id: id, .. } => match first {
                            None => first = Some(id),
                            Some(first) => {
                                let mut response = calls::Response::new(&mut server, id.id());
                                response.try_header().expect("occupy response event slot");
                                response.try_message(b"held").expect("block later response events");
                                server.streams().try_cancel(first, ErrorCode::Cancel)
                                    .expect("reset blocked request");
                                reset = true;
                            }
                        },
                        _ => {}
                    }
                }
            }
        },
        |peer| {
            let limits = calls::Settings::new(Some(2), Some(2), Some(1), Some(1))
                .try_build().expect("one output owner and response event");
            let (result, finished, quiet) = clients::Plan::new(peer.addr(), limits, 2).expect("client plan")
                .enter(|mut scope| {
                    let client = scope.handle;
                    fibers::TEST.try_run(&mut scope.app, client.wait_active())
                        .expect("client drive").expect("active connection");
                    let started = Instant::now();
                    while let Err(error) = observed.try_recv() {
                        assert_eq!(error, mpsc::TryRecvError::Empty, "settings observer");
                        assert!(started.elapsed() < GUARD, "settings deadline");
                        fibers::TEST.try_pause(&mut scope.app, Duration::from_millis(10))
                            .expect("drive settings");
                    }
                    let metadata = Metadata::new();
                    let mut request = fibers::TEST.try_run(
                        &mut scope.app, client.open_request(b"/proof/Reset", None, &metadata),
                    ).expect("request drive").expect("request admission");
                    fibers::TEST.try_run(&mut scope.app, request.send(Bytes, &b"queued".to_vec()))
                        .expect("send drive").expect("occupy sole request owner");
                    let mut bidi = fibers::TEST.try_run(
                        &mut scope.app, client.open_bidi(b"/proof/Blocked", None, &metadata),
                    ).expect("bidi drive").expect("bidi admission");
                    let started = Instant::now();
                    while let Err(error) = observed.try_recv() {
                        assert_eq!(error, mpsc::TryRecvError::Empty, "reset observer");
                        assert!(started.elapsed() < GUARD, "reset deadline");
                        fibers::TEST.try_pause(&mut scope.app, Duration::from_millis(10))
                            .expect("drive response and reset");
                    }
                    fibers::TEST.try_pause(&mut scope.app, Duration::from_millis(100))
                        .expect("settle previous I/O");
                    let finished = Cell::new(false);
                    let seen = &finished;
                    let send = &mut bidi;
                    let result = fibers::Harness::new(Duration::from_secs(1)).try_run(
                        &mut scope.app,
                        dope_fiber::fiber!('_ => async move {
                            let mut pool = batch::Pool::<2>::acquire().await.expect("two task slots");
                            let tasks = [(None, Some(send)), (Some(request), None)];
                            batch::Join::from_map(&mut pool, tasks, |(request, bidi)| {
                                dope_fiber::fiber!('_ => async move {
                                    if let Some(bidi) = bidi {
                                        bidi.send(Bytes, &b"waiting".to_vec()).await
                                            .expect("released request owner wakes sender");
                                        assert!(seen.get(), "sender waits until finish releases the owner");
                                    }
                                    if let Some(request) = request {
                                        let error = request.finish(Bytes).await.err()
                                            .expect("peer reset retired native stream");
                                        assert!(matches!(error, Error::Protocol(status) if matches!(
                                            Diagnostic::from(&status),
                                            Diagnostic::Drive(domain::Failure::Settle {
                                                terminal: conn::Error::InactiveStream(primary),
                                                error,
                                            }) if primary == error
                                        )));
                                        seen.set(true);
                                    }
                                })
                            }).await.expect("independently scheduled send and finish");
                        }),
                    );
                    let mut left = 16;
                    let quiet = fibers::TEST.try_run(&mut scope.app, abi::poll_fn(
                        |mut ctx: Pin<&mut context::Context<'_, '_>>| {
                            let ready = ctx.as_mut().driver().driver_ref().ready().has_ready();
                            if !ready || left == 0 {
                                return Poll::Ready(!ready);
                            }
                            left -= 1;
                            ctx.wake();
                            Poll::Pending
                        },
                    )).expect("observe scheduler after silent peer and blocked event slot");
                    (result, finished.get(), quiet)
                }, recovery::Policy::new(
                    |event| assert!(matches!(event, Reclaimed::Shutdown), "client retirement: {event:?}"),
                    common::recover,
                ), |result| result.expect("client completion"))
                .expect("client runtime");
            release.send(()).expect("release peer after client retirement");
            peer.finish().expect("scripted H2 peer");
            assert!(finished, "finish failed after releasing its queued owner");
            assert!(quiet, "blocked input must stop reactivating without new input or output");
            result.expect("failed finish must wake a sender waiting for its released owner");
        },
    ).expect("scripted H2 server");
}
