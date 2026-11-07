pub mod common;

use std::{cell::Cell, pin::Pin, task::Poll, time::Duration};

use common::{
    codec::Bytes,
    grpc::{
        call::calls,
        clients,
        native::{Peer, UNIMPLEMENTED_RESPONSE, ZERO_WINDOW},
    },
};
use dope::core::{
    driver::life::recovery,
    io::{Reclaimed, socket::Listener},
};
use dope_fiber::{
    abi, context,
    task::{batch, done::Once},
};
use dope_test::harness::GUARD;
use sark_core::manifold::client::domain;
use sark_grpc::{
    client::manifold::{
        Error,
        api::{Request as _, Session as _},
    },
    details::Diagnostic,
    metadata::Metadata,
};
use sark_h2::{
    conn,
    frame::{numbers::ErrorCode, payload::Payload},
};

#[test]
fn finish_wakes_sender() {
    let listener = Listener::bind(([127, 0, 0, 1], 0).into()).expect("native H2 peer");
    let limits = calls::Settings::new(Some(2), Some(1), Some(1), Some(1))
        .try_build()
        .expect("one output owner and unary completion");
    let (result, finished, quiet) = clients::Plan::new(listener.local_addr(), limits, 2)
        .expect("client plan")
        .enter_peer(
            listener,
            |mut app, client, listener| {
                app.drive_for(GUARD, Once::new(client.wait_active()))
                    .expect("client drive")
                    .expect("active connection");
                let mut peer = app
                    .drive_for(GUARD, Once::new(Peer::accept(listener, ZERO_WINDOW)))
                    .expect("zero-window SETTINGS handshake");
                let metadata = Metadata::new();
                let mut request = app
                    .drive_for(
                        GUARD,
                        Once::new(client.open_request(b"/proof/Reset", None, &metadata)),
                    )
                    .expect("request drive")
                    .expect("request admission");
                app.drive_for(GUARD, Once::new(request.send(Bytes, &b"queued".to_vec())))
                    .expect("send drive")
                    .expect("occupy sole request owner");
                let mut second = app
                    .drive_for(
                        GUARD,
                        Once::new(client.open_request(b"/proof/Blocked", None, &metadata)),
                    )
                    .expect("second request drive")
                    .expect("second request admission");
                for expected in [1, 3] {
                    let observe = &mut peer;
                    let id = app
                        .drive_for(
                            GUARD,
                            Once::new(dope_fiber::fiber!('_ => async move {
                                loop {
                                    if let Some(id) = observe.next(|frame| match frame {
                                        Payload::Headers(head) => Some(head.stream_id()),
                                        _ => None,
                                    }).await {
                                        break id;
                                    }
                                }
                            })),
                        )
                        .expect("request headers");
                    assert_eq!(id.into_u32(), expected);
                }
                app.drive_for(GUARD, Once::new(peer.write(UNIMPLEMENTED_RESPONSE)))
                    .expect("terminal response fills unary completion slot");
                app.drive_for(GUARD, Once::new(peer.write(&[0, 0, 0, 0, 0, 0, 0, 0, 1])))
                    .expect("empty DATA before response HEADERS");
                let observe = &mut peer;
                let reset = app.drive_for(GUARD, Once::new(dope_fiber::fiber!('_ => async move {
                loop {
                    if let Some(reset) = observe.next(|frame| match frame {
                        Payload::RstStream(reset) => Some((reset.stream_id(), reset.error())),
                        _ => None,
                    }).await {
                        break reset;
                    }
                }
            }))).expect("native rejection retires stream before finish");
                assert_eq!(reset.0.into_u32(), 1);
                assert_eq!(reset.1, ErrorCode::ProtocolError);
                let mut left = 16;
                let quiet = app
                    .drive_for(
                        GUARD,
                        Once::new(abi::poll_fn(
                            |mut ctx: Pin<&mut context::Context<'_, '_>>| {
                                let ready = ctx.as_mut().driver().driver_ref().ready().has_ready();
                                if !ready || left == 0 {
                                    return Poll::Ready(!ready);
                                }
                                left -= 1;
                                ctx.wake();
                                Poll::Pending
                            },
                        )),
                    )
                    .expect("observe scheduler with blocked reset and silent peer");
                let finished = Cell::new(false);
                let seen = &finished;
                let send = &mut second;
                let result = app.drive_for(
                Duration::from_secs(1),
                Once::new(dope_fiber::fiber!('_ => async move {
                    let mut pool = batch::Pool::<2>::acquire().await.expect("two task slots");
                    let tasks = [(None, Some(send)), (Some(request), None)];
                    batch::Join::from_map(&mut pool, tasks, |(request, send)| {
                        dope_fiber::fiber!('_ => async move {
                            if let Some(send) = send {
                                send.send(Bytes, &b"waiting".to_vec()).await
                                    .expect("released request owner wakes sender");
                                assert!(seen.get(), "sender waits until finish releases the owner");
                            }
                            if let Some(request) = request {
                                let error = request.finish(Bytes).await.err()
                                    .expect("native reset retired the stream");
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
                })),
            );
                (result, finished.get(), quiet)
            },
            recovery::Policy::new(
                |event| {
                    assert!(
                        matches!(event, Reclaimed::Shutdown),
                        "client retirement: {event:?}"
                    )
                },
                common::recover,
            ),
        )
        .expect("client runtime");
    assert!(finished, "finish failed after releasing its queued owner");
    assert!(
        quiet,
        "blocked reset must stop reactivating without new input or output"
    );
    result.expect("failed finish must wake a sender waiting for its released owner");
}
