use dope::core::driver::life::recovery;
pub mod common;

use std::{
    convert::Infallible,
    net::{SocketAddr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use common::client::recover;
use common::client::{
    Cause, runtime,
    runtime::{
        server::{RETAINED_PATHS, Server},
        tls::Plan,
    },
    tls,
};
use dope::core::{Errno, io::Reclaimed};
use dope_fiber::{
    BlockOn as _,
    reply::{Progress, order::Depth},
};
use dope_test::{fibers, harness::GUARD};
use dope_tls::tls::endpoint;
use o3::{base::cell::region, depot::capacity};
use sark_client::{
    conn::{Error, Kind},
    protocol::response,
};
use sark_fiber::client::{
    Ready as _, config,
    config::report::{Arm, Disarm, Failure, Never, Policy, Retain},
};
use sark_http::method::Method;

#[test]
fn recycles_failures() {
    use sark_client::{Error, Safety, capacity};

    const PATHS: [&str; 3] = ["/retain-first", "/retain-second", "/after-release"];

    let listener = (TcpListener::bind("127.0.0.1:0")).expect("bind raw TLS server");
    let addr = (listener.local_addr()).expect("raw TLS server address");
    let server = thread::spawn(move || {
        Server { listener }.try_truncate(
            tls::Keys::new(&tls::SEED)
                .server()
                .expect("TLS server config"),
            &PATHS,
        )
    });
    let plan = (Plan::new(
        addr,
        config::Config::try_new("test.local")
            .expect("HTTP host allocation")
            .timeout(Duration::from_secs(2)),
        endpoint::Config::from(
            tls::Keys::new(&tls::SEED)
                .client_plan()
                .expect("TLS client setup"),
        ),
        4,
        4,
        1,
    ))
    .expect("TLS client plan");
    let result = plan.enter(
        |mut app| {
            let client = app.handle;
            let second = {
                (app.wait()).as_ref().expect("first TLS connection");
                let first = (app
                    .app
                    .block_on(client.send(Method::POST, PATHS[0], &[]))
                    .expect("drive first truncated request"))
                .err()
                .expect("first truncated request must fail");
                (app.wait()).as_ref().expect("second TLS connection");
                let second = (app
                    .app
                    .block_on(client.send(Method::POST, PATHS[1], &[]))
                    .expect("drive second truncated request"))
                .err()
                .expect("second truncated request must fail");
                (app.wait()).as_ref().expect("third TLS connection");

                let denied = (app
                    .app
                    .block_on(client.send(Method::POST, "/must-not-submit", &[]))
                    .expect("drive request with exhausted report capacity"))
                .err()
                .expect("report-capacity rejection must fail");
                assert!((denied.safety()) == (Safety::BeforeSubmit));
                assert!(matches!(
                    denied.cause(),
                    Error::Capacity(capacity::Error::FailureReport)
                ));
                assert!(
                    matches!(first.cause(), sark_client::Error::Transport(error)
        if first.safety() == Safety::PossiblyCommitted
            && error.kind() == Kind::Wire
            && matches!(error.detail(),
                Some(cause) if matches!(cause.causes(), (dope_tls::Error::Io(error), None)
                    if error.kind() == std::io::ErrorKind::UnexpectedEof))),
                    "unexpected TLS failure"
                );
                second
            };
            let recycled = (app
                .app
                .block_on(client.send(Method::POST, PATHS[2], &[]))
                .expect("drive request after report release"))
            .err()
            .expect("request after report release must fail");
            for failure in [&second, &recycled] {
                assert!(
                    matches!(failure.cause(), sark_client::Error::Transport(error)
        if failure.safety() == Safety::PossiblyCommitted
            && error.kind() == Kind::Wire
            && matches!(error.detail(),
                Some(cause) if matches!(cause.causes(), (dope_tls::Error::Io(error), None)
                    if error.kind() == std::io::ErrorKind::UnexpectedEof))),
                    "unexpected TLS failure"
                );
            }
        },
        recovery::Policy::new(
            |event| std::assert_matches!(event, Reclaimed::Shutdown),
            recover,
        ),
        |result| result.expect("client execution"),
    );
    result.expect("run retained-failure client");
    (server.join())
        .expect("raw TLS server thread")
        .expect("TLS server execution");
}

#[test]
fn skips_blocked_report() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind TLS server");
    let addr = listener.local_addr().expect("TLS server address");
    let server = thread::spawn(move || {
        Server { listener }.try_retained(
            tls::Keys::new(&tls::SEED)
                .server()
                .expect("TLS server config"),
        )
    });
    let plan = Plan::new(
        addr,
        config::Config::try_new("test.local")
            .expect("HTTP host allocation")
            .timeout(Duration::from_secs(2))
            .inflight(3)
            .failures(0),
        endpoint::Config::from(
            tls::Keys::new(&tls::SEED)
                .client_plan()
                .expect("TLS client setup"),
        ),
        4,
        8,
        2,
    )
    .expect("TLS client plan");
    plan.enter(
        |mut app| {
            let client = app.handle;
            let [first, failed, second, third, finish] = RETAINED_PATHS;
            let started = Instant::now();
            while client.connections() != 2 {
                assert!(started.elapsed() < GUARD, "two TLS connections");
                app.app
                    .block_on(fibers::Yield::new())
                    .expect("TLS readiness");
            }
            let retained = app
                .app
                .block_on(client.send(Method::POST, failed, &[]))
                .expect("failure driver")
                .err()
                .expect("retained TLS failure");
            assert!(matches!(retained.cause(), sark_client::Error::Transport(_)));
            let started = Instant::now();
            while client.connections() != 1 {
                assert!(started.elapsed() < GUARD, "failed connection retired");
                app.app
                    .block_on(fibers::Yield::new())
                    .expect("TLS retirement");
            }
            let first = app
                .app
                .block_on(client.stream_get(first))
                .expect("first admission driver")
                .ok()
                .expect("first stream");
            let started = Instant::now();
            while client.connections() != 2 {
                assert!(started.elapsed() < GUARD, "replacement TLS connection");
                app.app
                    .block_on(fibers::Yield::new())
                    .expect("TLS replacement");
            }
            let second = app
                .app
                .block_on(client.stream_get(second))
                .expect("second admission driver")
                .ok()
                .expect("armed lane reused");
            let third = app
                .app
                .block_on(client.stream_get(third))
                .expect("third admission driver")
                .ok()
                .expect("blocked lane skipped");
            for mut stream in [first, second, third] {
                let mut status = None;
                while let Some(event) = app
                    .app
                    .block_on(stream.next_event())
                    .expect("response driver")
                {
                    if let response::Event::Head(head) = event.ok().expect("response on armed lane")
                    {
                        status = Some(head.status().as_u16());
                    }
                }
                assert_eq!(status, Some(200));
            }
            app.app
                .block_on(client.get(finish))
                .expect("final request driver")
                .ok()
                .expect("released report reused");
        },
        recovery::Policy::new(
            |event| std::assert_matches!(event, Reclaimed::Shutdown),
            recover,
        ),
        |result| result.expect("client execution"),
    )
    .expect("TLS client execution");
    server
        .join()
        .expect("TLS server thread")
        .expect("TLS server execution");
}

#[test]
fn rejects_report_overflow() {
    let error = Plan::new(
        SocketAddr::from(([127, 0, 0, 1], 1)),
        config::Config::try_new("test.local")
            .expect("HTTP host allocation")
            .failures(usize::MAX),
        endpoint::Config::from(
            tls::Keys::new(&tls::SEED)
                .client_plan()
                .expect("TLS client setup"),
        ),
        1,
        1,
        1,
    );
    assert!(matches!(
        &error,
        Err(runtime::tls::Build::Config(config::Error::FailureReports {
            live: 1,
            retained: usize::MAX,
        }))
    ));
}

#[test]
fn slot_reuses_and_retains() {
    let text = String::from("borrowed");
    let pool = <Retain as Policy<Cause<'_>>>::try_pool(capacity::Count::new(1));
    let pool = pool.expect("test operation must succeed");
    let source = <Retain as Policy<Cause<'_>>>::source(&pool);
    region::Token::scope(|mut token| {
        let first = <Retain as Policy<Cause<'_>>>::vacant(&token);
        let second = <Retain as Policy<Cause<'_>>>::vacant(&token);
        assert!(matches!(
            <Retain as Policy<Cause<'_>>>::arm(&source, &first, &mut token),
            Arm::Ready
        ));
        assert!(matches!(
            <Retain as Policy<Cause<'_>>>::arm(&source, &first, &mut token),
            Arm::Ready
        ));
        assert!(matches!(
            <Retain as Policy<Cause<'_>>>::arm(&source, &second, &mut token),
            Arm::Exhausted
        ));
        assert!(matches!(
            <Retain as Policy<Cause<'_>>>::disarm(&first, &mut token),
            Disarm::Vacant
        ));
        assert!(matches!(
            <Retain as Policy<Cause<'_>>>::arm(&source, &second, &mut token),
            Arm::Ready
        ));
        <Retain as Policy<Cause<'_>>>::failed(&second, &mut token, Failure::Wire(Cause(&text)));
        let detailed = <Retain as Policy<Cause<'_>>>::arm(&source, &second, &mut token);
        assert!(matches!(&detailed, Arm::Failed(Error::Wire { .. })));
        let Arm::Failed(Error::Wire { detail, receive }) = detailed else {
            return;
        };
        for progress in [
            Progress::Pending(Depth::ZERO),
            Progress::Pending(Depth::ZERO),
            Progress::Complete(Depth::ZERO),
        ] {
            let observed =
                <Retain as Policy<Cause<'_>>>::retire(&second, &mut token, |cause, _| {
                    assert!(matches!(cause, Some(Error::Wire { .. })));
                    if let Some(Error::Wire { detail, receive }) = cause {
                        assert_eq!((**detail).0, text);
                        assert_eq!(*receive, None);
                    }
                    progress
                });
            assert!(observed == progress);
        }
        assert_eq!((*detail).0, text);
        assert_eq!(receive, None);
        assert!(matches!(
            <Retain as Policy<Cause<'_>>>::arm(&source, &first, &mut token),
            Arm::Exhausted
        ));
        drop(detail);
        assert!(matches!(
            <Retain as Policy<Cause<'_>>>::arm(&source, &first, &mut token),
            Arm::Ready
        ));
        <Retain as Policy<Cause<'_>>>::failed(
            &second,
            &mut token,
            Failure::Receive(Errno::BAD_DESCRIPTOR),
        );
        assert!(matches!(
            <Retain as Policy<Cause<'_>>>::arm(&source, &second, &mut token),
            Arm::Failed(Error::Receive { errno }) if errno == Errno::BAD_DESCRIPTOR
        ));
        <Retain as Policy<Cause<'_>>>::retire(&second, &mut token, |cause, _| {
            assert!(matches!(cause, Some(Error::Receive { errno })
                if *errno == Errno::BAD_DESCRIPTOR));
            Progress::Complete(Depth::ZERO)
        });
        assert!(matches!(
            <Retain as Policy<Cause<'_>>>::arm(&source, &second, &mut token),
            Arm::Exhausted
        ));
    });
}

#[test]
fn receive_survives_turns() {
    region::Token::scope(|mut token| {
        let state = <Never as Policy<Infallible>>::vacant(&token);
        for (first, next) in [
            (Errno::BAD_DESCRIPTOR, Errno::CANCELED),
            (Errno::CANCELED, Errno::BAD_DESCRIPTOR),
        ] {
            <Never as Policy<Infallible>>::failed(&state, &mut token, Failure::Receive(first));
            <Never as Policy<Infallible>>::failed(&state, &mut token, Failure::Receive(next));
            for progress in [
                Progress::Pending(Depth::ZERO),
                Progress::Complete(Depth::ZERO),
            ] {
                let observed =
                    <Never as Policy<Infallible>>::retire(&state, &mut token, |cause, _| {
                        assert!(matches!(cause, Some(Error::Receive { errno }) if *errno == first));
                        progress
                    });
                assert!(observed == progress);
            }
            assert!(matches!(
                <Never as Policy<Infallible>>::arm(&Never, &state, &mut token),
                Arm::Ready,
            ));
        }
    });
}
