use std::{iter, mem, net::Shutdown};

use dope::core::driver::life::recovery;
pub mod common;

use std::{
    net::TcpListener,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use common::client::recover;
use common::client::{
    pool::{SATURATION_INFLIGHT, SATURATION_MAX_CONNECTIONS, SATURATION_WAVES, *},
    runtime::{
        peer::{Conn, Error},
        pools,
    },
};
use dope::core::{io::Reclaimed, platform::wake};
use dope_fiber::BlockOn as _;
use dope_test::{fibers, harness::GUARD};
use sark_fiber::client::{Ready, config::Config};

#[test]
fn saturates_each_lane() {
    for lanes in 1..=SATURATION_MAX_CONNECTIONS {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind saturation server");
        let addr = listener.local_addr().expect("saturation server address");
        let (accepted, notify) = wake::Ends::blocking().expect("acceptance pipe").split();
        let (full_tx, full_rx) = mpsc::channel();
        let (probe_tx, probe_rx) = mpsc::channel();
        let (free_tx, free_rx) = mpsc::channel();
        let server = thread::spawn(move || -> Result<Vec<usize>, Error> {
            let mut streams = Vec::with_capacity(lanes);
            let mut controls = Vec::with_capacity(lanes);
            for _ in 0..lanes {
                let (stream, _) = listener.accept()?;
                stream.set_nodelay(true)?;
                stream.set_read_timeout(Some(Duration::from_secs(10)))?;
                stream.set_write_timeout(Some(Duration::from_secs(10)))?;
                controls.push(stream.try_clone()?);
                streams.push(stream);
            }
            notify.notify().expect("all saturation lanes accepted");
            let (event_tx, event_rx) = mpsc::channel();
            let mut commands = Vec::with_capacity(lanes);
            let mut workers = Vec::with_capacity(lanes);
            for (id, stream) in streams.into_iter().enumerate() {
                let (tx, rx) = mpsc::channel();
                let events = event_tx.clone();
                commands.push(tx);
                workers.push(thread::spawn(move || {
                    let result = (|| -> Result<usize, Error> {
                        let mut conn = Conn::new(&stream);
                        let mut count = 0;
                        for _ in 0..SATURATION_WAVES {
                            conn.try_fill()?;
                            count += SATURATION_INFLIGHT;
                            events.send(Event::Full(id)).expect("full lane event");
                            assert_eq!(
                                rx.recv_timeout(Duration::from_secs(10))
                                    .expect("probe command"),
                                Command::Probe
                            );
                            let visible = conn.try_probe()?;
                            events
                                .send(Event::Probed { lane: id, visible })
                                .expect("probed lane event");
                            assert!(
                                !visible,
                                "connection exceeded its per-connection inflight cap"
                            );
                            assert_eq!(
                                rx.recv_timeout(Duration::from_secs(10))
                                    .expect("release command"),
                                Command::Release
                            );
                            conn.try_release()?;
                            events
                                .send(Event::Released(id))
                                .expect("released lane event");
                        }
                        if conn.try_tail()? {
                            count += 1;
                            events.send(Event::Tail(id)).expect("tail lane event");
                        }
                        Ok(count)
                    })();
                    if result.is_err() {
                        events.send(Event::Failed(id)).expect("failed lane event");
                    }
                    result
                }));
            }
            drop(event_tx);
            let outcome = (|| -> Result<usize, usize> {
                let events = || {
                    iter::repeat_with(|| {
                        event_rx
                            .recv_timeout(Duration::from_secs(10))
                            .expect("saturation phase event")
                    })
                };
                let check = |phase| -> Result<(), usize> {
                    let mut seen = vec![false; lanes];
                    for event in events().take(lanes) {
                        let (lane, actual, visible) = match event {
                            Event::Full(lane) => Some((lane, Phase::Full, false)),
                            Event::Probed { lane, visible } => Some((lane, Phase::Probed, visible)),
                            Event::Released(lane) => Some((lane, Phase::Released, false)),
                            Event::Tail(_) => None,
                            Event::Failed(lane) => return Err(lane),
                        }
                        .expect("saturation phase before tail");
                        assert_eq!(actual, phase);
                        assert!(
                            !visible,
                            "connection exceeded its per-connection inflight cap"
                        );
                        let slot = seen.get_mut(lane).expect("saturation lane");
                        assert!(!mem::replace(slot, true), "duplicate lane event");
                    }
                    Ok(())
                };
                for _ in 0..SATURATION_WAVES {
                    check(Phase::Full)?;
                    full_tx.send(()).expect("client awaits full lanes");
                    probe_rx
                        .recv_timeout(Duration::from_secs(10))
                        .expect("overflow probe");
                    for lane in &commands {
                        lane.send(Command::Probe).expect("probe lane");
                    }
                    check(Phase::Probed)?;
                    for lane in &commands {
                        lane.send(Command::Release).expect("release lane");
                    }
                    check(Phase::Released)?;
                    free_tx.send(()).expect("client awaits released lanes");
                }
                let event = event_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("released lane admits tail");
                assert!(matches!(event, Event::Tail(_) | Event::Failed(_)));
                match event {
                    Event::Tail(lane) => Ok(lane),
                    Event::Failed(lane) => Err(lane),
                    _ => Err(lanes),
                }
            })();
            drop(commands);
            let mut failure = None;
            for (id, stream) in controls.into_iter().enumerate() {
                if !matches!(outcome.as_ref(), Ok(owner) if *owner == id)
                    && let Err(error) = stream.shutdown(Shutdown::Both)
                    && failure.is_none()
                {
                    failure = Some(Error::Io(error));
                }
            }
            let mut requests = Vec::with_capacity(lanes);
            for worker in workers {
                match worker.join().expect("saturation worker thread") {
                    Ok(count) => requests.push(count),
                    Err(error) if failure.is_none() => failure = Some(error),
                    Err(_) => {}
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
            let owner = outcome.expect("failed lane retains its I/O cause");
            assert_eq!(
                requests.get(owner).copied(),
                Some(SATURATION_WAVES * SATURATION_INFLIGHT + 1),
                "tail request completes on its released lane"
            );
            Ok(requests)
        });

        let expected = SATURATION_WAVES * lanes * SATURATION_INFLIGHT + 1;
        let requests = SATURATION_WAVES * lanes * SATURATION_INFLIGHT + 1;
        let client = pools::Plan::new(
            addr,
            Config::try_new("127.0.0.1")
                .expect("HTTP host allocation")
                .timeout(Duration::from_secs(10))
                .request_pool(requests as u32, 4096)
                .queue_bytes(64)
                .inflight(SATURATION_INFLIGHT),
            lanes,
            requests,
        )
        .expect("fixed saturation client topology must fit configured capacities")
        .enter(
            |mut app| {
                let client = app.handle;
                app.wait().expect("saturation client readiness");
                let started = Instant::now();
                while client.connections() != lanes {
                    assert!(
                        started.elapsed() < GUARD,
                        "all client lanes must become active"
                    );
                    app.app
                        .block_on(fibers::Yield::new())
                        .expect("lane readiness driver");
                }
                accepted
                    .wait_timeout(GUARD.saturating_sub(started.elapsed()))
                    .expect("all peer lanes accepted");
                assert_eq!(client.connections(), lanes, "fixed client lane count");

                let mut done = 0;
                let batch = lanes * SATURATION_INFLIGHT;
                for _ in 0..SATURATION_WAVES {
                    let mut streams = Vec::with_capacity(batch);
                    for _ in 0..batch {
                        let stream = app
                            .app
                            .block_on(fibers::TEST.poll_once(client.stream_get(SATURATION_PATH)))
                            .expect("saturation admission driver")
                            .expect("available response lane must admit immediately")
                            .expect("saturation response stream");
                        streams.push(stream);
                    }
                    app.app
                        .block_on(fibers::Yield::new())
                        .expect("saturation client yield");
                    full_rx
                        .recv_timeout(Duration::from_secs(10))
                        .expect("peer must observe every saturated lane");

                    let overflow = app
                        .app
                        .block_on(fibers::TEST.poll_once(client.stream_get(SATURATION_PATH)))
                        .expect("overflow admission driver");
                    assert!(
                        overflow.is_none(),
                        "saturated response lane must keep overflow pending"
                    );
                    app.app
                        .block_on(fibers::Yield::new())
                        .expect("overflow probe yield");
                    probe_tx
                        .send(())
                        .expect("saturation peer must receive its probe");
                    free_rx
                        .recv_timeout(Duration::from_secs(10))
                        .expect("saturation peer must release its lanes");

                    for mut stream in streams {
                        assert_eq!(
                            app.drain(&mut stream).expect("saturation response"),
                            (0, Some(200), 2)
                        );
                        done += 1;
                    }
                }

                let mut tail = app
                    .app
                    .block_on(fibers::TEST.poll_once(client.stream_get(SATURATION_PATH)))
                    .expect("tail admission driver")
                    .expect("released response lane must admit immediately")
                    .expect("tail response stream");
                assert_eq!(
                    app.drain(&mut tail).expect("saturation tail response"),
                    (0, Some(200), 2)
                );
                done + 1
            },
            recovery::Policy::new(
                |event| std::assert_matches!(event, Reclaimed::Shutdown),
                recover,
            ),
            |result| result.expect("client execution"),
        )
        .expect("fixed saturation client topology must install, drive, and shut down");

        let server = server.join().expect("saturation server thread");
        assert_eq!(client, expected);
        let requests = server.expect("saturation server");
        assert_eq!(requests.len(), lanes);
        assert_eq!(requests.iter().sum::<usize>(), expected);
        assert_eq!(
            requests
                .iter()
                .filter(|&&count| count == SATURATION_WAVES * SATURATION_INFLIGHT + 1)
                .count(),
            1,
        );
        assert!(
            requests
                .iter()
                .all(|&count| count >= SATURATION_WAVES * SATURATION_INFLIGHT)
        );
    }
}
