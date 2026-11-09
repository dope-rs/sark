use std::{
    os::fd::AsFd as _,
    time::{Duration, Instant},
};

use common::client::{
    pool::{
        SATURATION_INFLIGHT, SATURATION_LAST_RESPONSE, SATURATION_MAX_CONNECTIONS, SATURATION_PATH,
        SATURATION_WAVES,
    },
    recover,
    runtime::{
        peer::{self, Conn, Error},
        pools,
    },
};
use dope::core::{
    Backend, Errno,
    dope_sys::io::poll::{self, Poll as _},
    driver::life::recovery,
    io::Reclaimed,
};
use dope_fiber::BlockOn as _;
use dope_test::{
    fibers,
    harness::{
        GUARD,
        peer::{AcceptScript, Error as PeerError},
    },
};
use sark_fiber::client::{Ready, config::Config};

pub mod common;

#[test]
fn saturates_each_lane() {
    for lanes in 1..=SATURATION_MAX_CONNECTIONS {
        AcceptScript::with(
            move |acceptor| -> Result<(), Error> {
                let mut streams = Vec::with_capacity(lanes);
                for _ in 0..lanes {
                    streams.push(acceptor.accept()?);
                }
                let mut conns = streams.iter().map(Conn::new).collect::<Vec<_>>();
                for _ in 0..SATURATION_WAVES {
                    for conn in &mut conns {
                        conn.try_fill()?;
                    }
                    for conn in &mut conns {
                        assert!(
                            !conn.try_probe()?,
                            "connection exceeded its per-connection inflight cap"
                        );
                    }
                    for conn in &mut conns {
                        conn.try_release()?;
                    }
                }

                let mut entries =
                    [poll::Entry::readable(streams[0].as_fd()); SATURATION_MAX_CONNECTIONS];
                for (entry, stream) in entries.iter_mut().zip(&streams) {
                    *entry = poll::Entry::readable(stream.as_fd());
                }
                for entry in &mut entries[lanes..] {
                    entry.retire();
                }
                let started = Instant::now();
                let owner = loop {
                    let left = GUARD.saturating_sub(started.elapsed());
                    if left.is_zero() {
                        return Err(PeerError::TimedOut.into());
                    }
                    let millis = (left.as_millis()
                        + u128::from(!left.subsec_nanos().is_multiple_of(1_000_000)))
                    .min(i32::MAX as u128) as i32;
                    match Backend::wait::<_, Errno>(&mut entries, millis) {
                        Ok(events) => {
                            if events.count() == 0 {
                                return Err(PeerError::TimedOut.into());
                            }
                            assert_eq!(events.count(), 1, "one released lane admits the tail");
                            break events
                                .entries()
                                .iter()
                                .position(|entry| entry.is_ready())
                                .expect("ready tail lane");
                        }
                        Err(error) if error.is_intr() => {}
                        Err(error) => return Err(PeerError::Syscall(error).into()),
                    }
                };
                let conn = &mut conns[owner];
                conn.try_request()?
                    .ok_or(Error::UnexpectedEof)?
                    .try_line()?;
                conn.write_all(SATURATION_LAST_RESPONSE)?;
                for conn in &mut conns {
                    peer::closed(conn)?;
                }
                Ok(())
            },
            |server| {
                let expected = SATURATION_WAVES * lanes * SATURATION_INFLIGHT + 1;
                pools::Plan::new(
                    server.addr(),
                    Config::try_new("127.0.0.1")
                        .expect("HTTP host allocation")
                        .timeout(Duration::from_secs(10))
                        .request_pool(expected as u32, 4096)
                        .queue_bytes(64)
                        .inflight(SATURATION_INFLIGHT),
                    lanes,
                    expected,
                )
                .expect("fixed saturation client topology must fit configured capacities")
                .enter(
                    (),
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

                        let batch = lanes * SATURATION_INFLIGHT;
                        for _ in 0..SATURATION_WAVES {
                            let streams = app
                                .app
                                .block_on(fibers::TEST.poll_once(
                                    dope_fiber::fiber!('_ => async move {
                                        let mut streams = Vec::with_capacity(batch);
                                        for _ in 0..batch {
                                            streams.push(
                                                client.stream_get(SATURATION_PATH)
                                                    .await
                                                    .expect("saturation response stream"),
                                            );
                                        }
                                        let overflow = fibers::TEST
                                            .poll_once(client.stream_get(SATURATION_PATH))
                                            .await;
                                        assert!(
                                            overflow.is_none(),
                                            "saturated response lane must keep overflow pending"
                                        );
                                        // Distinguish capacity waiting from exhausted poll work.
                                        dope_fiber::abi::Ready::new(streams).await
                                    }),
                                ))
                                .expect("saturation admission driver")
                                .expect("batch and overflow must complete before driving I/O");
                            for mut stream in streams {
                                assert_eq!(
                                    app.drain(&mut stream).expect("saturation response"),
                                    (0, Some(200), 2)
                                );
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
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("fixed saturation client topology must install, drive, and shut down");

                server
                    .finish()
                    .expect("saturation peer thread")
                    .expect("saturation server");
            },
        )
        .expect("spawn saturation peer");
    }
}
