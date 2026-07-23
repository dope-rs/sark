#![cfg(target_os = "linux")]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use dope_extra::harness::Harness;
use http::StatusCode;
use sark::{Executor, Throughput, driver};

#[sark_gen::request]
struct EmptyReq {}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: Vec<u8>,
}

#[sark_gen::handler]
async fn sleep_handler(_req: EmptyReq, _state: &(), timer: sark::Timer) -> Reply {
    timer.sleep(Duration::from_millis(100)).await;
    let mut body = Vec::new();
    body.extend_from_slice(b"slept 100ms");
    Reply {
        status: StatusCode::OK,
        body,
    }
}

sark_gen::define_route! {
    SleepDispatch: () => {
        GET "/sleep" => async(capacity = 32) sleep_handler,
    }
}

#[test]
fn handler_awaits_timer() {
    let bind: std::net::SocketAddr = "127.0.0.1:18890".parse().unwrap();
    let server = support::http_server(bind, Duration::from_secs(10));

    Harness::new(bind)
        .run_with_trigger(
            |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?;
                executor.enter(|mut session| {
                    let timer =
                        sark::Timer::with_capacity(support::MAX_CONNECTIONS.saturating_mul(2));
                    server.clone().serve(
                        &mut session,
                        SleepDispatch::new(
                            &(),
                            &timer,
                            sark::app::Config {
                                timer_capacity: support::MAX_CONNECTIONS.saturating_mul(2),
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                let start = Instant::now();
                sock.write_all(b"GET /sleep HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .unwrap();
                let mut resp = String::new();
                sock.read_to_string(&mut resp).unwrap();
                let elapsed = start.elapsed();

                assert!(
                    elapsed >= Duration::from_millis(90),
                    "elapsed: {:?}",
                    elapsed
                );
                assert!(resp.contains("200 OK"), "resp: {}", resp);
                assert!(resp.contains("slept 100ms"), "resp: {}", resp);
            },
        )
        .expect("harness");
}
