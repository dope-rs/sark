use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use dope_extra::testing::run_with_trigger;
use http::StatusCode;
use sark::{Build, ServerCfg};

#[sark_gen::request]
struct HelloRequest {}

#[sark_gen::response(raw)]
struct HelloReply {
    status: StatusCode,
    body: o3::buffer::Owned,
}

#[sark_gen::handler]
fn hello(_req: HelloRequest, _state: &()) -> HelloReply {
    let mut body = o3::buffer::Owned::new();
    body.extend_from_slice(b"hello");
    HelloReply {
        status: StatusCode::OK,
        body,
    }
}

sark_gen::define_route! {
    SmokeDispatch: () => {
        GET "/hello" => hello,
    }
}

const REQ: &[u8] = b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let addr_string: String = env_or("SARK_SMOKE_ADDR", "127.0.0.1:18080".to_string());
    let bind: std::net::SocketAddr = addr_string.parse().expect("parse bind");
    let connections: usize = env_or("SARK_SMOKE_CONNS", 4);
    let duration_secs: u64 = env_or("SARK_SMOKE_DURATION_SECS", 2);

    let cfg = ServerCfg {
        bind,
        max_conn: 1024,
        backlog: 4096,
    };

    run_with_trigger(
        bind,
        |ctx, trigger| Build::http(smoke_dispatch::new(&()), cfg.clone(), ctx, Some(trigger)),
        |bind| {
            let total_requests = Arc::new(AtomicU64::new(0));
            let total_errors = Arc::new(AtomicU64::new(0));

            let started = Instant::now();
            let load_deadline = started + Duration::from_secs(duration_secs);

            let mut handles = Vec::with_capacity(connections);
            for _ in 0..connections {
                let req_count = total_requests.clone();
                let err_count = total_errors.clone();
                let h = thread::spawn(move || {
                    let mut stream = match TcpStream::connect(bind) {
                        Ok(s) => s,
                        Err(_) => {
                            err_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };
                    stream.set_nodelay(true).ok();
                    let mut buf = [0u8; 4096];
                    while Instant::now() < load_deadline {
                        if stream.write_all(REQ).is_err() {
                            err_count.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        match stream.read(&mut buf) {
                            Ok(0) => {
                                err_count.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Ok(_) => {
                                req_count.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                err_count.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                });
                handles.push(h);
            }

            for h in handles {
                let _ = h.join();
            }

            let elapsed = started.elapsed();
            let req = total_requests.load(Ordering::Relaxed);
            let err = total_errors.load(Ordering::Relaxed);
            let rps = req as f64 / elapsed.as_secs_f64();

            println!(
                "STANDALONE_SMOKE addr={bind} connections={connections} \
                 duration_secs={duration_secs} requests={req} errors={err} \
                 elapsed_ms={elapsed_ms} rps={rps:.1}",
                elapsed_ms = elapsed.as_millis(),
            );

            if req == 0 {
                eprintln!("FAIL: zero requests succeeded");
                std::process::exit(2);
            }
        },
    );
}
