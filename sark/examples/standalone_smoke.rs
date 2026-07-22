use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use dope_extra::harness::Harness;
use http::StatusCode;
use sark::{HttpServer, Tcp, Throughput, app, driver, listener, tcp};

#[sark_gen::request]
struct HelloRequest {}

#[sark_gen::response(raw)]
struct HelloReply {
    status: StatusCode,
    body: &'static [u8],
}

#[sark_gen::handler]
fn hello(_req: HelloRequest, _state: &()) -> HelloReply {
    HelloReply {
        status: StatusCode::OK,
        body: b"hello",
    }
}

sark_gen::define_route! {
    SmokeDispatch: () => {
        GET "/hello" => hello,
    }
}

const REQ: &[u8] = b"GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";
const MAX_CONNECTIONS: usize = 1024;
const HTTP_LISTENER_ID: u8 = 0;
const DATE_UPDATER_ID: u8 = 1;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn read_response(stream: &mut TcpStream, pending: &mut Vec<u8>) -> io::Result<bool> {
    loop {
        if let Some(head_end) = pending.windows(4).position(|part| part == b"\r\n\r\n") {
            let head = std::str::from_utf8(&pending[..head_end])
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let mut lines = head.split("\r\n");
            let status = lines
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse::<u16>().ok());
            let content_length = lines.find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            });
            let Some(content_length) = content_length else {
                return Ok(false);
            };
            let total = head_end + 4 + content_length;
            if pending.len() >= total {
                let valid = status == Some(200) && &pending[head_end + 4..total] == b"hello";
                pending.drain(..total);
                return Ok(valid);
            }
        }
        let mut chunk = [0u8; 4096];
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Ok(false);
        }
        pending.extend_from_slice(&chunk[..read]);
    }
}

fn main() {
    let addr_string: String = env_or("SARK_SMOKE_ADDR", "127.0.0.1:18080".to_string());
    let bind: std::net::SocketAddr = addr_string.parse().expect("parse bind");
    let connections: usize = env_or("SARK_SMOKE_CONNS", 4);
    let duration_secs: u64 = env_or("SARK_SMOKE_DURATION_SECS", 2);
    let deadline_secs: u64 = env_or("SARK_SMOKE_DEADLINE_SECS", duration_secs + 5);

    let server = HttpServer::<HTTP_LISTENER_ID, DATE_UPDATER_ID, Throughput>::new(
        listener::Config::<Tcp> {
            bind,
            max_connections: MAX_CONNECTIONS,
            backlog: 4096,
            stream: tcp::stream::Config {
                no_delay: Some(true),
                ..Default::default()
            },
            transport: tcp::listener::Config {
                reuse_port: true,
                ..Default::default()
            },
            egress: Default::default(),
        },
        Duration::from_secs(10),
    );

    Harness::new(bind)
        .run_with_trigger(
            |_ctx, trigger| {
                let driver_config = driver::Config::for_tcp_profile::<Throughput>(MAX_CONNECTIONS);
                server.clone().run_worker(driver_config, |server, session| {
                    server.serve(
                        session,
                        SmokeDispatch::new(
                            (),
                            app::Config {
                                timer_capacity: MAX_CONNECTIONS.saturating_mul(2),
                                task_capacity: MAX_CONNECTIONS,
                            },
                        ),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let total_requests = Arc::new(AtomicU64::new(0));
                let total_errors = Arc::new(AtomicU64::new(0));

                let started = Instant::now();
                let load_deadline = started + Duration::from_secs(duration_secs);
                let smoke_deadline = started + Duration::from_secs(deadline_secs);

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
                        stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
                        stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
                        let mut pending = Vec::with_capacity(4096);
                        while Instant::now() < load_deadline && Instant::now() < smoke_deadline {
                            if stream.write_all(REQ).is_err() {
                                err_count.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            match read_response(&mut stream, &mut pending) {
                                Ok(true) => {
                                    req_count.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(false) | Err(_) => {
                                    err_count.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                            }
                        }
                    });
                    handles.push(h);
                }

                for h in handles {
                    if Instant::now() >= smoke_deadline {
                        total_errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
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

                if req == 0 || err != 0 || Instant::now() >= smoke_deadline {
                    eprintln!("FAIL: smoke validation failed");
                    std::process::exit(2);
                }
            },
        )
        .expect("harness");
}
