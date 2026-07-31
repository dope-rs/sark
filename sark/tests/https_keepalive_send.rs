use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_test::Harness;
use dope_tls::state::{self, State};
use dope_tls::tls::Tls;
use http::StatusCode;
use sark::{Executor, Throughput, driver};
use shin::crypto::sig::SigningKey;

const SEED: [u8; 32] = [7u8; 32];

#[sark_gen::request]
struct HelloRequest {}

#[sark_gen::response(raw)]
struct HelloReply {
    status: StatusCode,
    body: Vec<u8>,
}

#[sark_gen::handler]
fn hello(_req: HelloRequest, _state: &()) -> HelloReply {
    let mut body = Vec::new();
    body.extend_from_slice(b"hello");
    HelloReply {
        status: StatusCode::OK,
        body,
    }
}

const BIG_LEN: usize = 512 * 1024;

#[sark_gen::handler]
fn big(_req: HelloRequest, _state: &()) -> HelloReply {
    let mut body = Vec::new();
    let chunk = [b'x'; 1024];
    for _ in 0..(BIG_LEN / 1024) {
        body.extend_from_slice(&chunk);
    }
    HelloReply {
        status: StatusCode::OK,
        body,
    }
}

sark_gen::define_route! {
    TlsDispatch: () => {
        GET "/hello" => hello,
        GET "/big" => big,
    }
}

struct TlsClient {
    stream: TcpStream,
    state: State<state::sessions::Client>,
    plain: Vec<u8>,
}

impl TlsClient {
    fn connect(addr: std::net::SocketAddr) -> Self {
        let signing = SigningKey::from_seed(&SEED).expect("signing key");
        let expected_pubkey = *signing.pubkey().unwrap();
        let state = State::<state::sessions::Client>::new(shin::client::config::Config {
            verifier: shin::client::config::Verifier::RawPublicKey { expected_pubkey },
            transport_params: Vec::new(),
            alpn_protocols: Vec::new(),
            resumption: None,
            enable_early_data: false,
        })
        .expect("client state");
        let stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .expect("read timeout");
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .expect("write timeout");
        let mut me = Self {
            stream,
            state,
            plain: Vec::new(),
        };
        me.handshake();
        me
    }

    fn flush(&mut self) {
        loop {
            let pending = self.state.pending_send_slice();
            if pending.is_empty() {
                break;
            }
            let len = pending.len();
            self.stream.write_all(pending).expect("write wire");
            self.state.consume_pending_send(len).expect("consume wire");
        }
    }

    fn pump_once(&mut self) -> usize {
        let mut chunk = [0u8; 8192];
        let n = self.stream.read(&mut chunk).expect("read wire");
        if n == 0 {
            return 0;
        }
        let plain = &mut self.plain;
        self.state
            .read_tcp(&chunk[..n], |app| plain.extend_from_slice(app))
            .expect("read_tcp");
        n
    }

    fn handshake(&mut self) {
        for _ in 0..64 {
            self.flush();
            if self.state.is_established() {
                return;
            }
            if self.pump_once() == 0 {
                panic!("server closed during handshake");
            }
        }
        panic!("handshake did not complete");
    }

    fn request(&mut self, path: &str, addr: std::net::SocketAddr) {
        let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n\r\n");
        self.state.write_app(req.as_bytes()).expect("write_app");
        self.flush();
    }

    fn response(&mut self) -> Vec<u8> {
        loop {
            if let Some(end) = full_response_end(&self.plain) {
                let resp: Vec<u8> = self.plain.drain(..end).collect();
                return resp;
            }
            if self.pump_once() == 0 {
                panic!(
                    "server closed before full response; have {} bytes buffered",
                    self.plain.len()
                );
            }
        }
    }
}

fn full_response_end(buf: &[u8]) -> Option<usize> {
    let body_start = buf.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
    let headers = std::str::from_utf8(&buf[..body_start]).ok()?;
    let mut cl = 0usize;
    for line in headers.split("\r\n") {
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            cl = v.trim().parse().ok()?;
        }
    }
    if buf.len() >= body_start + cl {
        Some(body_start + cl)
    } else {
        None
    }
}

fn assert_hello(resp: &[u8]) {
    let text = std::str::from_utf8(resp).expect("utf8 response");
    assert!(text.starts_with("HTTP/1.1 200 "), "status line: {text:?}");
    let body_start = text.find("\r\n\r\n").expect("header terminator") + 4;
    assert_eq!(&text[body_start..], "hello", "body mismatch: {text:?}");
}

fn assert_big(resp: &[u8]) {
    let head_end = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header terminator")
        + 4;
    let head = std::str::from_utf8(&resp[..head_end]).expect("utf8 headers");
    assert!(head.starts_with("HTTP/1.1 200 "), "status line: {head:?}");
    let body = &resp[head_end..];
    assert_eq!(body.len(), BIG_LEN, "body length");
    assert!(body.iter().all(|&b| b == b'x'), "body content");
}

fn server(bind: std::net::SocketAddr) -> support::TestHttpsServer {
    support::https_server(bind, Duration::from_secs(10))
}

fn tls_config() -> shin::server::config::Config {
    shin::server::config::Config {
        source: shin::server::config::CertSource::RawPublicKey {
            signing_key: SigningKey::from_seed(&SEED).expect("signing key"),
        },
        alpn_protocols: Vec::new(),
        ticket_keys: None,
    }
}

#[test]
fn https_server_topology_is_send_and_clone_without_tls_state() {
    fn assert_send_and_clone<T: Send + Clone>() {}

    assert_send_and_clone::<support::TestHttpsServer>();
}

#[test]
fn https_streams_large_body() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let server = server(bind);
    harness
        .run_with_trigger(
            move |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let storage = sark::app::ServerStorage::<(), Tls>::try_with_capacity(
                    support::MAX_CONNECTIONS,
                )?;
                let executor = Executor::new(driver_config)?.with_storage(storage);
                executor.enter(|mut session| {
                    let timer =
                        sark::Timer::with_capacity(support::MAX_CONNECTIONS.saturating_mul(2));
                    server.serve(
                        &mut session,
                        TlsDispatch::new(
                            &(),
                            &timer,
                            sark::app::Config {
                                timer_capacity: support::MAX_CONNECTIONS.saturating_mul(2),
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        tls_config(),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let mut client = TlsClient::connect(bind);
                client.request("/big", bind);
                assert_big(&client.response());
            },
        )
        .expect("harness");
}

#[test]
fn https_keepalive_serves_two_requests() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let server = server(bind);

    harness
        .run_with_trigger(
            move |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let storage = sark::app::ServerStorage::<(), Tls>::try_with_capacity(
                    support::MAX_CONNECTIONS,
                )?;
                let executor = Executor::new(driver_config)?.with_storage(storage);
                executor.enter(|mut session| {
                    let timer =
                        sark::Timer::with_capacity(support::MAX_CONNECTIONS.saturating_mul(2));
                    server.serve(
                        &mut session,
                        TlsDispatch::new(
                            &(),
                            &timer,
                            sark::app::Config {
                                timer_capacity: support::MAX_CONNECTIONS.saturating_mul(2),
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        tls_config(),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let mut client = TlsClient::connect(bind);
                client.request("/hello", bind);
                assert_hello(&client.response());
                client.request("/hello", bind);
                assert_hello(&client.response());
            },
        )
        .expect("harness");
}
mod support;
