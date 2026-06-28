use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;

use dope_extra::testing::{ephemeral_addr, run_with_trigger};
use sark_grpc::Status;
use sark_grpc::server::{self, Cfg, Handler, Request, Response};
use shin::server::{CertSource, Config as TlsConfig};
use shin::sig::SigningKey;

const SEED: [u8; 32] = [9u8; 32];

struct NopHandler;

impl Handler for NopHandler {
    fn on_request(&mut self, _request: Request, response: &mut Response) {
        response.status = Status::ok();
    }
}

fn connect_retry(addr: SocketAddr) -> TcpStream {
    for _ in 0..200 {
        if let Ok(stream) = TcpStream::connect(addr) {
            return stream;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("readiness port never came up: {addr}");
}

fn probe_liveness(addr: SocketAddr) -> String {
    let mut stream = connect_retry(addr);
    stream
        .write_all(
            b"GET /baseline11?a=1&b=1 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .expect("write request");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read response");
    String::from_utf8_lossy(&buf).into_owned()
}

fn grpc_cfg(bind: SocketAddr, readiness: Option<SocketAddr>) -> Cfg {
    Cfg {
        bind,
        readiness,
        max_conn: 64,
        backlog: 128,
        grpc: Default::default(),
    }
}

fn tls_config() -> TlsConfig {
    TlsConfig {
        source: CertSource::RawPublicKey {
            signing_key: SigningKey::from_seed(&SEED).expect("signing key"),
        },
        transport_params: Vec::new(),
        alpn_protocols: Vec::new(),
        ticket_keys: None,
        accept_early_data: false,
    }
}

#[test]
fn h2c_answers_plaintext_liveness() {
    let bind = ephemeral_addr();
    let cfg = grpc_cfg(bind, Some(bind));
    run_with_trigger(
        bind,
        move |ctx, trigger| server::serve(NopHandler, cfg.clone(), ctx, Some(trigger)),
        |bind| {
            let response = probe_liveness(bind);
            assert!(
                response.starts_with("HTTP/1.1 200"),
                "expected 200 from h2c liveness, got: {response:?}"
            );
        },
    );
}

#[test]
fn h2c_preface_is_not_treated_as_liveness() {
    let bind = ephemeral_addr();
    let cfg = grpc_cfg(bind, Some(bind));
    run_with_trigger(
        bind,
        move |ctx, trigger| server::serve(NopHandler, cfg.clone(), ctx, Some(trigger)),
        |bind| {
            let mut stream = connect_retry(bind);
            stream
                .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
                .expect("write preface");
            stream
                .set_read_timeout(Some(Duration::from_millis(300)))
                .expect("set timeout");
            let mut buf = [0u8; 64];
            let read = stream.read(&mut buf).unwrap_or(0);
            let response = String::from_utf8_lossy(&buf[..read]).into_owned();
            assert!(
                !response.starts_with("HTTP/1.1"),
                "h2 preface must stay on the gRPC path, got h1 reply: {response:?}"
            );
        },
    );
}

#[test]
fn tls_answers_plaintext_liveness_on_sidecar_port() {
    let tls_bind = ephemeral_addr();
    let readiness = ephemeral_addr();
    let cfg = grpc_cfg(tls_bind, Some(readiness));
    run_with_trigger(
        tls_bind,
        move |ctx, trigger| {
            server::serve_tls(NopHandler, cfg.clone(), tls_config(), ctx, Some(trigger))
        },
        move |_tls_bind| {
            let response = probe_liveness(readiness);
            assert!(
                response.starts_with("HTTP/1.1 200"),
                "expected 200 from tls sidecar liveness, got: {response:?}"
            );
        },
    );
}
