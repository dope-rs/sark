mod common;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use common::run_get;
use sark_client::connector::Config;

struct Server {
    addr: String,
    stop: mpsc::Sender<()>,
    join: Option<thread::JoinHandle<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

fn request_path(buf: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(buf);
    let line = text.lines().next()?;
    let mut parts = line.split_whitespace();
    let _method = parts.next()?;
    Some(parts.next()?.to_string())
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

fn spawn_redirect_server<F>(reply: F) -> Server
where
    F: Fn(&str) -> Vec<u8> + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (stop_tx, stop_rx) = mpsc::channel::<()>();
    let reply = std::sync::Arc::new(reply);

    let join = thread::spawn(move || {
        loop {
            if stop_rx.try_recv().is_ok() {
                return;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    let reply = reply.clone();
                    thread::spawn(move || {
                        let mut stream = stream;
                        stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
                        let mut buf = Vec::new();
                        loop {
                            let head_end = loop {
                                if let Some(p) = find_head_end(&buf) {
                                    break p;
                                }
                                let mut chunk = [0u8; 4096];
                                match stream.read(&mut chunk) {
                                    Ok(0) => return,
                                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                                    Err(_) => return,
                                }
                            };
                            let path = match request_path(&buf[..head_end]) {
                                Some(p) => p,
                                None => return,
                            };
                            let out = reply(&path);
                            if stream.write_all(&out).is_err() {
                                return;
                            }
                            let _ = stream.flush();
                            buf.drain(..head_end);
                        }
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => return,
            }
        }
    });

    common::wait_for_ready(&addr);
    Server {
        addr,
        stop: stop_tx,
        join: Some(join),
    }
}

fn run_redirect(addr: SocketAddr, path: &'static str) -> Result<sark_core::http::Response, String> {
    run_get(addr, Config::new("127.0.0.1"), path)
}

#[test]
fn same_origin_redirect_followed() {
    let server = spawn_redirect_server(|path| {
        if path == "/start" {
            b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec()
        } else {
            b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: keep-alive\r\n\r\narrived"
                .to_vec()
        }
    });
    let addr: SocketAddr = server.addr.parse().expect("addr");

    let resp = run_redirect(addr, "/start").expect("redirect followed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(std::str::from_utf8(resp.body()).unwrap(), "arrived");
}

#[test]
fn redirect_chain_followed() {
    let server = spawn_redirect_server(|path| {
        match path {
        "/a" => b"HTTP/1.1 302 Found\r\nLocation: /b\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec(),
        "/b" => b"HTTP/1.1 301 Moved Permanently\r\nLocation: /c\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec(),
        _ => b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\ndone".to_vec(),
    }
    });
    let addr: SocketAddr = server.addr.parse().expect("addr");

    let resp = run_redirect(addr, "/a").expect("chain followed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(std::str::from_utf8(resp.body()).unwrap(), "done");
}

#[test]
fn relative_redirect_keeps_directory() {
    let server = spawn_redirect_server(|path| {
        if path == "/dir/start" {
            b"HTTP/1.1 302 Found\r\nLocation: next\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec()
        } else if path == "/dir/next" {
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok".to_vec()
        } else {
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"
                .to_vec()
        }
    });
    let addr: SocketAddr = server.addr.parse().expect("addr");

    let response = run_redirect(addr, "/dir/start").expect("relative redirect followed");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.body(), b"ok");
}

#[test]
fn query_redirect_keeps_path() {
    let server = spawn_redirect_server(|path| {
        if path == "/item?step=1" {
            b"HTTP/1.1 302 Found\r\nLocation: ?step=2\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec()
        } else if path == "/item?step=2" {
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok".to_vec()
        } else {
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"
                .to_vec()
        }
    });
    let addr: SocketAddr = server.addr.parse().expect("addr");

    let response = run_redirect(addr, "/item?step=1").expect("query redirect followed");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.body(), b"ok");
}

#[test]
fn redirect_loop_rejected() {
    let server = spawn_redirect_server(|path| {
        match path {
        "/a" => b"HTTP/1.1 302 Found\r\nLocation: /b\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec(),
        _ => b"HTTP/1.1 302 Found\r\nLocation: /a\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec(),
    }
    });
    let addr: SocketAddr = server.addr.parse().expect("addr");

    let err = run_redirect(addr, "/a").expect_err("loop must be rejected");
    assert!(err.contains("loop"), "err was: {err}");
}

#[test]
fn cross_host_redirect_rejected() {
    let server = spawn_redirect_server(|_path| {
        b"HTTP/1.1 302 Found\r\nLocation: http://other.example/x\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec()
    });
    let addr: SocketAddr = server.addr.parse().expect("addr");

    let err = run_redirect(addr, "/start").expect_err("cross-host must be rejected");
    assert!(err.contains("cross-host"), "err was: {err}");
}
