#![allow(dead_code)]

use std::marker::PhantomData;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::pin;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use dope::fiber::Fiber;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::profile::Production;
use dope::transport::Tcp;
use dope::wire::Identity;
use dope::{DriverCfg, DriverConfig, Executor};
use sark_client::connector::{Client, Session};

pub(crate) type PlainHttp = Connector<0, Session, Static<Tcp>, Bundle<Tcp, Identity, Production>>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
pub(crate) struct ConnRt<'d> {
    #[pin]
    #[manifold]
    conn: PlainHttp,
    _ph: PhantomData<&'d ()>,
}

pub(crate) fn run_gets(
    addr: SocketAddr,
    session: Session,
    capacity: usize,
    paths: &'static [&'static str],
) -> Result<Vec<u16>, String> {
    let mut exec = Executor::new(DriverCfg::for_tcp_profile::<Production>(4)).expect("driver");
    let driver = exec.driver_mut();
    let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(200));
    let mut rt = pin!(ConnRt {
        conn: Connector::new(session, upstreams, capacity, driver),
        _ph: PhantomData,
    });
    let client = rt.as_mut().conn_handle();

    let result = dope_extra::block_on(
        &mut exec,
        rt.as_mut(),
        Fiber::new(async move {
            client.wait_active().await?;
            let mut codes = Vec::with_capacity(paths.len());
            for p in paths {
                codes.push(client.get(p).await?.status().as_u16());
            }
            Ok::<_, sark_client::connector::Error>(codes)
        }),
    );
    result.map_err(|e| e.to_string())
}

/// Like `run_gets`, but sleeps `gap` between `batch1` and `batch2`. The gap lets
/// the connector proactively redial idle connections that then go stale (when
/// `gap` exceeds the session idle_timeout); the second batch must still succeed.
/// This exercises the idle-stale recycle path (`poke_close`) that previously
/// leaked dialer slots and starved every subsequent acquire.
pub(crate) fn run_gets_with_gap(
    addr: SocketAddr,
    session: Session,
    capacity: usize,
    batch1: &'static [&'static str],
    batch2: &'static [&'static str],
    gap: Duration,
) -> Result<Vec<u16>, String> {
    let mut exec = Executor::new(DriverCfg::for_tcp_profile::<Production>(4)).expect("driver");
    let driver = exec.driver_mut();
    let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(200));
    let mut rt = pin!(ConnRt {
        conn: Connector::new(session, upstreams, capacity, driver),
        _ph: PhantomData,
    });
    let client = rt.as_mut().conn_handle();

    let result = dope_extra::block_on(
        &mut exec,
        rt.as_mut(),
        Fiber::new(async move {
            client.wait_active().await?;
            let mut codes = Vec::with_capacity(batch1.len() + batch2.len());
            for p in batch1 {
                codes.push(client.get(p).await?.status().as_u16());
            }
            client.sleep(gap).await;
            for p in batch2 {
                codes.push(client.get(p).await?.status().as_u16());
            }
            Ok::<_, sark_client::connector::Error>(codes)
        }),
    );
    result.map_err(|e| e.to_string())
}

pub(crate) fn run_get(
    addr: SocketAddr,
    session: Session,
    path: &'static str,
) -> Result<sark_core::http::Response, String> {
    let mut exec = Executor::new(DriverCfg::for_tcp_profile::<Production>(4)).expect("driver");
    let driver = exec.driver_mut();
    let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(200));
    let mut rt = pin!(ConnRt {
        conn: Connector::new(session, upstreams, 1, driver),
        _ph: PhantomData,
    });
    let client = rt.as_mut().conn_handle();

    let result = dope_extra::block_on(
        &mut exec,
        rt.as_mut(),
        Fiber::new(async move {
            client.wait_active().await?;
            client.get(path).await
        }),
    );
    result.map_err(|e| e.to_string())
}

pub(crate) struct TestServer {
    addr: String,
    stop: mpsc::Sender<()>,
    join: Option<thread::JoinHandle<()>>,
}

impl TestServer {
    pub(crate) fn addr(&self) -> &str {
        &self.addr
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

pub(crate) fn wait_for_ready(addr: &str) {
    for _ in 0..200 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("server did not start: {addr}");
}

pub(crate) fn spawn_raw_server<F>(handler: F) -> TestServer
where
    F: Fn(&mut TcpStream, &[u8]) + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (stop_tx, stop_rx) = mpsc::channel::<()>();

    let join = thread::spawn(move || {
        loop {
            if stop_rx.try_recv().is_ok() {
                return;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buf = [0u8; 8192];
                    let n = std::io::Read::read(&mut stream, &mut buf).unwrap_or(0);
                    handler(&mut stream, &buf[..n]);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => return,
            }
        }
    });

    wait_for_ready(&addr);
    TestServer {
        addr,
        stop: stop_tx,
        join: Some(join),
    }
}

pub(crate) fn spawn_raw_server_with_state<S, F>(state: S, handler: F) -> TestServer
where
    S: Send + 'static,
    F: Fn(&S, &mut TcpStream, &[u8]) + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (stop_tx, stop_rx) = mpsc::channel::<()>();

    let join = thread::spawn(move || {
        loop {
            if stop_rx.try_recv().is_ok() {
                return;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buf = [0u8; 8192];
                    let n = std::io::Read::read(&mut stream, &mut buf).unwrap_or(0);
                    handler(&state, &mut stream, &buf[..n]);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => return,
            }
        }
    });

    wait_for_ready(&addr);
    TestServer {
        addr,
        stop: stop_tx,
        join: Some(join),
    }
}

pub(crate) fn raw_http_response(
    status_line: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Vec<u8> {
    sark_core::test_util::RawHttpResponse::build(status_line, headers, body)
}
