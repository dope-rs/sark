#![allow(dead_code)]

use std::marker::PhantomData;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::pin;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use dope::driver;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::Executor;
use dope::runtime::profile::Balanced;
use dope_fiber::SessionExt as _;
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use o3::cell::BrandCell as Branded;
use sark_client::connector::{Config, HttpHandle, Port, Session};

pub(crate) type PlainHttp<'d> =
    Connector<'d, 0, Session<'d>, Static<Tcp>, Bundle<Tcp, Identity, Balanced>>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
pub(crate) struct ConnRt<'d> {
    #[pin]
    #[manifold]
    conn: PlainHttp<'d>,
    _ph: PhantomData<&'d ()>,
}

pub(crate) fn run_gets(
    addr: SocketAddr,
    config: Config,
    capacity: usize,
    paths: &'static [&'static str],
) -> Result<Vec<u16>, String> {
    let exec = Executor::new(driver::Config::for_tcp_profile::<Balanced>(4))
        .expect("driver")
        .with_storage_factory(
            Port::factory(config, capacity, 1).expect("the test request pool layout is valid"),
        );
    exec.enter(|mut sess| {
        let backoff = sess.seed().derive(dope::hash::domain::BACKOFF).state();
        // The port is stored for the full executor session. Preserve its driver
        // lifetime brand while releasing the short borrow needed to access the
        // driver itself.
        let port = sess.storage() as *const Port<'_>;
        let port = unsafe { &*port };
        let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(200), backoff);
        let conn = {
            let mut driver = sess.driver_access();
            Connector::new(Session::new(port), upstreams, port.capacity(), &mut driver)
                .expect("connector")
        };
        let rt = pin!(Branded::new(ConnRt {
            conn,
            _ph: PhantomData,
        }));
        let client = HttpHandle::from_cell(ConnRt::conn_ref(rt.as_ref().borrow_pin(sess.token())));

        sess.block_on(rt.as_ref(), client.wait_active())
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let mut codes = Vec::with_capacity(paths.len());
        for path in paths {
            let response = sess
                .block_on(rt.as_ref(), client.get(path))
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            codes.push(response.status().as_u16());
        }
        Ok(codes)
    })
}

pub(crate) fn run_gets_with_gap(
    addr: SocketAddr,
    config: Config,
    capacity: usize,
    batch1: &'static [&'static str],
    batch2: &'static [&'static str],
    gap: Duration,
) -> Result<Vec<u16>, String> {
    let exec = Executor::new(driver::Config::for_tcp_profile::<Balanced>(4))
        .expect("driver")
        .with_storage_factory(
            Port::factory(config, capacity, 1).expect("the test request pool layout is valid"),
        );
    exec.enter(|mut sess| {
        let backoff = sess.seed().derive(dope::hash::domain::BACKOFF).state();
        // See `run_gets`: the raw pointer only separates two non-overlapping
        // borrows of the same session; the port never outlives the session.
        let port = sess.storage() as *const Port<'_>;
        let port = unsafe { &*port };
        let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(200), backoff);
        let conn = {
            let mut driver = sess.driver_access();
            Connector::new(Session::new(port), upstreams, port.capacity(), &mut driver)
                .expect("connector")
        };
        let rt = pin!(Branded::new(ConnRt {
            conn,
            _ph: PhantomData,
        }));
        let client = HttpHandle::from_cell(ConnRt::conn_ref(rt.as_ref().borrow_pin(sess.token())));

        sess.block_on(rt.as_ref(), client.wait_active())
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let mut codes = Vec::with_capacity(batch1.len() + batch2.len());
        for path in batch1 {
            let response = sess
                .block_on(rt.as_ref(), client.get(path))
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            codes.push(response.status().as_u16());
        }
        sess.block_on(rt.as_ref(), client.sleep(gap))
            .map_err(|error| error.to_string())?;
        for path in batch2 {
            let response = sess
                .block_on(rt.as_ref(), client.get(path))
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            codes.push(response.status().as_u16());
        }
        Ok(codes)
    })
}

pub(crate) fn run_get(
    addr: SocketAddr,
    config: Config,
    path: &'static str,
) -> Result<sark_core::http::Response, String> {
    let exec = Executor::new(driver::Config::for_tcp_profile::<Balanced>(4))
        .expect("driver")
        .with_storage_factory(
            Port::factory(config, 1, 1).expect("the test request pool layout is valid"),
        );
    exec.enter(|mut sess| {
        let backoff = sess.seed().derive(dope::hash::domain::BACKOFF).state();
        // See `run_gets`: retain the port's driver brand across the mutable
        // driver access without extending the executor session itself.
        let port = sess.storage() as *const Port<'_>;
        let port = unsafe { &*port };
        let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(200), backoff);
        let conn = {
            let mut driver = sess.driver_access();
            Connector::new(Session::new(port), upstreams, 1, &mut driver).expect("connector")
        };
        let rt = pin!(Branded::new(ConnRt {
            conn,
            _ph: PhantomData,
        }));
        let client = HttpHandle::from_cell(ConnRt::conn_ref(rt.as_ref().borrow_pin(sess.token())));

        sess.block_on(rt.as_ref(), client.wait_active())
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let result = sess.block_on(rt.as_ref(), client.get(path));
        result
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())
    })
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
    let mut out = Vec::new();
    out.extend_from_slice(status_line.as_bytes());
    out.extend_from_slice(b"\r\n");
    for (name, value) in headers {
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
    out
}
