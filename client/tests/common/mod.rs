#![allow(dead_code)]

use std::marker::PhantomData;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::pin;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use dope::driver;
use dope::manifold::connector::session::Connector;
use dope::manifold::connector::source::health::Static;
use dope::manifold::env::Bundle;
use dope::runtime::executor::Executor;
use dope::runtime::profile::Balanced;
use dope_fiber::extensions::SessionExt as _;
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
        let port = sess.storage();
        let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(200), backoff);
        let conn = {
            let mut driver = sess.driver_access();
            Connector::new(
                Session::new(port),
                upstreams,
                port.capacity(),
                port.egress(),
                &mut driver,
            )
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
        let port = sess.storage();
        let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(200), backoff);
        let conn = {
            let mut driver = sess.driver_access();
            Connector::new(
                Session::new(port),
                upstreams,
                port.capacity(),
                port.egress(),
                &mut driver,
            )
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
        let port = sess.storage();
        let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(200), backoff);
        let conn = {
            let mut driver = sess.driver_access();
            Connector::new(Session::new(port), upstreams, 1, port.egress(), &mut driver)
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
        let result = sess.block_on(rt.as_ref(), client.get(path));
        result
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())
    })
}

pub(crate) fn run_stream_get(
    addr: SocketAddr,
    config: Config,
    path: &'static str,
) -> Result<(u16, Vec<u8>, usize, usize), String> {
    use sark_client::connector::ResponseEvent;

    let exec = Executor::new(driver::Config::for_tcp_profile::<Balanced>(4))
        .expect("driver")
        .with_storage_factory(
            Port::factory(config, 1, 1).expect("the test request pool layout is valid"),
        );
    exec.enter(|mut sess| {
        let backoff = sess.seed().derive(dope::hash::domain::BACKOFF).state();
        let port = sess.storage();
        let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(200), backoff);
        let conn = {
            let mut driver = sess.driver_access();
            Connector::new(Session::new(port), upstreams, 1, port.egress(), &mut driver)
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
        let mut stream = sess
            .block_on(rt.as_ref(), client.get_stream(path))
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let mut status = None;
        let mut body = Vec::new();
        let mut trailer_count = 0;
        let mut informational_count = 0;
        loop {
            let event = sess
                .block_on(rt.as_ref(), stream.next_event())
                .map_err(|error| error.to_string())?;
            let Some(event) = event else {
                break;
            };
            match event.map_err(|error| error.to_string())? {
                ResponseEvent::Informational(_) => informational_count += 1,
                ResponseEvent::Head(response) => status = Some(response.status().as_u16()),
                ResponseEvent::Data(data) => body.extend_from_slice(data.as_ref()),
                ResponseEvent::Trailers(trailers) => trailer_count += trailers.len(),
            }
        }
        Ok((
            status.ok_or_else(|| "missing final response head".to_owned())?,
            body,
            trailer_count,
            informational_count,
        ))
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
