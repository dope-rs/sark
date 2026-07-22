use o3::cell::BrandCell as Branded;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use dope::DriverContext;
use dope::manifold::Manifold;
use dope::manifold::TypedToken;
use dope::manifold::listener::{self, Listener};
use dope::runtime::profile::Throughput;
use dope::runtime::{Executor, Idle, ShutdownTrigger, WorkerContext};
use dope_extra::harness::Harness;
use dope_fiber::{Context, Fiber, Ready, ready};
use dope_net::tcp::Tcp;
use o3::buffer::Shared;
use sark_h2::hpack::OwnedHeader;
use sark_h2::server::{App, Body, Config, Env, Handler, Request, Response, serve, serve_sync};
use sark_h2::{ClientRole, Conn, ErrorCode, Header, StreamId, conn};

fn server_config(bind_addr: SocketAddr, max_handler_tasks: usize) -> Config {
    Config {
        bind_addr,
        max_connections: 64,
        max_connections_per_ip: 64,
        listen_backlog: 128,
        max_handler_tasks,
        max_request_body_bytes: 16 << 20,
        max_connection_body_bytes: 64 << 20,
        max_outbound_bytes: 64 << 10,
        socket_receive_buffer_bytes: None,
        socket_send_buffer_bytes: None,
        tcp_fast_open_backlog: None,
        receive_buffer_bytes: 64 << 10,
        receive_buffer_count: 1024,
    }
}

struct Echo;

impl Handler for Echo {
    type Fut<'h> = Ready<Response>;

    fn request<'h>(&'h self, req: Request) -> Self::Fut<'h> {
        let method = req
            .headers
            .iter()
            .find(|h| h.name == b":method")
            .map(|h| h.value)
            .unwrap_or_default();
        let has_path = req.headers.iter().any(|h| h.name == b":path");
        let headers = vec![
            OwnedHeader::new(b":status", b"200"),
            OwnedHeader::new(b"x-method", method),
            OwnedHeader::new(b"x-has-path", if has_path { b"1" } else { b"0" }),
        ];
        ready(Response::new(headers, Shared::copy_from_slice(&req.body)))
    }
}

struct YieldResponse {
    response: Option<Response>,
    yielded: bool,
}

impl<'d> Fiber<'d> for YieldResponse {
    type Output = Response;
    fn poll(mut self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        if !self.yielded {
            self.yielded = true;
            cx.waker().wake();
            return Poll::Pending;
        }
        Poll::Ready(self.response.take().unwrap())
    }
}

struct YieldEcho;

impl Handler for YieldEcho {
    type Fut<'h> = YieldResponse;

    fn request<'h>(&'h self, req: Request) -> Self::Fut<'h> {
        YieldResponse {
            response: Some(Response::new(
                vec![OwnedHeader::new(b":status", b"200")],
                Shared::copy_from_slice(&req.body),
            )),
            yielded: false,
        }
    }
}

struct HoldOrReply {
    response: Option<Response>,
}

impl<'d> Fiber<'d> for HoldOrReply {
    type Output = Response;
    fn poll(mut self: Pin<&mut Self>, _cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        match self.response.take() {
            Some(response) => Poll::Ready(response),
            None => Poll::Pending,
        }
    }
}

struct CapacityHandler;

impl Handler for CapacityHandler {
    type Fut<'h> = HoldOrReply;

    fn request<'h>(&'h self, req: Request) -> Self::Fut<'h> {
        let ready = req
            .headers
            .iter()
            .any(|header| header.name == b":path" && header.value == b"/ready");
        HoldOrReply {
            response: ready.then(|| {
                Response::new(
                    vec![OwnedHeader::new(b":status", b"200")],
                    Shared::copy_from_slice(b"ready"),
                )
            }),
        }
    }
}

enum PanicPoll {
    Panic {
        yielded: bool,
        panics: Arc<AtomicUsize>,
        paired_ready: Option<Arc<AtomicBool>>,
    },
    YieldReady {
        yielded: bool,
        paired_ready: Arc<AtomicBool>,
        response: Option<Response>,
    },
    Ready(Option<Response>),
}

impl<'d> Fiber<'d> for PanicPoll {
    type Output = Response;
    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        match unsafe { self.get_unchecked_mut() } {
            Self::Panic { yielded, .. } if !*yielded => {
                *yielded = true;
                cx.wake();
                Poll::Pending
            }
            Self::Panic {
                panics,
                paired_ready,
                ..
            } if paired_ready
                .as_ref()
                .is_some_and(|ready| !ready.load(Ordering::Acquire)) =>
            {
                cx.wake();
                Poll::Pending
            }
            Self::Panic { panics, .. } => {
                panics.fetch_add(1, Ordering::Release);
                panic!("registered handler poll panic")
            }
            Self::YieldReady {
                yielded,
                paired_ready,
                response,
            } if !*yielded => {
                *yielded = true;
                paired_ready.store(true, Ordering::Release);
                cx.wake();
                Poll::Pending
            }
            Self::YieldReady { response, .. } => Poll::Ready(response.take().unwrap()),
            Self::Ready(response) => Poll::Ready(response.take().unwrap()),
        }
    }
}

impl Drop for PanicPoll {
    fn drop(&mut self) {
        assert!(
            !matches!(self, Self::Panic { .. }),
            "handler fiber drop panic"
        );
    }
}

struct PanicHandler {
    panics: Arc<AtomicUsize>,
    paired_ready: Option<Arc<AtomicBool>>,
}

impl Handler for PanicHandler {
    type Fut<'h> = PanicPoll;

    fn request<'h>(&'h self, req: Request) -> Self::Fut<'h> {
        let path = req
            .headers
            .iter()
            .find(|header| header.name == b":path")
            .map(|header| header.value);
        if path == Some(b"/ready") {
            PanicPoll::Ready(Some(Response::new(
                vec![OwnedHeader::new(b":status", b"200")],
                Shared::copy_from_slice(b"reused"),
            )))
        } else if path == Some(b"/after") {
            PanicPoll::YieldReady {
                yielded: false,
                paired_ready: Arc::clone(self.paired_ready.as_ref().unwrap()),
                response: Some(Response::new(
                    vec![OwnedHeader::new(b":status", b"200")],
                    Shared::copy_from_slice(b"after"),
                )),
            }
        } else {
            PanicPoll::Panic {
                yielded: false,
                panics: Arc::clone(&self.panics),
                paired_ready: self.paired_ready.as_ref().map(Arc::clone),
            }
        }
    }
}

struct SelfWakeState {
    polls: AtomicUsize,
    stop: AtomicBool,
}

enum SelfWakePoll {
    Pending(Arc<SelfWakeState>),
    Ready(Option<Response>),
}

impl<'d> Fiber<'d> for SelfWakePoll {
    type Output = Response;
    fn poll(self: Pin<&mut Self>, cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        match unsafe { self.get_unchecked_mut() } {
            Self::Pending(state) => {
                state.polls.fetch_add(1, Ordering::Release);
                if state.stop.load(Ordering::Acquire) {
                    return Poll::Ready(Response::new(
                        vec![OwnedHeader::new(b":status", b"200")],
                        Shared::copy_from_slice(b"stopped"),
                    ));
                }
                cx.wake();
                Poll::Pending
            }
            Self::Ready(response) => Poll::Ready(response.take().unwrap()),
        }
    }
}

struct SelfWakeHandler {
    state: Arc<SelfWakeState>,
}

impl Handler for SelfWakeHandler {
    type Fut<'h> = SelfWakePoll;

    fn request<'h>(&'h self, req: Request) -> Self::Fut<'h> {
        if req
            .headers
            .iter()
            .any(|header| header.name == b":path" && header.value == b"/ready")
        {
            SelfWakePoll::Ready(Some(Response::new(
                vec![OwnedHeader::new(b":status", b"200")],
                Shared::copy_from_slice(b"fair"),
            )))
        } else {
            SelfWakePoll::Pending(Arc::clone(&self.state))
        }
    }
}

struct DropPendingState {
    started: AtomicUsize,
    dropped: AtomicUsize,
}

struct DropPending {
    state: Arc<DropPendingState>,
    started: bool,
}

impl<'d> Fiber<'d> for DropPending {
    type Output = Response;
    fn poll(mut self: Pin<&mut Self>, _cx: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        if !self.started {
            self.started = true;
            self.state.started.fetch_add(1, Ordering::Release);
        }
        Poll::Pending
    }
}

impl Drop for DropPending {
    fn drop(&mut self) {
        self.state.dropped.fetch_add(1, Ordering::Release);
    }
}

struct DropPendingHandler {
    state: Arc<DropPendingState>,
}

impl Handler for DropPendingHandler {
    type Fut<'h> = DropPending;

    fn request<'h>(&'h self, _req: Request) -> Self::Fut<'h> {
        DropPending {
            state: Arc::clone(&self.state),
            started: false,
        }
    }
}

#[repr(transparent)]
struct DropLive<M> {
    inner: M,
}

impl<'d, M: Manifold<'d>> Manifold<'d> for DropLive<M> {
    const ID: u8 = M::ID;

    fn dispatch(self: Pin<&mut Self>, ev: dope::Event, driver: &mut DriverContext<'_, 'd>) {
        M::dispatch(
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) },
            ev,
            driver,
        );
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let target = unsafe { TypedToken::<M>::new_unchecked(target.into_inner()) };
        M::activate(
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) },
            target,
            driver,
        );
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        M::pre_park(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
        M::idle(unsafe { self.map_unchecked(|s| &s.inner) })
    }

    fn shutdown(self: Pin<&mut Self>, _driver: &mut DriverContext<'_, 'd>) {
        let _ = self;
    }
}

type PanicListener<'d> = Listener<'d, 0, App<'d, PanicHandler>, Env>;

#[repr(transparent)]
struct PanicIsolated<'d> {
    inner: PanicListener<'d>,
}

impl<'d> Manifold<'d> for PanicIsolated<'d> {
    const ID: u8 = 0;

    fn dispatch(self: Pin<&mut Self>, ev: dope::Event, driver: &mut DriverContext<'_, 'd>) {
        Manifold::dispatch(
            unsafe { self.map_unchecked_mut(|s| &mut s.inner) },
            ev,
            driver,
        );
    }

    fn activate(
        self: Pin<&mut Self>,
        target: TypedToken<Self>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let target = unsafe { TypedToken::<PanicListener<'d>>::new_unchecked(target.into_inner()) };
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            Manifold::activate(inner, target, driver);
        }));
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        Manifold::pre_park(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, driver);
    }

    fn idle(self: Pin<&Self>) -> Idle {
        Manifold::idle(unsafe { self.map_unchecked(|s| &s.inner) })
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        Manifold::shutdown(unsafe { self.map_unchecked_mut(|s| &mut s.inner) }, driver);
    }
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct PanicDispatcher<'d> {
    #[pin]
    #[manifold]
    listener: PanicIsolated<'d>,
}

type DropPendingListener<'d> = Listener<'d, 0, App<'d, DropPendingHandler>, Env>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct DropPendingDispatcher<'d> {
    #[pin]
    #[manifold]
    listener: DropLive<DropPendingListener<'d>>,
}

fn serve_panic_isolated(
    handler: PanicHandler,
    cfg: Config,
    context: WorkerContext,
    shutdown: &ShutdownTrigger,
) -> std::io::Result<()> {
    let listener_cfg = listener::Config::<Tcp> {
        max_connections: cfg.max_connections,
        bind: cfg.bind_addr,
        backlog: cfg.listen_backlog,
        stream: Default::default(),
        transport: Default::default(),
        egress: Default::default(),
    };
    let driver = dope::driver::Config::for_tcp_profile::<Throughput>(cfg.max_connections)
        .with_provided(cfg.receive_buffer_bytes, cfg.receive_buffer_count);
    Executor::with_seed(driver, context.seed())?
        .with_storage(handler)
        .enter(|mut session| {
            let handler = session.storage() as *const PanicHandler;
            let hash_builder = session.seed().derive(dope::hash::domain::ACCEPT).state();
            let listener = {
                let mut driver = session.driver_access();
                shutdown.try_register(&mut driver)?;
                Listener::<0, App<PanicHandler>, Env>::open_in(
                    // The handler is owned by this executor session and the
                    // dispatcher is dropped before the session returns.
                    App::new(unsafe { &*handler }, cfg),
                    listener_cfg,
                    hash_builder,
                    &mut driver,
                )?
            };
            let dispatcher = std::pin::pin!(Branded::new(PanicDispatcher {
                listener: PanicIsolated { inner: listener },
            }));
            session.run(dispatcher.as_ref())
        })
}

fn serve_drop_pending(
    handler: DropPendingHandler,
    cfg: Config,
    context: WorkerContext,
    shutdown: &ShutdownTrigger,
    completed: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let listener_cfg = listener::Config::<Tcp> {
        max_connections: cfg.max_connections,
        bind: cfg.bind_addr,
        backlog: cfg.listen_backlog,
        stream: Default::default(),
        transport: Default::default(),
        egress: Default::default(),
    };
    let driver = dope::driver::Config::for_tcp_profile::<Throughput>(cfg.max_connections)
        .with_provided(cfg.receive_buffer_bytes, cfg.receive_buffer_count);
    Executor::with_seed(driver, context.seed())?
        .with_storage(handler)
        .enter(|mut session| {
            let handler = session.storage() as *const DropPendingHandler;
            let hash_builder = session.seed().derive(dope::hash::domain::ACCEPT).state();
            let listener = {
                let mut driver = session.driver_access();
                shutdown.try_register(&mut driver)?;
                Listener::<0, App<DropPendingHandler>, Env>::open_in(
                    // The handler and listener share the enclosing session's
                    // lifetime; neither can escape this closure.
                    App::new(unsafe { &*handler }, cfg),
                    listener_cfg,
                    hash_builder,
                    &mut driver,
                )?
            };
            let result = {
                let dispatcher = std::pin::pin!(Branded::new(DropPendingDispatcher {
                    listener: DropLive { inner: listener },
                }));
                session.run(dispatcher.as_ref())
            };
            completed.store(true, Ordering::Release);
            result
        })
}

fn flush(stream: &mut TcpStream, client: &mut Conn<ClientRole>) {
    let out = client.outbound();
    if out.is_empty() {
        return;
    }
    let owned = out.to_vec();
    stream.write_all(&owned).expect("client write");
    client.drain_outbound(owned.len());
}

fn send_all(client: &mut Conn<ClientRole>, sid: StreamId, data: &[u8], end_stream: bool) {
    let mut off = 0;
    loop {
        let n = client
            .send_data(sid, &data[off..], end_stream)
            .expect("send_data");
        off += n;
        if off >= data.len() {
            break;
        }
    }
}

fn request_headers(path: &[u8]) -> [Header<'_>; 4] {
    [
        Header {
            name: b":method",
            value: b"POST",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: path,
        },
        Header {
            name: b":authority",
            value: b"localhost",
        },
    ]
}

fn round_trip(
    addr: SocketAddr,
    build: impl FnOnce(&mut Conn<ClientRole>) -> StreamId,
) -> (Vec<OwnedHeader>, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("read timeout");
    let mut client = Conn::<ClientRole>::new();
    let sid = build(&mut client);
    flush(&mut stream, &mut client);

    read_response(&mut stream, &mut client, sid)
}

fn read_response(
    stream: &mut TcpStream,
    client: &mut Conn<ClientRole>,
    sid: StreamId,
) -> (Vec<OwnedHeader>, Vec<u8>) {
    let mut headers = Vec::new();
    let mut body = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    'outer: loop {
        let n = match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        client.ingest(&buf[..n]).expect("client ingest");
        while let Some(ev) = client.poll_event() {
            match ev {
                conn::Event::Headers {
                    stream_id,
                    headers: h,
                    end_stream,
                    ..
                } if stream_id == sid => {
                    headers = h.to_owned();
                    if end_stream {
                        break 'outer;
                    }
                }
                conn::Event::Data {
                    stream_id,
                    data,
                    end_stream,
                } if stream_id == sid => {
                    body.extend_from_slice(&data);
                    if end_stream {
                        break 'outer;
                    }
                }
                _ => {}
            }
        }
        flush(stream, client);
    }
    (headers, body)
}

fn read_stream_reset(
    stream: &mut TcpStream,
    client: &mut Conn<ClientRole>,
    stream_id: StreamId,
) -> ErrorCode {
    let mut buffer = [0; 16 * 1024];
    loop {
        let read = stream.read(&mut buffer).expect("reset read");
        assert_ne!(read, 0, "connection closed before reset");
        client.ingest(&buffer[..read]).expect("reset ingest");
        while let Some(event) = client.poll_event() {
            if let conn::Event::StreamReset {
                stream_id: reset_stream_id,
                error,
            } = event
                && reset_stream_id == stream_id
            {
                return error;
            }
        }
        flush(stream, client);
    }
}

fn header_value<'a>(headers: &'a [OwnedHeader], name: &[u8]) -> Option<&'a [u8]> {
    headers
        .iter()
        .find(|h| h.name == name)
        .map(|h| h.value.as_slice())
}

fn wait_for_count(count: &AtomicUsize, expected: usize) {
    for _ in 0..200 {
        if count.load(Ordering::Acquire) >= expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(count.load(Ordering::Acquire), expected);
}

#[test]
fn bodied_request_delivers_headers_and_full_body() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let cfg = server_config(bind, 32);
    harness
        .run_with_trigger(
            move |ctx, trigger| serve(Echo, cfg, ctx, Some(trigger)),
            |addr| {
                let payload: Vec<u8> = (0..20_000).map(|i| (i % 251) as u8).collect();
                let expected = payload.clone();
                let (headers, body) = round_trip(addr, move |client| {
                    let sid = client
                        .start_request(&request_headers(b"/echo"), false)
                        .expect("start_request");
                    let mid = payload.len() / 2;
                    send_all(client, sid, &payload[..mid], false);
                    send_all(client, sid, &payload[mid..], true);
                    sid
                });

                assert_eq!(header_value(&headers, b":status"), Some(&b"200"[..]));
                assert_eq!(
                    header_value(&headers, b"x-method"),
                    Some(&b"POST"[..]),
                    "request header list must survive assembly"
                );
                assert_eq!(header_value(&headers, b"x-has-path"), Some(&b"1"[..]));
                assert_eq!(body, expected, "multi-frame body must arrive intact");
            },
        )
        .expect("harness");
}

#[test]
fn trailers_terminate_request() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let cfg = server_config(bind, 32);
    harness
        .run_with_trigger(
            move |ctx, trigger| serve(Echo, cfg, ctx, Some(trigger)),
            |addr| {
                let (headers, body) = round_trip(addr, |client| {
                    let sid = client
                        .start_request(&request_headers(b"/trailer"), false)
                        .expect("start_request");
                    send_all(client, sid, b"trailer-terminated-body", false);
                    client
                        .send_trailers(
                            sid,
                            &[Header {
                                name: b"x-checksum",
                                value: b"deadbeef",
                            }],
                        )
                        .expect("send_trailers");
                    sid
                });

                assert_eq!(header_value(&headers, b":status"), Some(&b"200"[..]));
                assert_eq!(header_value(&headers, b"x-method"), Some(&b"POST"[..]));
                assert_eq!(
                    body, b"trailer-terminated-body",
                    "a trailer-terminated request must be dispatched with its body"
                );
            },
        )
        .expect("harness");
}

#[test]
fn pending_handler_resumes_from_its_task_waker() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let cfg = server_config(bind, 32);
    harness
        .run_with_trigger(
            move |ctx, trigger| serve(YieldEcho, cfg, ctx, Some(trigger)),
            |addr| {
                let (headers, body) = round_trip(addr, |client| {
                    let stream_id = client
                        .start_request(&request_headers(b"/yield"), false)
                        .unwrap();
                    send_all(client, stream_id, b"yielded", true);
                    stream_id
                });
                assert_eq!(header_value(&headers, b":status"), Some(&b"200"[..]));
                assert_eq!(body, b"yielded");
            },
        )
        .expect("harness");
}

#[test]
fn configured_handler_task_limit_refuses_excess_streams() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let config = server_config(bind, 2);
    harness
        .run_with_trigger(
            move |ctx, trigger| serve(CapacityHandler, config, ctx, Some(trigger)),
            |addr| {
                let mut stream = TcpStream::connect(addr).expect("connect");
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("read timeout");
                let mut client = Conn::<ClientRole>::new();
                for _ in 0..2 {
                    client
                        .start_request(&request_headers(b"/hold"), true)
                        .expect("start held request");
                }
                let refused = client
                    .start_request(&request_headers(b"/hold"), true)
                    .expect("start refused request");
                flush(&mut stream, &mut client);
                assert_eq!(
                    read_stream_reset(&mut stream, &mut client, refused),
                    ErrorCode::RefusedStream
                );
            },
        )
        .expect("harness");
}

#[test]
fn configured_body_limit_applies_to_final_data_frame() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let mut config = server_config(bind, 2);
    config.max_request_body_bytes = 4;
    config.max_connection_body_bytes = 4;
    harness
        .run_with_trigger(
            move |ctx, trigger| serve(Echo, config, ctx, Some(trigger)),
            |addr| {
                let mut stream = TcpStream::connect(addr).expect("connect");
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("read timeout");
                let mut client = Conn::<ClientRole>::new();
                let stream_id = client
                    .start_request(&request_headers(b"/oversized"), false)
                    .expect("start request");
                send_all(&mut client, stream_id, b"12345", true);
                flush(&mut stream, &mut client);
                assert_eq!(
                    read_stream_reset(&mut stream, &mut client, stream_id),
                    ErrorCode::EnhanceYourCalm
                );
            },
        )
        .expect("harness");
}

#[test]
fn registered_poll_panics_reclaim_and_reuse_handler_slots() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let cfg = server_config(bind, 32);
    let attempts = cfg.max_handler_tasks + 8;
    let panics = Arc::new(AtomicUsize::new(0));
    let server_panics = Arc::clone(&panics);
    harness
        .run_with_trigger(
            move |ctx, trigger| {
                serve_panic_isolated(
                    PanicHandler {
                        panics: Arc::clone(&server_panics),
                        paired_ready: None,
                    },
                    cfg,
                    ctx,
                    trigger,
                )
            },
            |addr| {
                for expected in 1..=attempts {
                    let mut stream = TcpStream::connect(addr).expect("connect");
                    let mut client = Conn::<ClientRole>::new();
                    client
                        .start_request(&request_headers(b"/panic"), true)
                        .expect("start panic request");
                    flush(&mut stream, &mut client);
                    wait_for_count(&panics, expected);
                }

                let (headers, body) = round_trip(addr, |client| {
                    client
                        .start_request(&request_headers(b"/ready"), true)
                        .expect("start ready request")
                });
                assert_eq!(header_value(&headers, b":status"), Some(&b"200"[..]));
                assert_eq!(body, b"reused");
                assert_eq!(panics.load(Ordering::Acquire), attempts);
            },
        )
        .expect("harness");
}

#[test]
fn panic_requeues_unprocessed_ready_task_on_the_same_connection() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let cfg = server_config(bind, 32);
    let panics = Arc::new(AtomicUsize::new(0));
    let paired_ready = Arc::new(AtomicBool::new(false));
    let server_panics = Arc::clone(&panics);
    let server_ready = Arc::clone(&paired_ready);
    harness
        .run_with_trigger(
            move |ctx, trigger| {
                serve_panic_isolated(
                    PanicHandler {
                        panics: Arc::clone(&server_panics),
                        paired_ready: Some(Arc::clone(&server_ready)),
                    },
                    cfg,
                    ctx,
                    trigger,
                )
            },
            |addr| {
                let mut stream = TcpStream::connect(addr).expect("connect");
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("read timeout");
                let mut client = Conn::<ClientRole>::new();
                client
                    .start_request(&request_headers(b"/panic"), true)
                    .expect("start panic request");
                let stream_id = client
                    .start_request(&request_headers(b"/after"), true)
                    .expect("start after request");
                flush(&mut stream, &mut client);

                let (headers, body) = read_response(&mut stream, &mut client, stream_id);
                assert_eq!(header_value(&headers, b":status"), Some(&b"200"[..]));
                assert_eq!(body, b"after");
                assert_eq!(panics.load(Ordering::Acquire), 1);
                assert!(paired_ready.load(Ordering::Acquire));
            },
        )
        .expect("harness");
}

#[test]
fn repeated_self_wake_pending_does_not_monopolize_the_lane() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let cfg = server_config(bind, 32);
    let state = Arc::new(SelfWakeState {
        polls: AtomicUsize::new(0),
        stop: AtomicBool::new(false),
    });
    let server_state = Arc::clone(&state);
    harness
        .run_with_trigger(
            move |ctx, trigger| {
                serve(
                    SelfWakeHandler {
                        state: Arc::clone(&server_state),
                    },
                    cfg,
                    ctx,
                    Some(trigger),
                )
            },
            |addr| {
                let mut pending_stream = TcpStream::connect(addr).expect("pending connect");
                let mut pending_client = Conn::<ClientRole>::new();
                pending_client
                    .start_request(&request_headers(b"/pending"), true)
                    .expect("start pending request");
                flush(&mut pending_stream, &mut pending_client);
                wait_for_count(&state.polls, 1);

                let mut ready_stream = TcpStream::connect(addr).expect("ready connect");
                ready_stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .expect("ready timeout");
                let mut ready_client = Conn::<ClientRole>::new();
                let stream_id = ready_client
                    .start_request(&request_headers(b"/ready"), true)
                    .expect("start ready request");
                flush(&mut ready_stream, &mut ready_client);
                let response = read_response(&mut ready_stream, &mut ready_client, stream_id);
                state.stop.store(true, Ordering::Release);

                assert_eq!(header_value(&response.0, b":status"), Some(&b"200"[..]));
                assert_eq!(response.1, b"fair");
            },
        )
        .expect("harness");
}

#[test]
fn listener_drop_tears_down_pending_handler_tasks() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let cfg = server_config(bind, 32);
    let state = Arc::new(DropPendingState {
        started: AtomicUsize::new(0),
        dropped: AtomicUsize::new(0),
    });
    let completed = Arc::new(AtomicBool::new(false));
    let server_state = Arc::clone(&state);
    let server_completed = Arc::clone(&completed);
    let stream = harness
        .run_with_trigger(
            move |ctx, trigger| {
                serve_drop_pending(
                    DropPendingHandler {
                        state: Arc::clone(&server_state),
                    },
                    cfg,
                    ctx,
                    trigger,
                    Arc::clone(&server_completed),
                )
            },
            |addr| {
                let mut stream = TcpStream::connect(addr).expect("connect");
                let mut client = Conn::<ClientRole>::new();
                client
                    .start_request(&request_headers(b"/pending"), true)
                    .expect("start pending request");
                flush(&mut stream, &mut client);
                wait_for_count(&state.started, 1);
                stream
            },
        )
        .expect("harness");

    assert_eq!(state.dropped.load(Ordering::Acquire), 1);
    assert!(completed.load(Ordering::Acquire));
    drop(stream);
}

#[test]
fn connection_close_reclaims_its_pending_tasks() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let cfg = server_config(bind, 32);
    let max_handler_tasks = cfg.max_handler_tasks;
    harness
        .run_with_trigger(
            move |ctx, trigger| serve(CapacityHandler, cfg, ctx, Some(trigger)),
            |addr| {
                let mut stream = TcpStream::connect(addr).expect("connect");
                let mut client = Conn::<ClientRole>::new();
                for _ in 0..max_handler_tasks {
                    client
                        .start_request(&request_headers(b"/hold"), true)
                        .expect("start pending request");
                }
                flush(&mut stream, &mut client);
                std::thread::sleep(Duration::from_millis(100));
                drop(stream);
                std::thread::sleep(Duration::from_millis(100));

                let (headers, body) = round_trip(addr, |client| {
                    client
                        .start_request(&request_headers(b"/ready"), true)
                        .expect("start ready request")
                });
                assert_eq!(header_value(&headers, b":status"), Some(&b"200"[..]));
                assert_eq!(body, b"ready");
            },
        )
        .expect("harness");
}

#[test]
fn sync_handler_serves_static_and_reusable_bodies() {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let config = server_config(bind, 0);
    assert_eq!(config.bind_addr, bind);
    assert_eq!(config.max_connections, 64);
    assert_eq!(config.listen_backlog, 128);
    harness
        .run_with_trigger(
            move |ctx, trigger| {
                let large = Body::repeat(b'x', 1 << 20);
                serve_sync(
                    move |request| {
                        if request.path().is_some_and(|path| path == b"/large") {
                            Response::text(large.clone())
                        } else {
                            Response::text(b"hello from sync")
                        }
                    },
                    config,
                    ctx,
                    Some(trigger),
                )
            },
            |addr| {
                let (headers, body) = round_trip(addr, |client| {
                    client
                        .start_request(&request_headers(b"/"), true)
                        .expect("start static request")
                });
                assert_eq!(header_value(&headers, b":status"), Some(&b"200"[..]));
                assert_eq!(
                    header_value(&headers, b"content-type"),
                    Some(&b"text/plain; charset=utf-8"[..])
                );
                assert_eq!(body, b"hello from sync");

                for _ in 0..2 {
                    let (headers, body) = round_trip(addr, |client| {
                        client
                            .start_request(&request_headers(b"/large"), true)
                            .expect("start reusable request")
                    });
                    assert_eq!(header_value(&headers, b":status"), Some(&b"200"[..]));
                    assert_eq!(body.len(), 1 << 20);
                    assert!(body.iter().all(|byte| *byte == b'x'));
                }
            },
        )
        .expect("harness");
}
