#![cfg(target_os = "linux")]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use dope::DriverContext;
use dope::manifold::Outcome;
use dope::manifold::listener::{
    application::{Application, ApplicationHooks},
    state::{EgressCtx, State},
};
use dope_net::link::slot::Slot;
use dope_net::wire::identity::Identity;
use dope_test::Harness;
use http::StatusCode;
use o3::buffer::RetainBytes;
use sark::date::{DateHost, Stamp};
use sark::dispatch::H1Project;
use sark::dispatch::conn_state::ConnState;
use sark::timer::{Timer, TimerHost};
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

#[allow(dead_code, clippy::large_enum_variant)]
enum Wrap {
    Pad(u32),
    H1(ConnState),
}

impl Default for Wrap {
    fn default() -> Self {
        Wrap::H1(ConnState::default())
    }
}

fn proj(w: &mut Wrap) -> &mut ConnState {
    match w {
        Wrap::H1(c) => c,
        Wrap::Pad(_) => unreachable!(),
    }
}

#[pin_project::pin_project]
struct Demux<A> {
    #[pin]
    inner: A,
}

impl<'d, A> Application<'d> for Demux<A>
where
    A: Application<'d, Conn = ConnState, Wire = Identity>
        + DateHost
        + TimerHost<'d>
        + H1Project<'d, Identity>,
{
    type Conn = Wrap;
    type Wire = Identity;
    type Hooks = Self;
}

impl<'d, A> ApplicationHooks<'d, Demux<A>> for Demux<A>
where
    A: Application<'d, Conn = ConnState, Wire = Identity>
        + DateHost
        + TimerHost<'d>
        + H1Project<'d, Identity>,
{
    fn chunk<R: RetainBytes>(
        app: std::pin::Pin<&mut Demux<A>>,
        slot: &mut Slot<'d, Identity, State<Wrap>>,
        mut egress: EgressCtx<'_, '_>,
        chunk: R,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let bytes = chunk.as_slice();
        if app
            .project()
            .inner
            .chunk_proj(slot, bytes, &mut egress, driver, proj)
        {
            Outcome::Overrun
        } else {
            Outcome::Ok
        }
    }

    fn send(
        app: std::pin::Pin<&mut Demux<A>>,
        slot: &mut Slot<'d, Identity, State<Wrap>>,
        mut egress: EgressCtx<'_, '_>,
        sent: usize,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        app.project()
            .inner
            .send_proj(slot, proj, sent, &mut egress, driver);
    }

    fn activate(
        app: std::pin::Pin<&mut Demux<A>>,
        slot: &mut Slot<'d, Identity, State<Wrap>>,
        mut egress: EgressCtx<'_, '_>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        app.project()
            .inner
            .activate_proj(slot, proj, &mut egress, driver);
    }

    fn close(
        app: std::pin::Pin<&mut Demux<A>>,
        slot: &mut Slot<'d, Identity, State<Wrap>>,
        mut egress: EgressCtx<'_, '_>,
    ) {
        app.project().inner.close_proj(slot, proj, &mut egress);
    }
}

impl<A: DateHost> DateHost for Demux<A> {
    fn stamp(self: std::pin::Pin<&Self>) -> std::pin::Pin<&Stamp> {
        self.project_ref().inner.stamp()
    }
}

impl<'d, A: TimerHost<'d>> TimerHost<'d> for Demux<A> {
    fn timer(&self) -> &Timer<'d> {
        self.inner.timer()
    }
}

#[test]
fn async_route_resumes_through_non_identity_projection() {
    let bind: std::net::SocketAddr = "127.0.0.1:18895".parse().unwrap();
    let server = support::http_server(bind, Duration::from_secs(10));

    Harness::new(bind)
        .run_with_trigger(
            |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?
                    .with_storage(dope_net::link::egress::storage::Storage::default());
                executor.enter(|mut session| {
                    let timer = sark::Timer::with_capacity(32);
                    server.clone().serve(
                        &mut session,
                        Demux {
                            inner: SleepDispatch::new::<Identity>(
                                &(),
                                &timer,
                                sark::app::Config {
                                    timer_capacity: 32,
                                    task_capacity: support::MAX_CONNECTIONS,
                                },
                            ),
                        },
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
                let _ = matches!(Wrap::Pad(0), Wrap::Pad(_));
            },
        )
        .expect("harness");
}
