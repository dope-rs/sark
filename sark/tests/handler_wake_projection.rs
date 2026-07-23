#![cfg(target_os = "linux")]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use dope::DriverContext;
use dope::manifold::Outcome;
use dope::manifold::listener::{Application, Aux, State};
use dope_extra::harness::Harness;
use dope_net::link::slot::Slot;
use dope_net::wire::identity::Identity;
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

    fn chunk<R: RetainBytes>(
        self: std::pin::Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        chunk: R,
        aux: &mut Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Outcome {
        let bytes = chunk.as_slice();
        if self
            .project()
            .inner
            .chunk_proj(slot, bytes, aux, driver, proj)
        {
            Outcome::Overrun
        } else {
            Outcome::Ok
        }
    }

    fn send(
        self: std::pin::Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        sent: usize,
        aux: &mut Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.project()
            .inner
            .send_proj(slot, proj, sent, aux, driver);
    }

    fn activate(
        self: std::pin::Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        aux: &mut Aux,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        self.project().inner.activate_proj(slot, proj, aux, driver);
    }

    fn close(
        self: std::pin::Pin<&mut Self>,
        slot: &mut Slot<'d, Self::Wire, State<Self::Conn>>,
        aux: &mut Aux,
    ) {
        self.project().inner.close_proj(slot, proj, aux);
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
                let executor = Executor::new(driver_config)?;
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
