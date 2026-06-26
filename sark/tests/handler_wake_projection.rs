#![cfg(target_os = "linux")]

use std::cell::Cell;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use dope::Driver;
use dope::manifold::Outcome;
use dope::manifold::listener::{Application, Aux, State};
use dope::transport::link::Slot;
use dope::transport::wire::{Identity, RecvChunk};
use dope_extra::testing::run_with_trigger;
use http::StatusCode;
use o3::buffer::Owned;
use sark::date::{DateHost, Stamp};
use sark::dispatch::H1Project;
use sark::dispatch::conn_state::ConnState;
use sark::timer::{Timer, TimerHost};
use sark::{Build, ServerCfg};

#[sark_gen::request]
struct EmptyReq {}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: Owned,
}

#[sark_gen::handler]
async fn sleep_handler(_req: EmptyReq, _state: &(), timer: sark::Timer) -> Reply {
    timer.sleep(Duration::from_millis(100)).await;
    let mut body = Owned::new();
    body.extend_from_slice(b"slept 100ms");
    Reply {
        status: StatusCode::OK,
        body,
    }
}

sark_gen::define_route! {
    SleepDispatch: () => {
        GET "/sleep" => async sleep_handler,
    }
}

#[allow(dead_code)]
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

struct Demux<A> {
    inner: A,
}

impl<'d, A> Application for Demux<A>
where
    A: Application<Conn = ConnState, Wire = Identity>
        + DateHost
        + TimerHost<'d>
        + H1Project<Identity>,
{
    type Conn = Wrap;
    type Wire = Identity;

    fn on_chunk(
        &mut self,
        slot: &mut Slot<Self::Wire, State<Self::Conn>>,
        chunk: RecvChunk<'_>,
        aux: &mut Aux,
        driver: &mut Driver,
    ) -> Outcome {
        let bytes = chunk.as_slice();
        if self.inner.project_on_chunk(slot, bytes, aux, driver, proj) {
            Outcome::Overrun
        } else {
            Outcome::Ok
        }
    }

    fn on_send(
        &mut self,
        slot: &mut Slot<Self::Wire, State<Self::Conn>>,
        sent: usize,
        aux: &mut Aux,
        driver: &mut Driver,
    ) {
        self.inner.project_on_send(slot, proj, sent, aux, driver);
    }

    fn on_wake(
        &mut self,
        slot: &mut Slot<Self::Wire, State<Self::Conn>>,
        aux: &mut Aux,
        driver: &mut Driver,
    ) {
        self.inner.project_on_wake(slot, proj, aux, driver);
    }

    fn on_close(&mut self, slot: &mut Slot<Self::Wire, State<Self::Conn>>, aux: &mut Aux) {
        self.inner.project_on_close(slot, proj, aux);
    }
}

impl<A: DateHost> DateHost for Demux<A> {
    fn date_stamp(&self) -> &Stamp {
        self.inner.date_stamp()
    }
}

impl<'d, A: TimerHost<'d>> TimerHost<'d> for Demux<A> {
    fn timer_cell(&self) -> &Cell<Option<Timer<'d>>> {
        self.inner.timer_cell()
    }
}

#[test]
fn async_route_resumes_through_non_identity_projection() {
    let bind: std::net::SocketAddr = "127.0.0.1:18895".parse().unwrap();
    let cfg = ServerCfg {
        bind,
        max_conn: 16,
        backlog: 16,
        head_timeout: std::time::Duration::from_secs(10),
    };

    run_with_trigger(
        bind,
        |ctx, trigger| {
            Build::http(
                Demux {
                    inner: sleep_dispatch::new::<Identity>(&()),
                },
                cfg.clone(),
                ctx,
                Some(trigger),
            )
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
    );
}
