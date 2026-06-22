use std::cell::RefCell;
use std::future::poll_fn;
use std::marker::PhantomData;
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use dope::fiber::Fiber;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::profile::Production;
use dope::runtime::token::Token;
use dope::transport::Tcp;
use dope::wire::Identity;
use dope::{DriverCfg, DriverConfig, Executor};
use dope_extra::testing::{ephemeral_addr, run_with_trigger};
use sark_ws::client::{self, Client};
use sark_ws::server;

#[derive(Default)]
struct Captured {
    conn_id: Option<Token>,
    reply: Option<String>,
}

#[derive(Clone)]
struct CaptureHandler {
    state: Rc<RefCell<Captured>>,
}

impl client::Handler for CaptureHandler {
    fn on_open(&mut self, conn_id: Token) {
        self.state.borrow_mut().conn_id = Some(conn_id);
    }

    fn on_message(&mut self, _conn_id: Token, msg: client::Message) {
        if let client::Message::Text(bytes) = msg
            && let Ok(s) = std::str::from_utf8(bytes.as_slice())
        {
            self.state.borrow_mut().reply = Some(s.to_string());
        }
    }

    fn on_close(&mut self, _conn_id: Token) {}
}

type ClientWs =
    Connector<0, client::Session<CaptureHandler>, Static<Tcp>, Bundle<Tcp, Identity, Production>>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct ConnRt<'d> {
    #[pin]
    #[manifold]
    conn: ClientWs,
    _ph: PhantomData<&'d ()>,
}

fn ws_pong(msg: server::Message<'_>, response: &mut server::Response<'_>) {
    if let server::Message::Text(t) = msg
        && t == "ping"
    {
        response.text("pong");
    }
}

#[test]
fn ws_codec_smoke() {
    let addr = ephemeral_addr();
    let cfg = server::Cfg {
        bind: addr,
        max_conn: 1024,
        backlog: 4096,
        path: "/ws",
        max_frame_payload: 16 * 1024 * 1024,
    };

    run_with_trigger(
        addr,
        |ctx, trigger| {
            type Handler = for<'a, 'b, 'c> fn(server::Message<'a>, &'b mut server::Response<'c>);
            sark_ws::server::serve(ws_pong as Handler, cfg.clone(), ctx, Some(trigger))
        },
        |addr| {
            let mut exec =
                Executor::new(DriverCfg::for_tcp_profile::<Production>(8)).expect("driver");
            let driver = exec.driver_mut();
            let state = Rc::new(RefCell::new(Captured::default()));
            let handler = CaptureHandler {
                state: state.clone(),
            };
            let upstreams = Static::<Tcp>::new(vec![addr], Duration::from_millis(200));
            let mut rt = pin!(ConnRt {
                conn: Connector::new(
                    client::Session::new(handler, "127.0.0.1", "/ws"),
                    upstreams,
                    1,
                    driver,
                ),
                _ph: PhantomData,
            });
            let client = rt.as_mut().conn_handle();

            let state_fut = state.clone();
            let reply = dope_extra::block_on(
                &mut exec,
                rt.as_mut(),
                Fiber::new(async move {
                    client.wait_active().await.expect("ws active");
                    client.send_text(b"ping").await.expect("send text");

                    poll_fn(|cx| {
                        if let Some(r) = state_fut.borrow().reply.clone() {
                            return Poll::Ready(r);
                        }
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    })
                    .await
                }),
            );

            assert_eq!(reply, "pong");
        },
    );
}
