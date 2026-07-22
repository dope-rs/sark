use std::cell::RefCell;
use std::marker::PhantomData;
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use dope::driver;
use dope::driver::token::Token;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::runtime::Executor;
use dope::runtime::profile::Balanced;
use dope_extra::harness::Harness;
use dope_fiber::{SessionExt as _, poll_fn};
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use sark_ws::client::{self, Client, Config};
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
    fn open(&mut self, conn_id: Token) {
        self.state.borrow_mut().conn_id = Some(conn_id);
    }

    fn message(&mut self, _conn_id: Token, msg: client::Message) {
        if let client::Message::Text(bytes) = msg
            && let Ok(s) = std::str::from_utf8(bytes.as_slice())
        {
            self.state.borrow_mut().reply = Some(s.to_string());
        }
    }

    fn close<'d>(&mut self, _conn_id: Token) {}
}

type ClientWs<'d> = Connector<
    'd,
    0,
    client::Session<'d, CaptureHandler>,
    Static<Tcp>,
    Bundle<Tcp, Identity, Balanced>,
>;

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct ConnRt<'d> {
    #[pin]
    #[manifold]
    conn: ClientWs<'d>,
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
fn ws_fragmented_message_reassembles() {
    let harness = Harness::bind().expect("harness");
    let addr = harness.addr();
    let cfg = server::Config {
        bind: addr,
        max_connections: 1024,
        backlog: 4096,
        path: "/ws",
        max_frame_payload: 16 * 1024 * 1024,
    };

    harness
        .run_with_trigger(
            |ctx, trigger| {
                type Handler =
                    for<'a, 'b, 'c> fn(server::Message<'a>, &'b mut server::Response<'c>);
                sark_ws::server::serve(ws_pong as Handler, cfg.clone(), ctx, Some(trigger))
            },
            |addr| {
                let client_cfg = Config::new("127.0.0.1", "/ws").max_outbound_frame_payload(2);
                let exec = Executor::new(driver::Config::for_tcp_profile::<Balanced>(8))
                    .expect("driver")
                    .with_storage_factory(client::Port::factory(client_cfg, 1, 16));
                exec.enter(|mut sess| {
                    let backoff = sess.seed().derive(dope::hash::domain::BACKOFF).state();
                    let port = sess.storage() as *const client::Port<'_>;
                    let mut driver = sess.driver_access();
                    // The port is executor-owned and is dropped after the
                    // connector/dispatcher at the end of this closure.
                    let port = unsafe { &*port };
                    let state = Rc::new(RefCell::new(Captured::default()));
                    let handler = CaptureHandler {
                        state: state.clone(),
                    };
                    let upstreams =
                        Static::<Tcp>::new(vec![addr], Duration::from_millis(200), backoff);
                    let conn = {
                        Connector::new(
                            client::Session::new(handler, port),
                            upstreams,
                            port.capacity(),
                            &mut driver,
                        )
                        .expect("connector")
                    };
                    let rt = pin!(o3::cell::BrandCell::new(ConnRt {
                        conn,
                        _ph: PhantomData,
                    }));
                    let client = client::WsHandle::from_cell(ConnRt::conn_ref(
                        rt.as_ref().borrow_pin(sess.token()),
                    ));

                    let state_fut = state.clone();
                    sess.block_on(rt.as_ref(), client.wait_active())
                        .expect("runtime")
                        .expect("ws active");
                    sess.block_on(rt.as_ref(), client.send_text(b"ping"))
                        .expect("runtime")
                        .expect("send fragmented text");
                    let reply = sess
                        .block_on(
                            rt.as_ref(),
                            poll_fn(|cx| {
                                if let Some(r) = state_fut.borrow().reply.clone() {
                                    return Poll::Ready(r);
                                }
                                cx.wake();
                                Poll::Pending
                            }),
                        )
                        .expect("runtime");

                    assert_eq!(reply, "pong");
                })
            },
        )
        .expect("harness");
}
