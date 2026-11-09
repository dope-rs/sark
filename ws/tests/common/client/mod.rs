pub mod topology;
use std::{cell, net, task};

use dope::{core::driver::life::recovery, runtime::executor::drive};
use dope_fiber::abi;
use dope_test::fibers;
use sark_ws::{
    client,
    client::{channel, headers, mode},
};

enum Failure {
    Runtime(drive::Error),
    Guard(fibers::Run),
    Wait(topology::WaitError),
    Client(client::Error),
    Deadline,
}

/// Events captured from one installed WebSocket client.
#[derive(Default)]
pub struct Observation {
    opened: bool,
    reply: Option<String>,
    close: Option<client::Closed>,
}

impl Observation {
    /// Whether the client admitted the peer's upgrade response.
    #[must_use]
    pub const fn is_open(&self) -> bool {
        self.opened
    }

    /// Last text message observed by the client.
    #[must_use]
    pub fn reply(&self) -> Option<&str> {
        self.reply.as_deref()
    }

    /// Terminal reason observed by the client.
    #[must_use]
    pub fn close(&self) -> Option<&client::CloseReason> {
        self.close.as_ref().map(|closed| &closed.reason)
    }
}

/// Configures and runs an isolated Fiber WebSocket client probe.
pub struct Probe {
    address: net::SocketAddr,
    config: client::Config,
    added_headers: Vec<headers::Field>,
    remove_headers: Vec<String>,
}

impl Probe {
    /// Creates a probe for one already-listening peer.
    #[must_use]
    pub fn new(addr: net::SocketAddr, config: client::Config) -> Self {
        Self {
            address: addr,
            config,
            added_headers: Vec::new(),
            remove_headers: Vec::new(),
        }
    }

    /// Adds handler-owned fields to the opening handshake.
    pub fn with_headers(mut self, fields: Vec<(String, String)>) -> Result<Self, client::Error> {
        self.added_headers = fields
            .into_iter()
            .map(|(name, value)| headers::Field::new(name, value))
            .collect::<Result<_, _>>()?;
        Ok(self)
    }

    /// Removes configured fields from the opening handshake.
    #[must_use]
    pub fn omit(mut self, names: Vec<String>) -> Self {
        self.remove_headers = names;
        self
    }

    pub fn run(self, goal: Goal<'_>, policy: impl recovery::Observe) -> Observation {
        let state = cell::Cell::new(Observation::default());
        let hook = Capture {
            state: &state,
            added_headers: self.added_headers,
            remove_headers: self.remove_headers,
        };
        let result = topology::Plan::new(self.address, self.config, 1, 16).enter(
            |domain| {
                domain.run(channel::App::<_, mode::Observe>::new(hook), |mut app| {
                    let closing = matches!(goal, Goal::Close);
                    if !closing {
                        app.wait()
                            .map_err(Failure::Wait)?
                            .as_ref()
                            .map_err(|cause| cause.cause())
                            .expect("WebSocket opening");
                    }
                    match goal {
                        Goal::Open => return Ok(state.take()),
                        Goal::Echo(data) => {
                            let handle = app.client();
                            app.drive(handle.send_text(data))
                                .map_err(Failure::Runtime)?
                                .map_err(Failure::Client)?;
                        }
                        Goal::Message | Goal::Close => {}
                    }
                    let seen = app
                        .drive_within(abi::poll_fn(|cx| {
                            let observed = state.take();
                            let ready = if closing {
                                observed.close.is_some()
                            } else {
                                observed.reply.is_some()
                            };
                            state.set(observed);
                            if ready {
                                return task::Poll::Ready(());
                            }
                            cx.wake();
                            task::Poll::Pending
                        }))
                        .map_err(Failure::Guard)?;
                    seen.map(|()| state.take()).ok_or(Failure::Deadline)
                })
            },
            policy,
            |result| match result.expect("WebSocket execution") {
                Ok(observed) => observed,
                Err(Failure::Runtime(cause)) => panic!("WebSocket probe: Runtime({cause:?})"),
                Err(Failure::Guard(cause)) => panic!("WebSocket probe: Guard({cause:?})"),
                Err(Failure::Wait(cause)) => panic!("WebSocket probe: Wait({cause:?})"),
                Err(Failure::Client(cause)) => panic!("WebSocket probe: Client({cause:?})"),
                Err(Failure::Deadline) => panic!("WebSocket probe: Deadline"),
            },
        );
        result.expect("WebSocket topology")
    }
}

pub enum Goal<'a> {
    Echo(&'a str),
    Open,
    Message,
    Close,
}

struct Capture<'a> {
    state: &'a cell::Cell<Observation>,
    added_headers: Vec<headers::Field>,
    remove_headers: Vec<String>,
}

impl client::Handshake for Capture<'_> {
    fn handshake(&mut self, req: headers::Request<'_>) -> Result<headers::Encoded, client::Error> {
        let edits = headers::Changes::new(&self.added_headers, &self.remove_headers);
        req.finish_edits(edits)
    }
}

impl client::Observer for Capture<'_> {
    fn open(&mut self) {
        let mut observed = self.state.take();
        observed.opened = true;
        self.state.set(observed);
    }

    fn message(&mut self, msg: client::Message<'_>) {
        if let client::Message::Text(text) = msg {
            let reply = text.to_owned();
            let mut observed = self.state.take();
            observed.reply = Some(reply);
            self.state.set(observed);
        }
    }

    fn close(&mut self, closed: client::Closed) {
        let mut observed = self.state.take();
        observed.close = Some(closed);
        self.state.set(observed);
    }
}
