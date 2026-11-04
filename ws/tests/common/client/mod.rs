pub mod topology;
use std::{cell, net, rc, task};

use dope::{core::driver::life::recovery, runtime::executor::drive};
use dope_fiber::{abi, wait};
use dope_test::fibers;
use sark_ws::{
    client,
    client::{channel, headers, mode},
};

/// Exact failure borrowed while its original execution owner remains alive.
#[derive(Debug)]
pub enum Error<'a> {
    Topology(topology::Run<'a>),
    Runtime(&'a drive::Error),
    Guard(&'a fibers::Run),
    Wait(&'a topology::WaitError),
    Client(&'a client::Error),
    Deadline,
}

enum Failure {
    Runtime(drive::Error),
    Guard(fibers::Run),
    Wait(topology::WaitError),
    Client(client::Error),
    Deadline,
}

impl Failure {
    fn kind(&self) -> Error<'_> {
        match self {
            Self::Runtime(cause) => Error::Runtime(cause),
            Self::Guard(cause) => Error::Guard(cause),
            Self::Wait(cause) => Error::Wait(cause),
            Self::Client(cause) => Error::Client(cause),
            Self::Deadline => Error::Deadline,
        }
    }
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

    /// Opens the client, sends text, and waits for a text response.
    pub fn echo<Out>(
        self,
        data: &str,
        check: impl for<'d> FnOnce(Result<(), client::Active<wait::Error<'d>>>),
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<Observation, Error<'a>>) -> Out,
    ) -> Result<Out, topology::Error> {
        self.run(Goal::Echo(data), check, policy, done)
    }

    /// Waits until the peer's opening response is admitted.
    pub fn open<Out>(
        self,
        check: impl for<'d> FnOnce(Result<(), client::Active<wait::Error<'d>>>),
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<Observation, Error<'a>>) -> Out,
    ) -> Result<Out, topology::Error> {
        self.run(Goal::Open, check, policy, done)
    }

    /// Waits for the first text message from the peer.
    pub fn message<Out>(
        self,
        check: impl for<'d> FnOnce(Result<(), client::Active<wait::Error<'d>>>),
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<Observation, Error<'a>>) -> Out,
    ) -> Result<Out, topology::Error> {
        self.run(Goal::Message, check, policy, done)
    }

    /// Waits for the client to terminate the connection.
    pub fn close<Out>(
        self,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<Observation, Error<'a>>) -> Out,
    ) -> Result<Out, topology::Error> {
        self.run(Goal::Close, |_| {}, policy, done)
    }

    fn run<Out>(
        self,
        goal: Goal<'_>,
        check: impl for<'d> FnOnce(Result<(), client::Active<wait::Error<'d>>>),
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<Observation, Error<'a>>) -> Out,
    ) -> Result<Out, topology::Error> {
        let state = rc::Rc::new(cell::Cell::new(Observation::default()));
        let hook = Capture {
            state: rc::Rc::clone(&state),
            added_headers: self.added_headers,
            remove_headers: self.remove_headers,
        };
        topology::Plan::new(self.address, self.config, 1, 16).enter(
            move |domain| {
                domain.run(channel::App::<_, mode::Observe>::new(hook), |mut app| {
                    let closing = matches!(goal, Goal::Close);
                    if !closing {
                        app.wait(check).map_err(Failure::Wait)?;
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
            |result| match result {
                Ok(Ok(value)) => done(Ok(value)),
                Ok(Err(error)) => done(Err(error.kind())),
                Err(error) => done(Err(Error::Topology(error))),
            },
        )
    }
}

enum Goal<'a> {
    Echo(&'a str),
    Open,
    Message,
    Close,
}

struct Capture {
    state: rc::Rc<cell::Cell<Observation>>,
    added_headers: Vec<headers::Field>,
    remove_headers: Vec<String>,
}

impl client::Handshake for Capture {
    fn handshake(&mut self, req: headers::Request<'_>) -> Result<headers::Encoded, client::Error> {
        let edits = headers::Changes::new(&self.added_headers, &self.remove_headers);
        req.finish_edits(edits)
    }
}

impl client::Observer for Capture {
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
