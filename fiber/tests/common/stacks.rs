use std::{cell::Cell, convert::Infallible, fmt, time::Duration};

use dope::{
    core::{
        BranchError,
        driver::{
            life::recovery,
            route::{Identity, KeyTag},
            schedule::clock::timer::Timer,
            settings::errors::Claim,
        },
        io::socket::Listener,
    },
    manifold::{self, network::listener::conn},
    runtime::{
        executor,
        topology::{self, error},
    },
};
use dope_fiber::task::{
    storage::scope::Brand,
    time::sleep::{self, TimerExt as _},
};
use dope_test::harness::{observe::VecCellExt as _, signals::Progress};
use sark_core::{
    body, deadline,
    life::{self, audit},
};
use sark_fiber::{
    Config,
    dope::net::{wire, wire::domain},
};
use sark_h1::server::{listener::bind, run, run::prepare};
use sark_http::payload::response::status;

use crate::common::{retained::NestedReq, server};

pub struct State {
    pub challenge: Vec<u8>,
    pub admitted: Cell<bool>,
    pub invalid: Cell<bool>,
    pub sleep: Cell<Option<sleep::Error>>,
}

#[sark_fiber::request]
pub struct Challenge {
    #[header("x-proof-challenge", default = "missing")]
    pub challenge: storage::Shared,
}

#[sark_fiber::response(raw)]
pub struct Reply {
    pub status: status::Code,
    pub body: Vec<u8>,
}

#[sark_fiber::handler(pub)]
pub(crate) fn complete(request: Challenge, state: &State) -> Reply {
    if request.challenge != state.challenge {
        state.invalid.set(true);
    }
    state.admitted.set(true);
    Reply {
        status: status::Code::OK,
        body: request.challenge.to_vec(),
    }
}

#[sark_fiber::handler(pub)]
pub(crate) async fn cancel(request: Challenge, state: &State, timer: Timer<'_>) -> Reply {
    if request.challenge.as_slice() != state.challenge {
        state.invalid.set(true);
    }
    state.admitted.set(true);
    match timer.sleep(Duration::from_secs(30)) {
        Ok(sleep) => sleep.await,
        Err(error) => state.sleep.set(Some(error)),
    }
    Reply {
        status: status::Code::OK,
        body: b"unexpected completion".to_vec(),
    }
}

#[sark_fiber::handler(pub)]
async fn invalid_json(_: NestedReq, state: &State) -> Reply {
    state.invalid.set(true);
    Reply {
        status: status::Code::INTERNAL_SERVER_ERROR,
        body: Vec::new(),
    }
}

sark_fiber::define_route! {
    pub Routes: State => {
        GET "/complete" => request(header) complete,
        GET "/cancel" => request(header) async(capacity = 1, body = discarded) cancel,
        POST "/invalid-json" => async(capacity = 1, body = json) invalid_json,
    }
}

pub fn observed<C, O, R>(
    plan: run::Plan<
        KeyTag<{ server::HTTP_LISTENER_ID }>,
        KeyTag<{ server::DATE_UPDATER_ID }>,
        sark_fiber::Throughput,
    >,
    context: C,
    listener: Listener,
    state: &State,
    probe: O,
    policy: impl recovery::Observe,
    finish: impl for<'a> FnOnce(
        Result<(), error::Run<'a, Infallible, manifold::BuildError<bind::Error>>>,
    ) -> R,
) -> Result<R, BranchError<prepare::Error, Claim>>
where
    C: executor::Build,
    for<'d> O: life::Observer<conn::Id<'d, KeyTag<{ server::HTTP_LISTENER_ID }>>, u64>,
{
    use executor::session;

    let prepared = plan.with_bound(listener).prepare(())?;
    let mut report = session::Completion::new();
    let complete = &mut report;
    let result = prepared.enter_with(
        context,
        move |scope| {
            scope.lend(|plan, stage| {
                Brand::with(|brand| {
                    let app = Routes::with_observer(
                        brand,
                        state,
                        Config::new(server::MAX_CONNECTIONS),
                        probe,
                    );
                    plan.install(
                        stage,
                        domain::Local::<wire::Identity, _>::new(wire::Identity),
                        app,
                        |root, _wait, stage| {
                            let result = stage
                                .run_quiesce(complete, root, Duration::from_secs(2))
                                .map_err(topology::Failure::Run)?;
                            result.map_err(|error| {
                                topology::Failure::Run(session::Error::Drive(error))
                            })
                        },
                    )
                    .map_err(topology::Failure::Install)?
                })
            })
        },
        policy,
        |result| {
            let result = match result {
                Ok(result) => result.map_err(topology::RunError::Failure),
                Err(error) => Err(topology::RunError::from(error)),
            };
            finish(result.as_ref().copied().map_err(topology::RunError::cause))
        },
    );
    Ok(result)
}

/// Nested materialization needs more than finish_body's budget to reach
/// the wrong-typed tags sequence, independently of transport segmentation.
/// The caller waits for 100 Continue before sending the body to install Pending.
pub fn invalid_request() -> (String, Vec<u8>) {
    let mut body = br#"{"items":[{"#.to_vec();
    body.resize(body.len() + 2 * body::DEFAULT_BUILD_STEP_BUDGET.get(), b' ');
    body.extend_from_slice(br#""value":"ok","tags":false}]}"#);
    let head = format!(
        "POST /invalid-json HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nExpect: 100-continue\r\n\r\n",
        body.len(),
    );
    (head, body)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    Opened,
    Established,
    Exchange,
    Admitted,
    Input,
    Pending,
    Deadline(deadline::Phase),
    Response,
    Completed,
    Cancelled,
    Rejected,
    Retired,
    Closed,
}

#[derive(Clone, Copy)]
pub struct Probe<'a, D>(
    pub &'a Cell<Vec<Step>>,
    pub &'a Cell<Option<audit::Snapshot>>,
    pub D,
);

pub struct Count<'a> {
    pub events: &'a Cell<Vec<Step>>,
    pub step: Step,
}

impl Progress for Count<'_> {
    fn hits(&self) -> u32 {
        let events = self.events.take();
        let count = events.iter().filter(|step| **step == self.step).count() as u32;
        self.events.set(events);
        count
    }
}

impl<'d, ID: Identity, D: Fn(&dyn fmt::Debug)> life::Observer<conn::Id<'d, ID>, u64>
    for Probe<'_, D>
{
    fn observe(&mut self, event: life::Event<conn::Id<'d, ID>, u64>) {
        use sark_core::life::{Event, Outcome};
        let step = match event {
            Event::SessionOpened(_) => Step::Opened,
            Event::SessionEstablished(_) => Step::Established,
            Event::ExchangeOpened(_) => Step::Exchange,
            Event::HeadAdmitted(_) => Step::Admitted,
            Event::InputFinished(_) => Step::Input,
            Event::TaskPollPending(_) => Step::Pending,
            Event::ExchangeDeadlineExpired { phase, .. } => Step::Deadline(phase),
            Event::ResponseCommitted { .. } => Step::Response,
            Event::Terminal {
                outcome: Outcome::Completed,
                ..
            } => Step::Completed,
            Event::Terminal {
                outcome: Outcome::Cancelled,
                ..
            } => Step::Cancelled,
            Event::Terminal {
                outcome: Outcome::Rejected,
                ..
            } => Step::Rejected,
            Event::ExchangeRetired(_) => Step::Retired,
            Event::SessionClosed(_) => Step::Closed,
            _ => return,
        };
        self.0.push(step);
    }

    fn diagnostic<E: fmt::Debug>(&mut self, cause: &E) {
        (self.2)(cause);
    }

    fn audit(&mut self, _: conn::Id<'d, ID>, state: audit::Snapshot) {
        use sark_core::life::audit::Capability;
        if !state.is_zero(Capability::TASK_COMPLETION_SLOTS) && self.1.get().is_none() {
            self.1.set(Some(state));
        }
    }
}
