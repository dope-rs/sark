use std::{marker, ops, pin};

use conn::write::{self, body};
use data::compose;
use dope::{
    core::driver::{retained, route::Identity},
    manifold::network::listener::conn,
    net::{link::egress::data, wire},
};
use wire::receive::discard;

use crate::{
    pump,
    server::{
        conn::router,
        date, deadline,
        payload::{
            self,
            write::{CycleCommit, Failure, Response, Wake},
        },
    },
    session, switch,
};

type Context<'a, 'b, 'c, 'e, 'f, 'd, ID, W, R, H, T> = conn::Ctx<
    'a,
    'b,
    'c,
    'e,
    'f,
    'd,
    ID,
    W,
    session::Conn<
        'd,
        R,
        <H as router::Routes<'d, R, ID, T>>::Pending,
        T,
        <H as router::Routes<'d, R, ID, T>>::Ready,
        <H as router::Routes<'d, R, ID, T>>::Head,
    >,
    data::Bound<
        compose::Either<
            payload::Body<'d, <H as router::Routes<'d, R, ID, T>>::TransportTag>,
            payload::Stream<'d, <H as router::Routes<'d, R, ID, T>>::TransportTag>,
        >,
        <H as router::Routes<'d, R, ID, T>>::Egress,
    >,
>;

pub struct Driver<'a, 'd, H> {
    app: pin::Pin<&'a mut H>,
    date: &'a date::Clock,
    driver: marker::PhantomData<fn(&'d ()) -> &'d ()>,
}

/// One HTTP/1 turn's direct task-plane continuation proof.
#[must_use = "an HTTP/1 drive must settle its continuation"]
pub struct Drive<H> {
    flow: Flow,
    tail: pump::Tail<H>,
    progress: deadline::Progress,
}

/// One settled drive that supplied no input to the pump.
///
/// `pump::Tail::Handoff` is constructed only for a non-empty callback-borrowed
/// suffix, so the absent field proves no call site can receive one.
#[must_use = "an HTTP/1 drive must settle its continuation"]
pub struct Settled {
    flow: Flow,
    progress: deadline::Progress,
}

/// Terminal disposition of one consumed drive capability.
pub enum Flow {
    Continue,
    Closed,
}

impl Flow {
    /// Applies one task-activity observation to this flow.
    fn woken(self, activity: pump::Activity, wake: impl FnOnce()) -> Self {
        match (self, activity) {
            (Self::Continue, pump::Activity::Idle) => Self::Continue,
            (Self::Continue, pump::Activity::Task | pump::Activity::Stream) => {
                wake();
                Self::Continue
            }
            (Self::Closed, _) => Self::Closed,
        }
    }
}

/// Exact result of one HTTP/1 input callback and its deadline mutation.
pub enum Ingress<H = ()> {
    Continue,
    Handoff { staged: H, range: ops::Range<usize> },
    CloseAfter,
}

impl<H> Drive<H> {
    const fn new(flow: Flow) -> Self {
        Self {
            flow,
            tail: pump::Tail::Empty,
            progress: deadline::Progress::new(deadline::Head::Unchanged, deadline::Body::None),
        }
    }

    fn with_tail(mut self, tail: pump::Tail<H>) -> Self {
        self.tail = tail;
        self
    }

    fn with_input(mut self, progress: deadline::Progress) -> Self {
        self.progress = progress;
        self
    }

    pub fn settle(
        self,
        activity: pump::Activity,
        wake: impl FnOnce(),
    ) -> (Flow, pump::Tail<H>, deadline::Progress) {
        let Self {
            flow,
            tail,
            progress,
        } = self;
        (flow.woken(activity, wake), tail, progress)
    }

    pub fn into_parts(self) -> (Flow, pump::Tail<H>, deadline::Progress) {
        let Self {
            flow,
            tail,
            progress,
        } = self;
        (flow, tail, progress)
    }
}

impl Settled {
    const fn new(flow: Flow) -> Self {
        Self {
            flow,
            progress: deadline::Progress::new(deadline::Head::Unchanged, deadline::Body::None),
        }
    }

    /// Applies one task-activity observation to this settled drive.
    pub fn settle(
        self,
        activity: pump::Activity,
        wake: impl FnOnce(),
    ) -> (Flow, deadline::Progress) {
        let Self { flow, progress } = self;
        (flow.woken(activity, wake), progress)
    }

    pub fn into_parts(self) -> (Flow, deadline::Progress) {
        (self.flow, self.progress)
    }
}

impl<'a, 'd, H> Driver<'a, 'd, H> {
    pub fn new(app: pin::Pin<&'a mut H>, date: &'a date::Clock) -> Self {
        Self {
            app,
            date,
            driver: marker::PhantomData,
        }
    }

    /// Drives one turn that supplies no input to the pump.
    pub fn resume<Runtime, Transitions, ID: Identity, W>(
        &mut self,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, W, Runtime, H, Transitions>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Settled
    where
        H: router::Routes<'d, Runtime, ID, Transitions>,
        Transitions: switch::Engine,
        Runtime: pump::Tasks,
        W: wire::Spec,
    {
        // Empty input cannot construct `pump::Tail::Handoff`, so no borrowed suffix is kept.
        let Drive { flow, progress, .. } = self.run(&[], conn, driver);
        Settled { flow, progress }
    }

    /// Drives one turn of freshly arrived callback input, handing any
    /// unconsumed borrowed suffix back to the transport.
    pub fn run<Runtime, Transitions, ID: Identity, W>(
        &mut self,
        bytes: &[u8],
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, W, Runtime, H, Transitions>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Drive<Transitions::Staged>
    where
        H: router::Routes<'d, Runtime, ID, Transitions>,
        Transitions: switch::Engine,
        Runtime: pump::Tasks,
        W: wire::Spec,
    {
        let bytes = match pump::Ingress::preflight(conn.state_mut(), bytes) {
            pump::Preflight::Drive => bytes,
            pump::Preflight::Resume => &[],
            pump::Preflight::Finished => {
                return Drive::new(Flow::Continue).with_input(deadline::Progress::new(
                    deadline::Head::Ready,
                    deadline::Body::None,
                ));
            }
            pump::Preflight::Settled => return Drive::new(Flow::Continue),
            pump::Preflight::Rejected(error) => {
                self.app
                    .as_mut()
                    .failure(&crate::wire::Failure::Pump(error));
                conn.close_after();
                return Drive::new(Flow::Closed);
            }
        };

        let id = conn.id();
        let prior = pump::Ingress::deferred(conn.state());
        let commit = conn.write().try_draft(driver, |state, write| {
            let mut disp = router::Dispatch {
                app: self.app.as_mut(),
                id,
                date: self.date,
            };
            let cycle = pump::Ingress::drive(state, pin::Pin::new(&mut disp), bytes, write, prior);
            Response::<H::TransportTag>::try_cycle(cycle, |charge| {
                router::Routes::response_tag(
                    disp.app.as_mut(),
                    disp.id,
                    &mut state.async_state,
                    charge,
                )
            })
        });
        let CycleCommit {
            progress,
            discard_remaining: left,
            state,
            transport: sent,
            wake,
            tail,
        } = match commit {
            Ok(write::Outcome::Committed(done)) => done,
            Ok(write::Outcome::Deferred(
                write::Deferred::Turn | write::Deferred::Capacity | write::Deferred::OrderedDrain,
            )) => {
                let held = pump::Ingress::retain(conn.state_mut(), bytes);
                return match held.state {
                    Ok(()) => Drive::new(Flow::Continue).with_input(held.progress),
                    Err(error) => {
                        self.app
                            .as_mut()
                            .failure(&crate::wire::Failure::Pump(error));
                        conn.close_after();
                        Drive::new(Flow::Closed).with_input(held.progress)
                    }
                };
            }
            Ok(write::Outcome::Closed) => return Drive::new(Flow::Closed),
            Err(cause) => {
                let (receipt, failure) = match Response::<H::TransportTag>::reject(cause) {
                    Failure::Capacity(cause) => (None, body::Failure::Capacity(cause)),
                    Failure::Rejected { cause, receipt } => (Some(receipt), cause),
                };
                if let Some(receipt) = receipt
                    && let Some(tag) = receipt.transport
                {
                    router::Routes::cancel(
                        self.app.as_mut(),
                        id,
                        &mut conn.state_mut().async_state,
                        tag,
                    );
                }
                conn.write().reject(failure);
                return Drive::new(Flow::Closed);
            }
        };
        match wake {
            Wake::None => {}
            Wake::Pending => conn.wake_target().wake(),
        }
        match state {
            Ok(()) => {
                if let Some(tag) = sent {
                    router::Routes::accept(
                        self.app.as_mut(),
                        id,
                        &mut conn.state_mut().async_state,
                        tag,
                    );
                }
            }
            Err(error) => {
                self.app
                    .as_mut()
                    .failure(&crate::wire::Failure::Pump(error));
                conn.close_after();
                return Drive::new(Flow::Closed);
            }
        }
        if let Some(prefix) = discard::Prefix::try_new(left)
            && matches!(conn.try_discard(prefix), discard::Start::Started)
        {
            pump::Ingress::discard(conn.state_mut());
        }
        Drive::new(Flow::Continue)
            .with_tail(tail)
            .with_input(progress)
    }

    /// Handles prefix write progress for a transition-capable connection.
    /// A staged switch remains inert until the gated adapter supplies its
    /// separate final-drain proof.
    pub fn progress<Runtime, Transitions, ID: Identity, W>(
        &mut self,
        conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, W, Runtime, H, Transitions>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> Settled
    where
        H: router::Routes<'d, Runtime, ID, Transitions>,
        Transitions: switch::Engine,
        Runtime: pump::Tasks,
        W: wire::Spec,
    {
        match pump::Drain::progress(conn.state_mut()) {
            pump::Next::None => Settled::new(Flow::Continue),
            pump::Next::Close(reason) => {
                if !reason.is_empty()
                    && let Some(reason) =
                        Response::<H::TransportTag>::send_static(conn, reason, driver)
                {
                    conn.state_mut().defer(reason);
                    return Settled::new(Flow::Continue);
                }
                conn.close_after();
                Settled::new(Flow::Closed)
            }
            pump::Next::Resume => self.resume(conn, driver),
        }
    }
}
