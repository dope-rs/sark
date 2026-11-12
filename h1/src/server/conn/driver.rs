use std::pin;

use conn::write::{self, body};
use data::compose;
use dope::{
    core::driver::{self, route::Identity},
    manifold::network::listener::conn,
    net::{link::egress::data, wire},
};
use wire::receive::discard;

use crate::{
    pump,
    server::{
        conn::router,
        deadline,
        payload::{
            self,
            write::{CycleCommit, Failure, Wake},
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
        <H as router::Routes<'d, R, T>>::Pending,
        T,
        <H as session::Dispatch<'d, R, <H as router::Routes<'d, R, T>>::Pending, T>>::Ready,
        <H as session::Dispatch<'d, R, <H as router::Routes<'d, R, T>>::Pending, T>>::Head,
    >,
    data::Bound<
        compose::Either<
            payload::Body<'d, <H as router::Routes<'d, R, T>>::TransportTag>,
            payload::Stream<'d, <H as router::Routes<'d, R, T>>::TransportTag>,
        >,
        <H as router::Routes<'d, R, T>>::Egress,
    >,
>;

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
    pub const fn is_runnable(&self, activity: pump::Activity) -> bool {
        matches!(
            (self, activity),
            (
                Self::Continue,
                pump::Activity::Task | pump::Activity::Stream
            )
        )
    }
}

impl<H> Drive<H> {
    const fn new(flow: Flow) -> Self {
        Self {
            flow,
            tail: pump::Tail::Empty,
            progress: deadline::Progress::new(deadline::Head::Unchanged, deadline::Body::None),
        }
    }

    fn with_input(mut self, progress: deadline::Progress) -> Self {
        self.progress = progress;
        self
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

    pub fn into_parts(self) -> (Flow, deadline::Progress) {
        (self.flow, self.progress)
    }
}

/// Drives one turn that supplies no input to the pump.
pub fn resume<'d, H, Runtime, Transitions, ID: Identity, W>(
    routes: pin::Pin<&mut H>,
    conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, W, Runtime, H, Transitions>,
    driver: &mut driver::Context<'_, 'd>,
) -> Settled
where
    H: router::Routes<'d, Runtime, Transitions>,
    Transitions: switch::Engine,
    Runtime: pump::Tasks,
    W: wire::Spec,
{
    // Empty input cannot construct `pump::Tail::Handoff`, so no borrowed suffix is kept.
    let Drive { flow, progress, .. } = run(routes, &[], conn, driver);
    Settled { flow, progress }
}

/// Drives one turn of freshly arrived callback input, handing any
/// unconsumed borrowed suffix back to the transport.
pub fn run<'d, H, Runtime, Transitions, ID: Identity, W>(
    mut routes: pin::Pin<&mut H>,
    bytes: &[u8],
    conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, W, Runtime, H, Transitions>,
    driver: &mut driver::Context<'_, 'd>,
) -> Drive<Transitions::Staged>
where
    H: router::Routes<'d, Runtime, Transitions>,
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
            routes.as_mut().failure(&crate::wire::Failure::Pump(error));
            conn.close_after();
            return Drive::new(Flow::Closed);
        }
    };

    let prior = pump::Ingress::deferred(conn.state());
    let commit = conn.write().try_commit(driver, |state, mut prefix| {
        let cycle = pump::Ingress::drive(state, routes.as_mut(), bytes, prefix.writer(), prior);
        payload::write::try_cycle(prefix, cycle, |charge| {
            routes.as_mut().response_tag(&mut state.async_state, charge)
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
                    routes.as_mut().failure(&crate::wire::Failure::Pump(error));
                    conn.close_after();
                    Drive::new(Flow::Closed).with_input(held.progress)
                }
            };
        }
        Ok(write::Outcome::Closed) => return Drive::new(Flow::Closed),
        Err(cause) => {
            let (receipt, failure) = match payload::write::reject(cause) {
                Failure::Capacity(cause) => (None, body::Failure::Capacity(cause)),
                Failure::Rejected { cause, receipt } => (Some(receipt), cause),
            };
            if let Some(receipt) = receipt
                && let Some(tag) = receipt.transport
            {
                routes
                    .as_mut()
                    .cancel(&mut conn.state_mut().async_state, tag);
            }
            conn.write().reject(failure);
            return Drive::new(Flow::Closed);
        }
    };
    match wake {
        Wake::None => {}
        Wake::Pending => conn.wake_target().wake(),
    }
    if let Err(error) = state {
        routes.as_mut().failure(&crate::wire::Failure::Pump(error));
        conn.close_after();
        return Drive::new(Flow::Closed);
    }
    if let Some(tag) = sent {
        routes
            .as_mut()
            .accept(&mut conn.state_mut().async_state, tag);
    }
    if let Some(prefix) = discard::Prefix::try_new(left)
        && matches!(conn.try_discard(prefix), discard::Start::Started)
    {
        pump::Ingress::discard(conn.state_mut());
    }
    Drive {
        flow: Flow::Continue,
        tail,
        progress,
    }
}

/// Handles prefix write progress for a transition-capable connection.
/// A staged switch remains inert until the gated adapter supplies its
/// separate final-drain proof.
pub fn progress<'d, H, Runtime, Transitions, ID: Identity, W>(
    routes: pin::Pin<&mut H>,
    conn: &mut Context<'_, '_, '_, '_, '_, 'd, ID, W, Runtime, H, Transitions>,
    driver: &mut driver::Context<'_, 'd>,
) -> Settled
where
    H: router::Routes<'d, Runtime, Transitions>,
    Transitions: switch::Engine,
    Runtime: pump::Tasks,
    W: wire::Spec,
{
    match pump::Drain::progress(conn.state_mut()) {
        pump::Next::None => Settled::new(Flow::Continue),
        pump::Next::Close(reason) => {
            if !reason.is_empty()
                && let Some(reason) = payload::write::send_static(conn, reason, driver)
            {
                conn.state_mut().defer(reason);
                return Settled::new(Flow::Continue);
            }
            conn.close_after();
            Settled::new(Flow::Closed)
        }
        pump::Next::Resume => resume(routes, conn, driver),
    }
}
