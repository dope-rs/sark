use std::{mem, pin};

use conn::write::{self, body};
use data::compose;
use dope::{
    core::driver::{self, route::Identity},
    manifold::network::listener::conn,
    net::{link::egress::data, wire},
};
use o3::buffer::hold;
use wire::receive::discard;

use crate::{
    pump,
    server::{conn::router, deadline, payload},
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
    let admission = 'output: {
        let mut native_slot = mem::MaybeUninit::uninit();
        let mut native = match conn.try_output(driver, &mut native_slot) {
            Ok(native) => native,
            Err(cause) => break 'output cause,
        };
        let (cycle, tag, sent) = {
            let (state, writer) = native.get_mut().parts();
            let cycle = pump::Ingress::drive(state, routes.as_mut(), bytes, writer, prior);
            let charge = match cycle.state.as_ref() {
                Ok(pump::Egress::Shared { body }) => {
                    crate::wire::output::Owner::shared(body).charge
                }
                Ok(pump::Egress::Frozen { body }) => {
                    crate::wire::output::Owner::shared(body).charge
                }
                Ok(pump::Egress::Segmented { body }) => body.charge(),
                _ => hold::Charge::default(),
            };
            let (tag, sent) = if charge.resident_bytes == 0 {
                (H::TransportTag::default(), None)
            } else {
                let tag = routes.as_mut().response_tag(&mut state.async_state, charge);
                (tag, Some(tag))
            };
            (cycle, tag, sent)
        };
        let pump::Cycle {
            progress,
            discard_remaining: left,
            state,
            connection,
            tail,
        } = cycle;
        let (egress, state) = match state {
            Ok(egress) => (egress, Ok(())),
            Err(cause) => (pump::Egress::None, Err(cause)),
        };
        let wake = matches!(egress, pump::Egress::Wake);
        let body = payload::write::try_prepare(egress, tag);
        let end = match connection {
            crate::wire::head::Connection::KeepAlive => write::End::Open,
            crate::wire::head::Connection::Close => write::End::Close,
        };
        if let Err(cause) = payload::write::try_publish(native, body, end) {
            if let Some(tag) = sent {
                routes
                    .as_mut()
                    .cancel(&mut conn.state_mut().async_state, tag);
            }
            conn.reject_write(cause);
            return Drive::new(Flow::Closed);
        }
        if wake {
            conn.wake_target().wake();
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
        return Drive {
            flow: Flow::Continue,
            tail,
            progress,
        };
    };
    match admission {
        write::error::Admission::Deferred(_) => {
            let held = pump::Ingress::retain(conn.state_mut(), bytes);
            match held.state {
                Ok(()) => Drive::new(Flow::Continue).with_input(held.progress),
                Err(error) => {
                    routes.as_mut().failure(&crate::wire::Failure::Pump(error));
                    conn.close_after();
                    Drive::new(Flow::Closed).with_input(held.progress)
                }
            }
        }
        write::error::Admission::Closed => Drive::new(Flow::Closed),
        write::error::Admission::Rejected(cause) => {
            conn.reject_write(body::Failure::Capacity(cause));
            Drive::new(Flow::Closed)
        }
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
