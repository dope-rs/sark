//! Server-driven connection lifetime transitions.

use dope::{
    core::driver::{self, route::Identity},
    manifold::network::listener::{conn, handler},
    net::{
        link::{egress::data, slot::types::CloseGate},
        wire,
    },
};
use o3::buffer::{resident::bound, storage};
use sark_core::duplex::{self, machine};

use crate::{
    pump,
    server::{duplex::Runtime, payload},
    session, switch,
};

impl<'d, State: pump::Tasks, Pending, Transitions: switch::Engine, Ready, Heads>
    session::Conn<'d, State, Pending, Transitions, Ready, Heads>
{
    pub fn http_close_gate(&self) -> CloseGate {
        if self.ingress.is_discard() || self.has_http_work() {
            CloseGate::Deferred
        } else {
            CloseGate::Immediate
        }
    }
}

impl<'d, State, Pending, D, Ready, Heads>
    session::Conn<'d, State, Pending, switch::Enabled<D>, Ready, Heads>
{
    /// Transfers the staged protocol and retained input on the final transport drain.
    pub fn finish_switch<'a, ID: Identity, W: wire::Spec, B: data::raw::Payload<'d>>(
        conn: &'a mut conn::Drained<'_, '_, '_, '_, '_, 'd, ID, W, Self, B>,
    ) -> Option<(&'a mut D, Option<storage::Shared>)> {
        let state = conn.state_mut();
        let raw = state.transitions.commit()?;
        Some((raw, state.ingress.recv.take_ready()))
    }
}

impl<'d, State: pump::Tasks, Pending, D: duplex::Session<'d>, Ready, Heads>
    session::Conn<'d, State, Pending, switch::Enabled<machine::Machine<'d, D>>, Ready, Heads>
{
    pub fn close_gate(&self) -> CloseGate {
        if self.http_close_gate() == CloseGate::Deferred {
            return CloseGate::Deferred;
        }
        match self.raw() {
            Ok(raw) if raw.progress() == machine::Progress::Finished => CloseGate::Immediate,
            Err(switch::Mode::Http) => CloseGate::Immediate,
            Ok(_) | Err(switch::Mode::Switching(_)) => CloseGate::Deferred,
        }
    }

    pub fn is_blocked(&self) -> bool {
        match self.raw() {
            Ok(raw) => {
                raw.hold() == machine::Hold::Held
                    || matches!(raw.access(), machine::Access::Blocked)
            }
            Err(switch::Mode::Http) => false,
            Err(switch::Mode::Switching(_)) => true,
        }
    }

    #[inline(always)]
    pub fn send_drained<ID: Identity, W: wire::Spec, B: data::raw::Payload<'d>>(
        mut conn: conn::Drained<'_, '_, '_, '_, '_, 'd, ID, W, Self, B>,
    ) -> handler::PostDrain {
        let progress = if let Some((raw, tail)) = Self::finish_switch(&mut conn) {
            if let Some(tail) = tail {
                match raw.input() {
                    machine::Access::Vacant(slot) => slot.commit(tail.into()),
                    machine::Access::Blocked => {
                        conn.close_after();
                        return handler::PostDrain::Idle;
                    }
                }
            }
            raw.progress()
        } else {
            if conn.state_mut().resume_after_drain() {
                return handler::PostDrain::Activate;
            }
            match conn.state().raw() {
                Ok(raw) => raw.progress(),
                Err(switch::Mode::Http | switch::Mode::Switching(_)) => {
                    return handler::PostDrain::Idle;
                }
            }
        };
        match progress {
            machine::Progress::Turn => handler::PostDrain::Activate,
            machine::Progress::Idle | machine::Progress::Finished => handler::PostDrain::Idle,
        }
    }
}

/// Applies listener lifetime events to one exact H1 connection.
pub trait Drain<'d, ID: Identity, B, K = ()>: Sized
where
    B: data::raw::Payload<'d>,
{
    fn begin<W: wire::Spec>(
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, Self, B>,
        driver: &mut driver::Context<'_, 'd>,
        scratch: &mut Vec<u8>,
    ) -> handler::Drain;
}

impl<'d, State, Pending, Ready, Heads, ID: Identity, B, K> Drain<'d, ID, B, K>
    for session::Conn<'d, State, Pending, switch::Disabled, Ready, Heads>
where
    B: data::raw::Payload<'d>,
    State: pump::Tasks,
{
    fn begin<W: wire::Spec>(
        mut conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, Self, B>,
        _: &mut driver::Context<'_, 'd>,
        _: &mut Vec<u8>,
    ) -> handler::Drain {
        let result = if conn.state().has_http_work() {
            handler::Drain::Pending
        } else {
            handler::Drain::Complete
        };
        conn.close_after();
        result
    }
}

impl<'d, State, Pending, Ready, Heads, ID: Identity, D, K>
    Drain<'d, ID, data::Bound<payload::Owner<'d, K>, bound::Unbounded>, K>
    for session::Conn<'d, State, Pending, switch::Enabled<machine::Machine<'d, D>>, Ready, Heads>
where
    D: duplex::Session<'d>,
    K: Copy + Default + 'd,
    State: pump::Tasks,
{
    fn begin<W: wire::Spec>(
        mut conn: conn::Ctx<
            '_,
            '_,
            '_,
            '_,
            '_,
            'd,
            ID,
            W,
            Self,
            data::Bound<payload::Owner<'d, K>, bound::Unbounded>,
        >,
        driver: &mut driver::Context<'_, 'd>,
        scratch: &mut Vec<u8>,
    ) -> handler::Drain {
        conn.close_after();
        if matches!(conn.state().mode(), switch::Mode::Raw(_)) {
            return Runtime::<_, K>::new(&mut conn, scratch).finish_input(driver);
        }
        let state = conn.state_mut();
        if state.can_resume() {
            return handler::Drain::Pending;
        }
        if let Ok(raw) = state.staged_mut() {
            if let Ok((finish, ())) = raw.finish(()) {
                finish.commit();
            }
            return handler::Drain::Pending;
        }
        if state.deferred_action.is_some() || state.async_state.activity() != pump::Activity::Idle {
            handler::Drain::Pending
        } else {
            handler::Drain::Complete
        }
    }
}
