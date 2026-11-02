//! Server-driven connection lifetime transitions.

use data::compose;
use dope::{
    core::driver::{retained, route::Identity},
    manifold::network::listener::{conn, handler},
    net::{link::egress::data, wire},
};
use o3::buffer::resident::bound;
use sark_core::duplex::{self, machine};

use crate::{
    pump,
    server::{duplex::Runtime, payload},
    session, switch,
};

/// Applies listener lifetime events to one exact H1 connection.
pub trait Drain<'d, ID: Identity, B, K = ()>: Sized
where
    B: data::raw::Payload<'d>,
{
    fn begin<W: wire::Spec>(
        conn: conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, Self, B>,
        driver: &mut retained::Context<'_, '_, 'd>,
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
        _: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Drain {
        let state = conn.state();
        if state.can_resume() {
            conn.close_after();
            return handler::Drain::Pending;
        }
        let result = match (state.async_state.activity(), state.deferred_action.as_ref()) {
            (pump::Activity::Idle, None) => handler::Drain::Complete,
            (pump::Activity::Idle, Some(_))
            | (pump::Activity::Task | pump::Activity::Stream, _) => handler::Drain::Pending,
        };
        conn.close_after();
        result
    }
}

impl<'d, State, Pending, Ready, Heads, ID: Identity, D, K>
    Drain<
        'd,
        ID,
        data::Bound<
            compose::Either<payload::Body<'d, K>, payload::Stream<'d, K>>,
            bound::Unbounded,
        >,
        K,
    > for session::Conn<'d, State, Pending, switch::Enabled<machine::Machine<'d, D>>, Ready, Heads>
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
            data::Bound<
                compose::Either<payload::Body<'d, K>, payload::Stream<'d, K>>,
                bound::Unbounded,
            >,
        >,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> handler::Drain {
        conn.close_after();
        let mode = conn.state().mode();
        if matches!(mode, switch::Mode::Raw(_)) {
            return Runtime::<_, K>::new(&mut conn).finish_input(driver);
        }
        let state = conn.state_mut();
        if state.can_resume() {
            return handler::Drain::Pending;
        }
        match state.transitions.active_mut() {
            Ok(raw) => match raw.finish(()) {
                Ok((finish, ())) => finish.commit(),
                Err((machine::FinishError::Finished | machine::FinishError::Closed, ())) => {}
            },
            Err(switch::Mode::Http) => {}
        }
        match (
            state.async_state.activity(),
            state.deferred_action.as_ref(),
            mode,
        ) {
            (pump::Activity::Idle, None, switch::Mode::Http) => handler::Drain::Complete,
            (
                pump::Activity::Idle,
                None | Some(_),
                switch::Mode::Switching(_) | switch::Mode::Raw(_),
            )
            | (pump::Activity::Idle, Some(_), switch::Mode::Http)
            | (pump::Activity::Task | pump::Activity::Stream, _, _) => handler::Drain::Pending,
        }
    }
}
