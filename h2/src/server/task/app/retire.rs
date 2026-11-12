use crate::server::app as semantic;
use std::pin;

use dope::{
    core::driver::route::Identity,
    manifold::{
        app::receive,
        network::listener::{conn::Id, handler::retire},
    },
    net::link::pool::recv::input::Execution,
};
use sark_core::{decode::request, life, manifold::task::app::Runtime};

use crate::server::{
    Target,
    response::Handoff,
    runtime::output::flush,
    task::{App, Build, Retired, Transport, app::turn::schedule::Schedule, service, timers},
};

pub struct Retire;

impl<'d, ID: Identity + 'd, R, W, M, D, P, A> retire::Policy<'d, App<'d, ID, R, W, M, D, P, A>, ID>
    for Retire
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    A: service::Select<R> + semantic::Timed<'d, crate::conn::event::Retirement, Id<'d, ID>>,
    W: Execution + 'd,
    receive::Scoped<true>: receive::Mode<W>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    M::Retention: Handoff,
    D: timers::Policy<'d>,
    P: crate::server::runtime::app::Fault<'d, W, A::Failure, ID, Build<A::Build>>,
    <R::Head as request::Head>::Start: Schedule<'d, R>,
{
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(
        self,
        app: pin::Pin<&mut App<'d, ID, R, W, M, D, P, A>>,
        mut conn: Retired<'_, '_, 'd, ID, R, W, M, D, A>,
    ) {
        let this = app.project();
        let id = conn.id();
        let bytes = conn.response_len();
        let state = conn.state_mut();
        let count = state.task_index.len();
        let empty = state.task_index.is_empty();
        life::Mode::close(this.mode, id, || {
            state.turn().audit(count, empty).wire_bytes(bytes)
        });
    }
}
