use std::pin;

use dope::{
    core::driver::route::Identity,
    manifold::{app::receive, network::listener::handler::retire},
    net::link::pool::recv::input::Execution,
};
use sark_core::{decode::request, manifold::task::app::Runtime};

use crate::server::{
    Target,
    response::Handoff,
    runtime::output::flush,
    task::{
        App, Retired, Transport,
        app::{boundary::close, turn::schedule::Schedule},
        timers,
    },
};

pub struct Retire;

impl<'d, ID: Identity + 'd, R, W, M, D, P> retire::Policy<'d, App<'d, ID, R, W, M, D, P>, ID>
    for Retire
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
    M::Retention: Handoff,
    D: timers::Policy<'d>,
    <R::Head as request::Head>::Start: Schedule<'d, R>,
{
    fn select() -> Option<Self> {
        Some(Self)
    }

    fn retire(
        self,
        app: pin::Pin<&mut App<'d, ID, R, W, M, D, P>>,
        mut conn: Retired<'_, '_, 'd, ID, R, W, M, D>,
    ) {
        let this = app.project();
        close::Close::new(this.router, this.tasks, this.mode).audit::<W, D>(&mut conn);
    }
}
