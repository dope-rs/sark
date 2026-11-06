use std::pin;

use dope::{
    core::driver::route::Identity,
    manifold::{
        app::receive,
        network::listener::{conn::Id, handler::retire},
    },
    net::link::pool::recv::input::Execution,
};
use sark_core::{
    decode::request,
    manifold::{server::app as semantic, task::app::Runtime},
};

use crate::server::{
    Target,
    response::Handoff,
    runtime::output::flush,
    task::{
        App, Build, Retired, Transport,
        app::{boundary::close, turn::schedule::Schedule},
        service, timers,
    },
};

pub struct Retire;

impl<'d, ID: Identity + 'd, R, W, M, D, P, A> retire::Policy<'d, App<'d, ID, R, W, M, D, P, A>, ID>
    for Retire
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    A: service::Select<R> + semantic::Timed<'d, service::Connection<'d, A>, Id<'d, ID>>,
    W: Execution + 'd,
    receive::Scoped: receive::Mode<W>,
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
        close::Close::new(this.router, this.tasks, this.mode).audit::<W, D, A>(&mut conn);
    }
}
