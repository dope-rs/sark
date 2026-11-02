use dope::{core::driver::route::Identity, manifold::network::listener::conn};
use o3::mem::quota;
use sark_core::{
    decode::{self, request},
    manifold::task::app::Runtime,
};

use crate::{
    conn::Error,
    server::{
        response::Handoff,
        state::Transitions,
        task::{
            Borrowed, Target,
            app::{
                header::admit::Begin,
                turn::{Availability, Outcome, Turn, guard::Guard},
            },
            works,
        },
    },
};

pub(in crate::server::task::app) trait Schedule<'d, R>:
    works::Family + Begin<'d, R>
where
    R: Runtime<'d>,
{
    fn admit<ID: Identity + 'd, Q>(
        graph: &mut Self::Graph,
        turn: &mut Turn<'_, '_, '_, '_, 'd, ID, R, Q>,
    ) -> quota::Admission<Result<Outcome, Error>>
    where
        R: Runtime<'d, Target = Target<'d, ID>>,
        Q: Transitions<conn::Id<'d, ID>>,
        Q::Retention: Handoff;

    fn account(graph: &mut Self::Graph);

    fn has_pending(state: &mut Borrowed<'_, 'd, R>) -> Availability;

    fn has_local(state: &mut Borrowed<'_, 'd, R>) -> Availability {
        Self::has_pending(state)
    }

    fn is_ready(state: &mut Borrowed<'_, 'd, R>) -> Availability {
        match Self::has_local(state) {
            Availability::Ready => Availability::Ready,
            Availability::Idle if state.has_events() || state.responses.has_runnable() => {
                Availability::Ready
            }
            Availability::Idle => Availability::Idle,
        }
    }
}

impl<'d, R, U> Schedule<'d, R> for request::Plain<U>
where
    R: Runtime<'d>,
    R::Head: request::Head<Start = request::Plain<U>>,
    request::Plain<U>: Begin<'d, R>,
{
    fn admit<ID: Identity + 'd, Q>(
        graph: &mut Self::Graph,
        turn: &mut Turn<'_, '_, '_, '_, 'd, ID, R, Q>,
    ) -> quota::Admission<Result<Outcome, Error>>
    where
        R: Runtime<'d, Target = Target<'d, ID>>,
        Q: Transitions<conn::Id<'d, ID>>,
        Q::Retention: Handoff,
    {
        graph.admit(|source| match source {
            works::Plain::Build => turn.build(),
            works::Plain::Task => turn.task(),
            works::Plain::Ingress => turn.ingress(),
            works::Plain::Egress => {
                let pump = turn.pump();
                turn.egress(pump)
            }
        })
    }

    fn account(graph: &mut Self::Graph) {
        graph.account(works::Plain::Egress);
    }

    fn has_pending(state: &mut Borrowed<'_, 'd, R>) -> Availability {
        if state.build_ready.is_empty() {
            Availability::Idle
        } else {
            Availability::Ready
        }
    }
}

impl<'d, R, S> Schedule<'d, R> for S
where
    R: Runtime<'d> + decode::Guarded,
    R::Head: request::Head<Start = S>,
    S: Guard + works::Family<Graph = works::GuardedGraph> + Begin<'d, R>,
{
    fn admit<ID: Identity + 'd, Q>(
        graph: &mut Self::Graph,
        turn: &mut Turn<'_, '_, '_, '_, 'd, ID, R, Q>,
    ) -> quota::Admission<Result<Outcome, Error>>
    where
        R: Runtime<'d, Target = Target<'d, ID>>,
        Q: Transitions<conn::Id<'d, ID>>,
        Q::Retention: Handoff,
    {
        graph.admit(|source| match source {
            works::Guarded::Admission => turn.admission(),
            works::Guarded::Build => turn.build(),
            works::Guarded::Task => turn.task(),
            works::Guarded::Ingress => turn.ingress(),
            works::Guarded::Egress => {
                let pump = turn.guarded_pump();
                turn.egress(pump)
            }
        })
    }

    fn account(graph: &mut Self::Graph) {
        graph.account(works::Guarded::Egress);
    }

    fn has_pending(state: &mut Borrowed<'_, 'd, R>) -> Availability {
        if state.admission_ready.is_empty() && state.build_ready.is_empty() {
            Availability::Idle
        } else {
            Availability::Ready
        }
    }

    fn has_local(state: &mut Borrowed<'_, 'd, R>) -> Availability {
        if state.can_admit() || !state.build_ready.is_empty() {
            Availability::Ready
        } else {
            Availability::Idle
        }
    }
}
