use dope::{core::driver::route::Identity, manifold::network::listener::conn, net::wire};
use o3::mem::quota;
use sark_core::{
    decode::{self, request},
    manifold::{server::app as semantic, task::app::Runtime},
};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::Error,
    server::{
        response::Handoff,
        runtime::app::Fault,
        state::Transitions,
        task::{
            Borrowed, Build, Target,
            app::{
                header::admit::Begin,
                turn::{Outcome, Turn, guard::Guard},
            },
            service, works,
        },
    },
};

pub(in crate::server::task::app) trait Schedule<'d, R>:
    works::Family + Begin<'d, R>
where
    R: Runtime<'d>,
{
    fn admit<ID: Identity + 'd, Q, W, A, P>(
        graph: &mut Self::Graph,
        turn: &mut Turn<'_, '_, '_, 'd, ID, R, Q, W, A, P>,
    ) -> quota::Admission<Result<Outcome, semantic::Error<Error, A::Failure>>>
    where
        R: Runtime<'d, Target = Target<'d, ID>>,
        Q: Transitions<conn::Id<'d, ID>>,
        Q::Retention: Handoff,
        W: wire::Spec,
        A: service::Select<R>
            + semantic::Application<'d, service::Connection<'d, A>, conn::Id<'d, ID>>,
        P: Fault<'d, W, A::Failure, ID, Build<A::Build>>;

    fn account(graph: &mut Self::Graph);

    fn has_local<F: rules::Policy>(state: &mut Borrowed<'_, 'd, R, F>) -> bool;

    fn is_ready<F: rules::Policy>(state: &mut Borrowed<'_, 'd, R, F>) -> bool {
        Self::has_local(state) || state.has_events() || state.responses.has_runnable()
    }
}

impl<'d, R, U> Schedule<'d, R> for request::Plain<U>
where
    R: Runtime<'d>,
    R::Head: request::Head<Start = request::Plain<U>>,
    request::Plain<U>: Begin<'d, R>,
{
    fn admit<ID: Identity + 'd, Q, W, A, P>(
        graph: &mut Self::Graph,
        turn: &mut Turn<'_, '_, '_, 'd, ID, R, Q, W, A, P>,
    ) -> quota::Admission<Result<Outcome, semantic::Error<Error, A::Failure>>>
    where
        R: Runtime<'d, Target = Target<'d, ID>>,
        Q: Transitions<conn::Id<'d, ID>>,
        Q::Retention: Handoff,
        W: wire::Spec,
        A: service::Select<R>
            + semantic::Application<'d, service::Connection<'d, A>, conn::Id<'d, ID>>,
        P: Fault<'d, W, A::Failure, ID, Build<A::Build>>,
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

    fn has_local<F: rules::Policy>(state: &mut Borrowed<'_, 'd, R, F>) -> bool {
        !state.build_ready.is_empty()
    }
}

impl<'d, R, S> Schedule<'d, R> for S
where
    R: Runtime<'d> + decode::Guarded,
    R::Head: request::Head<Start = S>,
    S: Guard + works::Family<Graph = works::GuardedGraph> + Begin<'d, R>,
{
    fn admit<ID: Identity + 'd, Q, W, A, P>(
        graph: &mut Self::Graph,
        turn: &mut Turn<'_, '_, '_, 'd, ID, R, Q, W, A, P>,
    ) -> quota::Admission<Result<Outcome, semantic::Error<Error, A::Failure>>>
    where
        R: Runtime<'d, Target = Target<'d, ID>>,
        Q: Transitions<conn::Id<'d, ID>>,
        Q::Retention: Handoff,
        W: wire::Spec,
        A: service::Select<R>
            + semantic::Application<'d, service::Connection<'d, A>, conn::Id<'d, ID>>,
        P: Fault<'d, W, A::Failure, ID, Build<A::Build>>,
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

    fn has_local<F: rules::Policy>(state: &mut Borrowed<'_, 'd, R, F>) -> bool {
        state.can_admit() || !state.build_ready.is_empty()
    }
}
