//! Generated-task HTTP/2 execution with pre-FIN request construction.

use core::convert::Infallible;

use conn::deadline;
use data::compose;
use dope::{
    core::driver::{retained, route::Identity},
    manifold::network::listener::{conn, handler},
    net::{link::egress::data, wire},
};
use ingress::errors;
use o3::{
    alloc,
    buffer::resident,
    depot::{heap, queue::indexed::lanes::typed::Value},
};
use sark_core::{
    body, decode, duplex, life,
    life::{exchange, mode, owners},
    manifold::{schedule::typed, task::app::Runtime},
};
use sark_http::owner::{self, cause};

use crate::{
    conn::{DataBlock, Error, event},
    frame::numbers,
    server::{
        self,
        response::Pending,
        runtime::output::{self, flush},
    },
    stream,
};

type Reply<'d, R> = response::Response<
    'd,
    <R as decode::Decode>::Prepared,
    <R as Runtime<'d>>::Completion,
    <R as duplex::Application<'d>>::Duplex,
>;

type Data<'d, R> = response::Data<'d, <R as duplex::Application<'d>>::Duplex>;
type Resident<'d, R> = response::Stored<
    'd,
    <R as decode::Decode>::Prepared,
    <R as Runtime<'d>>::Completion,
    <R as duplex::Application<'d>>::Duplex,
>;
type Next<'d, R> = typed::Next<Reply<'d, R>, Data<'d, R>, Infallible, DataBlock>;

type Built<'d, B> = body::BuildOutcome<
    stream::Key,
    <B as body::Build<'d>>::Output,
    errors::Build<<B as body::Build<'d>>::StepError>,
>;

type Borrowed<'a, 'd, R> = book::Turn<
    'a,
    'd,
    <R as decode::Decode>::Prepared,
    <R as decode::Decode>::Routes,
    <R as decode::Decode>::Plan,
    <R as Runtime<'d>>::Build,
    <R as Runtime<'d>>::Completion,
    <R as duplex::Application<'d>>::Duplex,
>;

type Carrier<'d, R, D> = State<
    'd,
    <R as decode::Decode>::Prepared,
    <R as decode::Decode>::Routes,
    <R as decode::Decode>::Plan,
    <R as Runtime<'d>>::Build,
    <R as Runtime<'d>>::Completion,
    <R as duplex::Application<'d>>::Duplex,
    <D as timers::Policy<'d>>::State,
>;

type Context<'a, 'b, 'c, 'e, 'f, 'd, ID, R, W, M, D> = conn::Ctx<
    'a,
    'b,
    'c,
    'e,
    'f,
    'd,
    ID,
    W,
    Carrier<'d, R, D>,
    data::Bound<<M as Transport<'d, ID>>::Output, <R as decode::Decode>::Egress<'d>>,
>;

type Retired<'a, 'b, 'd, ID, R, W, M, D> = conn::Retired<
    'a,
    'b,
    'd,
    ID,
    W,
    Carrier<'d, R, D>,
    data::Bound<<M as Transport<'d, ID>>::Output, <R as decode::Decode>::Egress<'d>>,
>;

mod app;
mod book;
mod ingress;
pub(in crate::server) mod registry;
pub(in crate::server) mod response;
mod state;
pub mod timers;
pub mod works;

pub use app::{App, retire::Retire};
pub(in crate::server) use state::State;

use crate::server::Target;

#[derive(Debug, Eq, PartialEq)]
pub enum Build {
    Connection(crate::conn::Build),
    Book(alloc::Error),
    Tasks(alloc::Error),
    Deadlines(heap::Error),
}

/// Scheduling and protocol bounds for the generated H2 adapter.
pub(in crate::server) struct Config {
    plan: server::Plan,
    tasks: usize,
}

impl Config {
    pub(in crate::server) const fn new(plan: server::Plan, tasks: usize) -> Self {
        Self { plan, tasks }
    }

    pub(super) const fn into_parts(self) -> (server::Plan, usize) {
        (self.plan, self.tasks)
    }
}

fn identify<C: Copy>(conn: C, stream: stream::Key) -> exchange::Id<C, stream::Key> {
    exchange::Id::new(conn, stream)
}

fn release_pending<'d, C: Copy, M: life::Mode<C, stream::Key>>(
    mode: &mut M,
    exchange: exchange::Id<C, stream::Key>,
    pending: Pending<'d>,
    cause: cause::Staging,
) {
    if !M::ENABLED {
        return;
    }
    let charge = pending.retention();
    drop(pending);
    owners::Projection::new(mode).apply(owner::Effect::new(
        exchange,
        charge,
        owner::Change::StagingReleased(cause),
    ));
}

pub trait Transport<'d, ID: Identity>: life::Mode<conn::Id<'d, ID>, stream::Key> {
    type Output: data::raw::Payload<'d> + 'd;

    fn release<R: decode::Decode>(
        egress: data::Bound<Self::Output, R::Egress<'d>>,
        cause: handler::Disposition,
    ) -> Option<owner::Effect<stream::Key>>;
}

impl<'d, ID: Identity> Transport<'d, ID> for mode::Disabled {
    type Output = resident::Shared<'d>;

    fn release<R: decode::Decode>(
        _: data::Bound<Self::Output, R::Egress<'d>>,
        _: handler::Disposition,
    ) -> Option<owner::Effect<stream::Key>> {
        None
    }
}

impl<'d, ID: Identity, O> Transport<'d, ID> for mode::Observed<O>
where
    O: life::Observer<conn::Id<'d, ID>, stream::Key>,
{
    type Output = compose::Tagged<resident::Shared<'d>, stream::Key>;

    fn release<R: decode::Decode>(
        egress: data::Bound<Self::Output, R::Egress<'d>>,
        cause: handler::Disposition,
    ) -> Option<owner::Effect<stream::Key>> {
        let (charge, tag) = {
            let (owner, tag) = egress.into_inner().into_parts();
            (data::raw::Payload::charge(&owner), tag)
        };
        let cause = match cause {
            handler::Disposition::Sent => cause::Transport::Sent,
            handler::Disposition::Closed => cause::Transport::Closed,
        };
        Some(owner::Effect::new(
            tag,
            charge,
            owner::Change::TransportReleased(cause),
        ))
    }
}

fn finish_transport<'d, ID: Identity, R, W, C, M>(
    conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, data::Bound<M::Output, R::Egress<'d>>>,
    driver: &mut retained::Context<'_, '_, 'd>,
    error: Option<Error>,
    mode: &mut M,
) where
    W: wire::Spec,
    C: output::State<'d>,
    R: decode::Decode,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
{
    let error = error.as_ref().map(|cause| {
        mode.diagnostic(cause);
        numbers::ErrorCode::from(cause)
    });
    flush::Flush::new(conn).observe(driver, error, mode);
}

pub(in crate::server::task) enum Sync {
    Armed,
    Finished,
}

fn arm_transport<'d, ID: Identity, R, W, C, M>(
    conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, data::Bound<M::Output, R::Egress<'d>>>,
    driver: &mut retained::Context<'_, '_, 'd>,
    arm: deadline::Arm<'d>,
    mode: &mut M,
) -> Sync
where
    W: wire::Spec,
    C: output::State<'d>,
    R: decode::Decode,
    M: Transport<'d, ID>,
    M::Output: flush::Evidence<'d>,
{
    use deadline::Arm;

    match arm {
        Arm::Armed => Sync::Armed,
        Arm::Disabled | Arm::Sealed | Arm::Overflow(_) => {
            flush::Flush::new(conn).observe(driver, Some(numbers::ErrorCode::InternalError), mode);
            Sync::Finished
        }
    }
}

fn release_retired<C: Copy, M: life::Mode<C, stream::Key>>(
    mode: &mut M,
    conn: C,
    write: event::Retirement,
    cause: cause::Staging,
) {
    let charge = write.charge;
    owners::Projection::new(mode).apply(owner::Effect::new(
        identify(conn, write.stream_id),
        charge,
        owner::Change::StagingReleased(cause),
    ));
}
