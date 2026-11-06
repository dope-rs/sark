//! Generated-task HTTP/2 execution with pre-FIN request construction.

use core::convert::Infallible;

use data::compose;
use dope::{
    core::driver::route::Identity,
    manifold::network::listener::{conn, handler},
    net::link::egress::data,
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
    manifold::{schedule::typed, server::app as semantic, task::app::Runtime},
};
use sark_http::owner::{self, cause};

use crate::{
    conn::{DataBlock, event},
    frame::numbers,
    server::response::Pending,
    stream,
};

type Reply<'d, R> = response::Response<
    'd,
    <R as decode::Decode>::Prepared,
    <R as duplex::Application<'d>>::Duplex,
    <R as Runtime<'d>>::Fixed,
>;

type Data<'d, R> = response::Data<'d, <R as duplex::Application<'d>>::Duplex>;
type Resident<'d, R> = response::Stored<
    'd,
    <R as decode::Decode>::Prepared,
    <R as duplex::Application<'d>>::Duplex,
    <R as Runtime<'d>>::Fixed,
>;
type Next<'d, R> = typed::Next<Reply<'d, R>, Data<'d, R>, Infallible, DataBlock>;

type Built<'d, B> = body::BuildOutcome<
    stream::Key,
    <B as body::Build<'d>>::Output,
    errors::Build<<B as body::Build<'d>>::StepError>,
>;

type Borrowed<'a, 'd, R, F = <R as decode::Decode>::Plan> = book::Turn<
    'a,
    'd,
    <R as decode::Decode>::Prepared,
    <R as decode::Decode>::Routes,
    F,
    <R as Runtime<'d>>::Build,
    <R as duplex::Application<'d>>::Duplex,
    <R as Runtime<'d>>::Fixed,
>;

type Carrier<'d, R, D, F = <R as decode::Decode>::Plan, S = ()> = State<
    'd,
    <R as decode::Decode>::Prepared,
    <R as decode::Decode>::Routes,
    F,
    <R as Runtime<'d>>::Build,
    <R as duplex::Application<'d>>::Duplex,
    <R as Runtime<'d>>::Fixed,
    <D as timers::Policy<'d>>::State,
    S,
>;

type Context<'a, 'b, 'c, 'e, 'f, 'd, ID, R, W, M, D, A = service::Native<R>> = conn::Ctx<
    'a,
    'b,
    'c,
    'e,
    'f,
    'd,
    ID,
    W,
    Carrier<'d, R, D, <A as semantic::Spec>::Fields, service::State<'d, ID, A>>,
    data::Bound<<M as Transport<'d, ID>>::Output, <A as semantic::Spec>::Egress<'d>>,
>;

type Retired<'a, 'b, 'd, ID, R, W, M, D, A = service::Native<R>> = conn::Retired<
    'a,
    'b,
    'd,
    ID,
    W,
    Carrier<'d, R, D, <A as semantic::Spec>::Fields, service::State<'d, ID, A>>,
    data::Bound<<M as Transport<'d, ID>>::Output, <A as semantic::Spec>::Egress<'d>>,
>;

mod app;
mod book;
mod ingress;
pub(in crate::server) mod registry;
pub(in crate::server) mod response;
pub mod service;
mod state;
pub mod timers;
pub mod works;

pub use app::{App, retire::Retire};
pub(in crate::server) use state::State;

use crate::server::Target;

#[derive(Debug, Eq, PartialEq)]
pub enum Build<E = Infallible> {
    Connection(crate::conn::Build),
    Book(alloc::Error),
    Tasks(alloc::Error),
    Deadlines(heap::Error),
    Service(E),
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

    fn release<B: resident::bound::Bound>(
        egress: data::Bound<Self::Output, B>,
        cause: handler::Disposition,
    ) -> Option<owner::Effect<stream::Key>>;
}

impl<'d, ID: Identity> Transport<'d, ID> for mode::Disabled {
    type Output = resident::Shared<'d>;

    fn release<B: resident::bound::Bound>(
        _: data::Bound<Self::Output, B>,
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

    fn release<B: resident::bound::Bound>(
        egress: data::Bound<Self::Output, B>,
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

pub(in crate::server::task) enum Sync {
    Armed,
    Finished,
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

fn rejected<'d, R: Runtime<'d>>(
    failure: sark_core::manifold::schedule::Insert<stream::Key, Next<'d, R>>,
) -> (stream::Key, numbers::ErrorCode) {
    use sark_core::manifold::schedule::Insert;
    match failure {
        Insert::Full { stream, .. } => (stream, numbers::ErrorCode::EnhanceYourCalm),
        Insert::Occupied { stream, .. } | Insert::Excluded { stream, .. } => {
            (stream, numbers::ErrorCode::InternalError)
        }
    }
}
