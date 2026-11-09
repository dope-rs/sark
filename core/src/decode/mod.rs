//! Request decoding plans and response encoding contracts.
use core::convert::Infallible;
use std::ops::ControlFlow;

use o3::buffer::{bytes::scope, pool, resident::bound, storage::Shared};
use sark_http::{
    field::{
        metadata::{
            packed,
            section::{
                self, rules,
                state::{RetainedHead, Selected},
                storage::Fault,
            },
        },
        text,
    },
    method,
    owner::handoff::{self, input::Bound, stage},
    payload::response::{
        dispatch::{Accept, Sink},
        status::Code,
    },
};

use crate::{body, decode::state::admit, request::frame::proof, route, route::field::header};

mod call;
pub mod dispatch;
pub mod exec;
pub mod request;
pub mod state;

pub use call::Call;

/// Projected request coupled to the lifetime of its body borrow.
#[repr(C)]
pub struct Scoped<'req, 'd: 'req, D: Decode + ?Sized, Q: stage::Proof = ()>(
    D::Projected<'req, 'd>,
    handoff::Slot<'req, 'd, (), Bound<'req, 'd, Q>>,
);

impl<'req, 'd: 'req, D: Decode + ?Sized, Q: stage::Proof> Scoped<'req, 'd, D, Q> {
    #[doc(hidden)]
    pub const fn new(
        value: D::Projected<'req, 'd>,
        source: handoff::Slot<'req, 'd, (), Bound<'req, 'd, Q>>,
    ) -> Self {
        Self(value, source)
    }

    pub fn into_inner(self) -> D::Projected<'req, 'd> {
        self.0
    }

    pub fn into_parts(
        self,
    ) -> (
        D::Projected<'req, 'd>,
        handoff::Slot<'req, 'd, (), Bound<'req, 'd, Q>>,
    ) {
        (self.0, self.1)
    }
}

/// Ownership of request-head storage while a response body may retain it.
pub trait Lineage<'id>: Sized {
    type Output;

    fn resolve_head<O: Into<scope::Owner<'id, Shared>>, P: proof::Proof>(
        self,
        head: crate::request::Head<O, P>,
    ) -> Self::Output;
}

/// Ends response preparation and returns its exact failure target.
pub trait Cancel {
    type Target;

    fn cancel(self) -> Self::Target;
}

pub trait Encode<'req, 'd: 'req>:
    Cancel
    + Accept<
        'd,
        Output = <Self as Encode<'req, 'd>>::Prepared,
        Error = (<Self as Encode<'req, 'd>>::Error, Self::Target),
    >
{
    type Error;
    type Prepared;
    type Pending<'id>: Lineage<'id, Output = Self::Prepared>;
    type Head<'a, 'id>: Accept<
            'd,
            Output = Self::Pending<'id>,
            Error = (<Self as Encode<'req, 'd>>::Error, Self::Target),
        > + Cancel<Target = Self::Target>;

    fn begin_head<'a, 'id>(self, head: scope::View<'a, 'id>) -> Self::Head<'a, 'id>;
}

/// Separates an encoded head from the body retained by a streaming task.
pub trait Streaming<'req, 'd: 'req>: Encode<'req, 'd> + Sink<'d> {
    fn split_stream<S>(output: Self::Stream<S>) -> (Self::Prepared, Option<S>);
}

/// Terminal semantic failure while preparing a request head.
#[derive(PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum HeadError<C = Infallible> {
    BadRequest,
    ContentLength(text::IntError),
    Method(method::Error),
    Bind(crate::request::BindError),
    Storage(Fault),
    Slot(header::Tag),
    NotFound,
    Schema(C),
}

impl<C> HeadError<C> {
    pub const fn status(&self) -> Code {
        match self {
            Self::BadRequest
            | Self::ContentLength(_)
            | Self::Method(method::Error::Syntax(_))
            | Self::Bind(_)
            | Self::Storage(_)
            | Self::Slot(_)
            | Self::Schema(_) => Code::BAD_REQUEST,
            Self::NotFound => Code::NOT_FOUND,
            Self::Method(method::Error::Allocation(_)) => Code::INTERNAL_SERVER_ERROR,
        }
    }
}

pub trait Decode {
    type Prepared: request::Prepared;
    type Routes: state::Family<Self::Prepared>;
    type Head: request::Head<
            Prepared = Self::Prepared,
            Start: request::Selection<
                Self::Prepared,
                Guard = Self::Routes,
                Routed = <Self::Routes as state::Family<Self::Prepared>>::State<state::Routed>,
            >,
        >;
    type Plan: rules::Policy;
    type Cause;
    type Error<'req>;
    type Projected<'req, 'd>
    where
        'd: 'req;

    /// Response backing bound; executable adapters require it to outlive their driver.
    type Egress<'d>: bound::Bound;

    fn reject<'req>(error: Self::Error<'req>) -> route::Reject;

    /// Routes an admitted request while retaining every decoded field.
    fn try_full(
        &self,
        head: RetainedHead<packed::Decoded, Selected<rules::RetainRaw>>,
    ) -> Result<Self::Head, HeadError<Self::Cause>>;

    fn try_head(
        &self,
        head: RetainedHead<<Self::Plan as section::Policy>::Block, Selected<Self::Plan>>,
    ) -> Result<Self::Head, HeadError<Self::Cause>>;

    fn try_project<'d, I, T, F>(
        &self,
        prepared: Self::Prepared,
        body: body::Input<I>,
        project: F,
    ) -> body::Projection<T, I::Owner, (Self::Prepared, body::Input<I>)>
    where
        I: handoff::Project<'d>,
        F: for<'req> FnOnce(Scoped<'req, 'd, Self, I::Proof>) -> T;

    fn try_dispatch<'req, 'd, E: Encode<'req, 'd>>(
        &self,
        request: Self::Projected<'req, 'd>,
        encoder: E,
    ) -> Dispatched<'req, 'd, E, Self::Error<'req>>
    where
        'd: 'req;
}

/// Routes a native raw head while preserving its compile-time storage prefix.
pub trait Full: Decode {
    fn try_prefixed<const PREFIX: usize>(
        &self,
        head: RetainedHead<packed::Decoded<pool::Span, PREFIX>, Selected<rules::RetainRaw<PREFIX>>>,
    ) -> Result<Self::Head, HeadError<Self::Cause>>;
}

/// Decoder capability implemented only by applications with guarded routes.
pub trait Guarded: Decode {
    fn try_admit<'req, 'd: 'req, E, F, G, V>(
        &self,
        request: request::Guarded<<Self::Routes as state::Family<Self::Prepared>>::State<F>>,
        encoder: E,
        visit: &mut V,
    ) -> Admission<'req, 'd, Self, E, F, G, V>
    where
        E: Encode<'req, 'd>,
        F: state::Set,
        G: state::Set,
        V: admit::TryVisit<Self::Prepared, F, G, <E as Encode<'req, 'd>>::Error>;
}

type Dispatched<'req, 'd, E, F> = Result<
    <E as Encode<'req, 'd>>::Prepared,
    (
        dispatch::Error<<E as Encode<'req, 'd>>::Error, Infallible, F>,
        <E as Cancel>::Target,
    ),
>;

type Admission<'req, 'd, D, E, F, G, V> = ControlFlow<
    <V as admit::TryVisit<<D as Decode>::Prepared, F, G, <E as Encode<'req, 'd>>::Error>>::Break<
        <E as Encode<'req, 'd>>::Prepared,
        <E as Cancel>::Target,
    >,
    <<<D as Decode>::Prepared as request::Prepared>::Family as state::Family<
        <D as Decode>::Prepared,
    >>::State<G>,
>;
