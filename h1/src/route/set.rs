//! Closed continuation operations for a generated portable router.

use o3::buffer::segments::assembly;
use sark_core::{body::credit, route::table};
use sark_http::{
    h1::head::request::Coding,
    method::Method,
    payload::{compress, owned},
};

use crate::{
    route::{PolicyOf, sync},
    wire::parsing::{self, framing},
};

pub trait Slot<S, Set>: table::Slot<S, Set = Set, Route: PolicyOf> {
    type Pending<'d>;
    type Head;

    const METHOD: Method;

    fn head(value: framing::PendingHead<Self::Route, Coding>) -> Self::Head
    where
        Self::Route: table::Sync<S>;

    fn pending<'d>(value: sync::Pending<'d, Self::Route>) -> Self::Pending<'d>
    where
        Self::Route: table::Sync<S>;
}

pub trait Visit<'io, S, Set, Pending, Heads> {
    type Output;

    fn visit<P>(self, found: parsing::raw::Bound<&parsing::Head<'_>, P::Route>) -> Self::Output
    where
        P: Slot<S, Set, Pending<'io> = Pending, Head = Heads>,
        P::Route: table::Sync<S>;
}

pub trait OwnedVisit<'io, S, Set, Pending, Heads> {
    type Output;

    fn visit<P>(self, found: parsing::raw::Bound<parsing::Owned, P::Route>) -> Self::Output
    where
        P: Slot<S, Set, Pending<'io> = Pending, Head = Heads>,
        P::Route: table::Sync<S>;
}

pub trait ResumeHead<'io, S, Set, Pending, Heads> {
    type Output;

    fn resume<P>(self, pending: framing::PendingHead<P::Route, Coding>) -> Self::Output
    where
        P: Slot<S, Set, Pending<'io> = Pending, Head = Heads>,
        P::Route: table::Sync<S>;
}

pub trait Edit<'io, S, Set> {
    type Output;

    fn edit<P>(self, pending: &mut sync::Pending<'io, P::Route>) -> Self::Output
    where
        P: Slot<S, Set>,
        P::Route: table::Sync<S>;
}

impl<'io, S, Set> Edit<'io, S, Set> for owned::Segments<'io> {
    type Output = Result<(), assembly::Error>;

    fn edit<P>(self, pending: &mut sync::Pending<'io, P::Route>) -> Self::Output
    where
        P: Slot<S, Set>,
        P::Route: table::Sync<S>,
    {
        pending.push_body(self, credit::Disabled)
    }
}

pub trait Take<'io, S, Set> {
    type Output;

    fn take<P>(self, pending: sync::Pending<'io, P::Route>) -> Self::Output
    where
        P: Slot<S, Set>,
        P::Route: table::Sync<S>;
}

pub trait Table<S> {
    type Set;
    type Pending<'io>;
    type Head;
    type Compression: compress::Compression;

    const LEN: usize;
    const HAS_CACHE: bool;
    const METHODS: u8;

    /// Looks up one route, returning the visitor back when nothing matched so
    /// the caller can settle without a second lookup or an impossible branch.
    fn find<'io, V>(head: &parsing::Head<'_>, visit: V) -> Result<V::Output, V>
    where
        V: Visit<'io, S, Self::Set, Self::Pending<'io>, Self::Head>;

    fn find_owned<'io, V>(
        owner: parsing::Owned,
        visit: V,
    ) -> Result<V::Output, (parsing::Owned, V)>
    where
        V: OwnedVisit<'io, S, Self::Set, Self::Pending<'io>, Self::Head>;

    fn resume_head<'io, H>(pending: Self::Head, resume: H) -> H::Output
    where
        H: ResumeHead<'io, S, Self::Set, Self::Pending<'io>, Self::Head>;

    fn edit<'io, E>(pending: &mut Self::Pending<'io>, edit: E) -> E::Output
    where
        E: Edit<'io, S, Self::Set>;

    fn take<'io, T>(pending: Self::Pending<'io>, take: T) -> T::Output
    where
        T: Take<'io, S, Self::Set>;
}
