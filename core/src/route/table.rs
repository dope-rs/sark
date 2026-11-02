//! Static route-table contracts shared by protocol adapters.

use sark_http::{method, payload::response::dispatch};

use crate::{
    decode, manifold,
    request::path,
    route::{self, guard},
};

/// One protocol-neutral route lookup.
pub struct Key<'a> {
    method: method::Known,
    target: &'a [u8],
    protocol: Option<&'a [u8]>,
}

impl<'a> Key<'a> {
    pub const fn new(method: method::Known, target: &'a [u8], protocol: Option<&'a [u8]>) -> Self {
        Self {
            method,
            target,
            protocol,
        }
    }

    #[doc(hidden)]
    pub const fn into_parts(self) -> (method::Known, &'a [u8], Option<&'a [u8]>) {
        (self.method, self.target, self.protocol)
    }
}

/// A synchronous handler usable by the portable router.
pub trait Sync<S>: route::Spec<Kind = dispatch::Sync> + manifold::Route<S> {}

impl<S, R> Sync<S> for R where R: route::Spec<Kind = dispatch::Sync> + manifold::Route<S> {}

/// Generated identity for one route inside exactly one table.
pub trait Slot<S>: guard::Slot<S> {
    type Set;
    type Route: route::Spec;

    const INDEX: u32;
}

/// Direct leaf operation. Implementations are monomorphized at the selected
/// generated slot; no erased callback crosses this boundary.
pub trait Visit<S, Set> {
    type Output;

    fn visit<'a, P>(
        self,
        target: path::Target<'a>,
        path_end: usize,
        captures: <<<P::Route as route::Spec>::Request as route::Schema>::RawParams as route::RawParams>::Captures,
    ) -> Self::Output
    where
        P: Slot<S, Set = Set>,
        P::Route: Sync<S>;
}

/// Route-type operation selected by a trusted generated route index.
pub trait Each<S, Set> {
    type Output;

    fn each<P>(self) -> Self::Output
    where
        P: Slot<S, Set = Set>,
        P::Route: Sync<S>;
}

/// A state-borrowing decoder backed by one generated route table.
pub trait Router: decode::Decode {
    type State;
    type Table: route::raw::Table<Self::State>;

    fn state(&self) -> &Self::State;
}
