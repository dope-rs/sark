use sark_core::decode::{exec, request};
use sark_http::payload::{owned, response::Method};

use crate::manifold::requests::Id;

pub(in crate::manifold) enum Input<'d, P, O> {
    Buffered(request::Ready<exec::Task, P, owned::Segments<'d>>),
    Built { output: O },
}

pub(in crate::manifold) struct Buffered<'d, P> {
    pub(in crate::manifold) request: request::Ready<exec::Sync, P, owned::Segments<'d>>,
    pub(in crate::manifold) method: Method,
}

pub(in crate::manifold) enum Dispatch<E> {
    Idle,
    Active { key: Id },
    Rejected { key: Id, error: E },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::manifold) enum Close {
    Missing,
    Closed(Method),
}
