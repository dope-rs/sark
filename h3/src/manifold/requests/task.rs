use sark_core::decode::{exec, request};
use sark_http::payload::{
    owned,
    response::{Method, dispatch::chunk::Chunk},
};

use crate::conn::io::replies::Plan;
use crate::manifold::{encoder, owners, requests::Id};

pub(in crate::manifold) enum Input<'d, P, O> {
    Buffered(request::Ready<exec::Task, P, owned::Segments<'d>>),
    Built { output: O },
}

pub(in crate::manifold) struct Buffered<'d, P> {
    pub(in crate::manifold) request: request::Ready<exec::Sync, P, owned::Segments<'d>>,
    pub(in crate::manifold) method: Method,
}

pub(in crate::manifold) enum Pending<'d, M: owners::Handoff> {
    // Initial response readiness has not been reported.
    Future,
    // Initial readiness was reported; no output owner is waiting for admission.
    Body,
    Response(encoder::Pending<'d, M>),
    Chunk(Plan<Chunk<'d>>),
    End,
}

pub(in crate::manifold) struct Active<'d, T, M: owners::Handoff> {
    pub(in crate::manifold) task: T,
    pub(in crate::manifold) pending: Pending<'d, M>,
}

pub(in crate::manifold) enum Owned<'d, T, M: owners::Handoff> {
    Active(Active<'d, T, M>),
    Response(encoder::Pending<'d, M>),
}

pub(in crate::manifold) enum Dispatch<E> {
    Idle,
    Active { key: Id },
    Rejected { key: Id, error: E },
}
