use o3::depot::{arena::hash, capacity};
use sark_core::body::credit;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::manifold) struct Limits {
    pub(in crate::manifold) max_request_body_bytes: usize,
    pub(in crate::manifold) max_body_chunks: usize,
    pub(in crate::manifold) max_connection_body_bytes: usize,
    pub(in crate::manifold) max_connection_body_chunks: usize,
}

impl Limits {
    /// Per-request bound: one request cannot spend the whole connection budget.
    pub(in crate::manifold::requests) const fn request(self) -> credit::Limits {
        credit::Limits::new(self.max_request_body_bytes, self.max_body_chunks)
    }

    pub(in crate::manifold::requests) const fn connection(self) -> credit::Limits {
        credit::Limits::new(
            self.max_connection_body_bytes,
            self.max_connection_body_chunks,
        )
    }
}

#[derive(Clone, Copy)]
pub(in crate::manifold) struct Plan {
    requests: hash::Plan,
    chunk_capacity: capacity::Count,
    limits: Limits,
}

impl Plan {
    pub(in crate::manifold) const fn new(
        requests: hash::Plan,
        chunks: capacity::Count,
        limits: Limits,
    ) -> Self {
        Self {
            requests,
            chunk_capacity: chunks,
            limits,
        }
    }

    pub(in crate::manifold) const fn capacity(self) -> capacity::Count {
        self.requests.capacity()
    }

    pub(super) const fn into_parts(self) -> (hash::Plan, capacity::Count, Limits) {
        (self.requests, self.chunk_capacity, self.limits)
    }
}
