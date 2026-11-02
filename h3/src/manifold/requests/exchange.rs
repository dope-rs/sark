use sark_core::body::{self, credit};

use crate::manifold::requests::{self, task};

pub(in crate::manifold) enum Reset {
    Missing,
    Cancelled,
}

pub(in crate::manifold) enum Termination<'d, P> {
    Retained,
    Removed,
    Completed(task::Buffered<'d, P>),
    InputFinished,
    Deferred,
    Cancelled,
}

pub(in crate::manifold) enum Error {
    Request(requests::Error),
    Release {
        body: body::Error,
        release: credit::Error,
    },
}

pub(in crate::manifold) enum Stop<T> {
    Ignored,
    Exchange(usize),
    Task(T),
}

pub(in crate::manifold) enum Drained<T, O: credit::Origin = credit::raw::Stored> {
    Task(T),
    Exchange { pending: Option<credit::Release<O>> },
}
