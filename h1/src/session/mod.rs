//! HTTP/1 routing contracts and connection ownership.
use core::{convert::Infallible, pin};
use o3::buffer::{Complete as _, hold, write::Slice};

use sark_core::life::{self, exchange, sequence};
use sark_http::{
    h1::field::expect,
    payload::{fixed, owned},
};

use crate::{
    switch,
    wire::{head, output, parsing},
};

mod conn;
mod drive;
mod ingress;

pub use conn::Conn;
pub use drive::{Flow, Turn};
pub use ingress::Ingress;

/// Framing proof attached to a route suspended for body input.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BodyAwait {
    consumed: usize,
}

impl BodyAwait {
    pub(crate) const fn new(used: usize) -> Self {
        Self { consumed: used }
    }
}

pub enum Consumption {
    Buffered(usize),
    Discard(Discarded),
}

const _: () = assert!(core::mem::size_of::<Consumption>() == 3 * core::mem::size_of::<usize>());

/// Validated head and body bytes discarded from one request frame.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Discarded {
    head: usize,
    body: usize,
}

impl Discarded {
    pub(crate) const fn new(head: usize, body: usize) -> Self {
        Self { head, body }
    }
}

const _: () = assert!(core::mem::size_of::<Discarded>() == 2 * core::mem::size_of::<usize>());

pub enum Outcome<'d, D = Infallible> {
    Complete {
        consumption: Consumption,
        response: output::Complete<'d>,
        connection: head::Connection,
    },
    /// Switching response paired with its affine raw owner.
    Switch {
        next: switch::Transition<D>,
        consumed: usize,
        response: output::Switch<'d>,
    },
    Streamed {
        consumed: usize,
        close: head::Connection,
    },
    Park {
        consumed: usize,
        close: head::Connection,
    },
    Close(fixed::Body<'d>),
    Failed(crate::wire::Failure),
}

impl<D> Outcome<'_, D> {
    pub const fn park(consumed: usize, response: crate::wire::response::Context) -> Self {
        Self::Park {
            consumed,
            close: response.connection(),
        }
    }

    pub fn settle<const AT_HEAD: bool, C: Copy, M: sequence::Mode<C>>(
        &self,
        mode: &mut M,
        session: &mut M::Session,
        id: exchange::Id<C, u64>,
    ) {
        match self {
            Self::Complete { response, .. } => response.finish::<AT_HEAD, _, _>(mode, session, id),
            Self::Switch { .. } => {
                if AT_HEAD {
                    mode.observe(life::Event::InputFinished(id));
                }
                mode.commit(session, id, 0, hold::Charge::default());
                id.retire(mode, life::Outcome::Completed);
            }
            Self::Park { .. } | Self::Streamed { .. } => {
                if AT_HEAD {
                    mode.observe(life::Event::InputFinished(id));
                }
                if matches!(self, Self::Streamed { .. }) {
                    mode.commit(session, id, 0, hold::Charge::default());
                }
                mode.observe(life::Event::TaskAdmitted(id));
            }
            Self::Close(_) | Self::Failed(_) => {
                if AT_HEAD {
                    mode.reject(id);
                } else {
                    id.retire(mode, life::Outcome::Rejected);
                }
            }
        }
    }
}

/// Result of the only routing pass for a request head.
pub enum RouteOutcome<'d, P, D = Infallible> {
    Outcome(Outcome<'d, D>),
    AwaitBody {
        state: BodyAwait,
        expectation: expect::Decision,
        pending: P,
    },
}

impl<'d, P, D> From<Outcome<'d, D>> for RouteOutcome<'d, P, D> {
    fn from(ready: Outcome<'d, D>) -> Self {
        Self::Outcome(ready)
    }
}

/// Terminal body framing counts.
pub struct Complete {
    pub body_len: usize,
    pub consumed: usize,
}

/// One bounded step after request framing has completed.
pub enum BodyOutcome<'d, P, D = Infallible> {
    Complete(Outcome<'d, D>),
    Pending(P),
}

/// Static dispatch seam between the protocol and generated router.
pub trait Dispatch<'d, Runtime, Pending = (), Transitions: switch::Engine = switch::Disabled>:
    Sized
{
    type Ready;
    type Head;

    /// Receives the exact failure before its wire response is selected.
    fn failure(self: pin::Pin<&mut Self>, cause: &crate::wire::Failure) -> fixed::Body<'d> {
        cause.body()
    }

    fn reject_head(self: pin::Pin<&mut Self>, _: &mut Runtime) {}

    /// Settles a framing failure while ingress still owns its pending route.
    /// Implementations with route accounting must cancel it before resetting ingress.
    fn abort_body(
        self: pin::Pin<&mut Self>,
        conn: &mut Conn<'d, Runtime, Pending, Transitions, Self::Ready, Self::Head>,
        error: crate::wire::Failure,
    ) -> fixed::Body<'d> {
        conn.ingress.finish_frame();
        self.failure(&error)
    }

    fn try_consume(
        input: crate::Input<'_>,
        turn: &mut Turn<'_, '_, 'd, Runtime, Pending, Transitions, Self>,
        prior: usize,
    ) -> Flow;

    fn consume_line(
        source: parsing::Source,
        turn: &mut Turn<'_, '_, 'd, Runtime, Pending, Transitions, Self>,
        prior: usize,
    ) -> Flow {
        let source = source.freeze();
        Self::try_consume(crate::Input::Buffered(source.view()), turn, prior)
    }

    fn resume_head(
        pending: Self::Head,
        turn: &mut Turn<'_, '_, 'd, Runtime, Pending, Transitions, Self>,
    ) -> Flow;

    fn body_data(
        self: pin::Pin<&mut Self>,
        pending: Pending,
        body: owned::Segments<'d>,
        runtime: &mut Runtime,
    ) -> Result<Pending, fixed::Body<'d>>;

    /// Reports only work which can advance before another HTTP body byte arrives.
    fn has_body_work(pending: &Pending) -> bool;

    /// Advances one bounded turn without changing HTTP framing or supplying input.
    fn resume_pending(
        self: pin::Pin<&mut Self>,
        pending: Pending,
        runtime: &mut Runtime,
    ) -> Result<Pending, fixed::Body<'d>>;

    fn finish_body(
        self: pin::Pin<&mut Self>,
        pending: Pending,
        complete: Complete,
        write: Slice<'_, &mut usize>,
        runtime: &mut Runtime,
    ) -> BodyOutcome<'d, Self::Ready, Transitions::Raw>;

    fn resume_body(
        self: pin::Pin<&mut Self>,
        ready: Self::Ready,
        write: Slice<'_, &mut usize>,
        runtime: &mut Runtime,
    ) -> BodyOutcome<'d, Self::Ready, Transitions::Raw>;
}
