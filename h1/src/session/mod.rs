//! HTTP/1 routing contracts and connection ownership.
use core::{convert::Infallible, pin};

use o3::buffer::storage::range;
use sark_http::{
    h1::field::expect,
    payload::{fixed, owned},
};

use crate::{
    switch,
    wire::{InputError, head, output, parsing},
};

pub(crate) mod batch;
mod conn;
mod drain;
mod ingress;

pub use conn::Conn;
pub use ingress::Ingress;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NeedMore {
    Head,
    Line,
}

/// Framing proof attached to a route suspended for body input.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BodyAwait {
    consumed: usize,
}

impl BodyAwait {
    pub(crate) const fn new(used: usize) -> Self {
        Self { consumed: used }
    }

    pub(super) const fn consumed(self) -> usize {
        self.consumed
    }
}

pub enum Consumption {
    Buffered(Buffered),
    Discard(Discarded),
}

const _: () = assert!(core::mem::size_of::<Consumption>() == 3 * core::mem::size_of::<usize>());

/// Buffered request bytes consumed by a protocol switch.
#[derive(Clone, Copy)]
pub struct Buffered {
    used: usize,
}

impl Buffered {
    pub const fn new(used: usize) -> Self {
        Self { used }
    }

    pub(super) const fn used(self) -> usize {
        self.used
    }
}

const _: () = assert!(core::mem::size_of::<Buffered>() == core::mem::size_of::<usize>());

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

    pub(crate) const fn head(self) -> usize {
        self.head
    }

    pub(crate) const fn body(self) -> usize {
        self.body
    }
}

const _: () = assert!(core::mem::size_of::<Discarded>() == 2 * core::mem::size_of::<usize>());

/// Linear permission for exactly one synchronous dispatch.
pub struct Permit {
    proof: (),
}

impl Permit {
    pub const fn new() -> Self {
        Self { proof: () }
    }

    pub(crate) fn defer(self) {
        let Self { proof: () } = self;
    }

    pub(crate) fn commit(self) {
        let Self { proof: () } = self;
    }

    pub(crate) fn switch<'d, D>(
        self,
        next: switch::Transition<D>,
        buffered: Buffered,
        response: output::Switch<'d>,
    ) -> Outcome<'d, D> {
        let Self { proof: () } = self;
        Outcome::Switch {
            next,
            buffered,
            response,
        }
    }
}

impl Default for Permit {
    fn default() -> Self {
        Self::new()
    }
}

pub enum Outcome<'d, D = Infallible> {
    NeedMore {
        permit: Permit,
        state: NeedMore,
    },
    Complete {
        permit: Permit,
        consumption: Consumption,
        response: output::Complete<'d>,
        connection: head::Connection,
    },
    /// Switching response paired with its affine raw owner.
    Switch {
        next: switch::Transition<D>,
        buffered: Buffered,
        response: output::Switch<'d>,
    },
    Streamed {
        consumed: usize,
        written: usize,
        close: head::Connection,
    },
    Park {
        consumed: usize,
        close: head::Connection,
    },
    Close(fixed::Body<'d>),
    Failed(crate::wire::Failure),
}

/// Result of the only routing pass for a request head.
pub enum RouteOutcome<'d, P, D = Infallible, H = Infallible, Rejected = Infallible> {
    Outcome(Outcome<'d, D>),
    AwaitReject {
        permit: Permit,
        pending: Rejected,
    },
    AwaitHead {
        permit: Permit,
        pending: H,
    },
    AwaitBody {
        state: BodyAwait,
        expectation: expect::Decision,
        pending: P,
    },
}

impl<'d, P, D, H, Rejected> From<Outcome<'d, D>> for RouteOutcome<'d, P, D, H, Rejected> {
    fn from(ready: Outcome<'d, D>) -> Self {
        Self::Outcome(ready)
    }
}

impl<'d, P, D> RouteOutcome<'d, P, D> {
    pub fn with_head<H, Rejected>(self) -> RouteOutcome<'d, P, D, H, Rejected> {
        match self {
            Self::Outcome(outcome) => RouteOutcome::Outcome(outcome),
            Self::AwaitBody {
                state,
                expectation: expect,
                pending,
            } => RouteOutcome::AwaitBody {
                state,
                expectation: expect,
                pending,
            },
            Self::AwaitHead { pending, .. } => match pending {},
            Self::AwaitReject { pending, .. } => match pending {},
        }
    }
}

impl<'d, P, D, H, Rejected> RouteOutcome<'d, P, D, H, Rejected> {
    pub(crate) fn after_line(self, prior: usize) -> Self {
        match self {
            Self::AwaitHead { .. } | Self::AwaitReject { .. } => self,
            outcome => outcome.after_head(prior),
        }
    }

    pub(crate) fn after_head(mut self, prior: usize) -> Self {
        let result = match &mut self {
            Self::Outcome(Outcome::Complete {
                consumption: frame,
                connection: conn,
                ..
            }) => match frame {
                Consumption::Buffered(buffered)
                    if buffered.used == 0 && *conn == head::Connection::Close =>
                {
                    Ok(())
                }
                Consumption::Buffered(buffered) => subtract_head(&mut buffered.used, prior),
                Consumption::Discard(frame) => subtract_head(&mut frame.head, prior),
            },
            Self::Outcome(Outcome::Switch { buffered, .. }) => {
                subtract_head(&mut buffered.used, prior)
            }
            Self::Outcome(Outcome::Streamed { consumed, .. } | Outcome::Park { consumed, .. }) => {
                subtract_head(consumed, prior)
            }
            Self::AwaitBody { state, .. } => subtract_head(&mut state.consumed, prior),
            Self::Outcome(Outcome::Close(_) | Outcome::Failed(_)) => Ok(()),
            Self::Outcome(Outcome::NeedMore { .. })
            | Self::AwaitHead { .. }
            | Self::AwaitReject { .. } => Err(crate::wire::Failure::BadRequest),
        };
        match result {
            Ok(()) => self,
            Err(error) => Self::Outcome(Outcome::Failed(error)),
        }
    }
}

fn subtract_head(used: &mut usize, prior: usize) -> Result<(), crate::wire::Failure> {
    *used = used.checked_sub(prior).ok_or_else(|| {
        crate::wire::Failure::Input(InputError::Range(range::Error::new(0..prior, *used)))
    })?;
    Ok(())
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
pub trait Dispatch<'d, Runtime, Pending = (), Transitions: switch::Engine = switch::Disabled> {
    type Ready;
    type Head;

    /// Receives the exact failure before its wire response is selected.
    fn failure(self: pin::Pin<&mut Self>, cause: &crate::wire::Failure) -> fixed::Body<'d> {
        cause.body()
    }

    fn reject_line(
        self: pin::Pin<&mut Self>,
        _: Permit,
        _: &mut Conn<'d, Runtime, Pending, Transitions, Self::Ready, Self::Head>,
        error: parsing::LineError<'d>,
    ) -> fixed::Body<'d> {
        error.response(|cause| self.failure(cause))
    }

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
        self: pin::Pin<&mut Self>,
        permit: Permit,
        input: crate::Input<'_>,
        write: &mut [u8],
        conn: &mut Conn<'d, Runtime, Pending, Transitions, Self::Ready, Self::Head>,
    ) -> RouteOutcome<'d, Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>>;

    fn consume_line(
        self: pin::Pin<&mut Self>,
        permit: Permit,
        source: parsing::Source,
        write: &mut [u8],
        conn: &mut Conn<'d, Runtime, Pending, Transitions, Self::Ready, Self::Head>,
    ) -> RouteOutcome<'d, Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>>;

    fn resume_head(
        self: pin::Pin<&mut Self>,
        pending: Self::Head,
        permit: Permit,
        input: crate::Input<'_>,
        write: &mut [u8],
        conn: &mut Conn<'d, Runtime, Pending, Transitions, Self::Ready, Self::Head>,
    ) -> RouteOutcome<'d, Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>>;

    fn resume_error(
        self: pin::Pin<&mut Self>,
        pending: parsing::Rejected<'d>,
        permit: Permit,
        input: crate::Input<'_>,
        _: &mut [u8],
        _: &mut Conn<'d, Runtime, Pending, Transitions, Self::Ready, Self::Head>,
    ) -> RouteOutcome<'d, Pending, Transitions::Raw, Self::Head, parsing::Rejected<'d>> {
        pending.resume(input, permit)
    }

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
        permit: Permit,
        write: &mut [u8],
        runtime: &mut Runtime,
    ) -> BodyOutcome<'d, Self::Ready, Transitions::Raw>;

    fn resume_body(
        self: pin::Pin<&mut Self>,
        ready: Self::Ready,
        permit: Permit,
        write: &mut [u8],
        runtime: &mut Runtime,
    ) -> BodyOutcome<'d, Self::Ready, Transitions::Raw>;
}
