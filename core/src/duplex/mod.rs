//! Protocol-neutral contracts for accepted byte streams.
use core::convert::Infallible;
type Opened<S, B, E> = Result<(S, B), (OpenError<E>, B)>;
use sark_http::{field::metadata::block, payload::owned};

pub mod admit;
pub mod input;
pub mod machine;

use crate::{
    decode,
    decode::{exec, request::Admitted},
    request, route,
};

/// Response-shape marker for a route which establishes a duplex byte stream.
pub enum Shape {}

/// Failure before a carrier committed its successful establishment response.
#[repr(u8)]
pub enum OpenError<E> {
    InvalidRequest(E),
    Length,
    Unsupported,
}

/// Directional disposition causally adjacent to one session action.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Directive {
    Wait,
    Activate,
    FinishOutput,
    FinishSession,
}

#[must_use = "return this output action to the duplex machine"]
pub struct Action<'d> {
    output: Option<owned::Segments<'d>>,
    directive: Directive,
}

impl<'d> Action<'d> {
    pub const fn idle() -> Self {
        Self {
            output: None,
            directive: Directive::Wait,
        }
    }

    pub fn send(output: impl Into<owned::Segments<'d>>) -> Self {
        Self {
            output: Some(output.into()),
            directive: Directive::Wait,
        }
    }

    pub const fn end() -> Self {
        Self {
            output: None,
            directive: Directive::FinishOutput,
        }
    }

    pub fn send_end(output: impl Into<owned::Segments<'d>>) -> Self {
        Self {
            output: Some(output.into()),
            directive: Directive::FinishOutput,
        }
    }

    pub const fn close() -> Self {
        Self {
            output: None,
            directive: Directive::FinishSession,
        }
    }

    pub fn send_close(output: impl Into<owned::Segments<'d>>) -> Self {
        Self {
            output: Some(output.into()),
            directive: Directive::FinishSession,
        }
    }

    pub const fn activate(mut self) -> Self {
        if matches!(self.directive, Directive::Wait | Directive::Activate) {
            self.directive = Directive::Activate;
        }
        self
    }

    fn into_parts(self) -> (Option<owned::Segments<'d>>, Directive) {
        (self.output, self.directive)
    }
}

/// Established byte-stream owner retained without type erasure.
pub trait Session<'d> {
    fn receive(
        &mut self,
        input: owned::Segments<'d>,
        output: &mut [u8],
        scratch: &mut Vec<u8>,
    ) -> (usize, Action<'d>);

    /// Observes the peer's orderly end of the input direction exactly once.
    fn finish(&mut self, output: &mut [u8], scratch: &mut Vec<u8>) -> (usize, Action<'d>);

    fn activate(&mut self, output: &mut [u8], scratch: &mut Vec<u8>) -> (usize, Action<'d>);
}

/// Uninhabited session used by generated applications with no duplex routes.
pub enum Never {}

impl<'d> Session<'d> for Never {
    fn receive(
        &mut self,
        _: owned::Segments<'d>,
        _: &mut [u8],
        _: &mut Vec<u8>,
    ) -> (usize, Action<'d>) {
        match *self {}
    }

    fn finish(&mut self, _: &mut [u8], _: &mut Vec<u8>) -> (usize, Action<'d>) {
        match *self {}
    }

    fn activate(&mut self, _: &mut [u8], _: &mut Vec<u8>) -> (usize, Action<'d>) {
        match *self {}
    }
}

/// Exact duplex constructor consumed by multiplexed carriers.
pub trait Application<'d>: decode::Decode {
    type Duplex: Session<'d>;

    /// Extended CONNECT capability of the route language.
    const CONNECT: block::Connect = block::Connect::Disabled;

    fn try_open<B>(
        &self,
        req: Admitted<exec::Duplex, Self::Prepared, B>,
    ) -> Opened<Self::Duplex, B, Self::Error<'d>>;
}

/// Synchronous admission whose session depends only on the state lifetime.
pub trait Route<'state, State>: route::Spec<Kind = Shape> {
    type Session: 'state;

    fn open<'req, 'd>(
        params: <Self::Request as route::Project>::Params<'req>,
        req: &request::View<'req, 'd, <Self::Request as route::Schema>::Frame>,
        head: <Self::Request as route::Project>::Headers<'req>,
        body: <Self::Request as route::Project>::ParsedBody<'req, 'd>,
        state: &'state State,
    ) -> Self::Session
    where
        'd: 'req + 'state,
        Self::Session: Session<'d>;
}

const _: () = {
    assert!(core::mem::size_of::<OpenError<Infallible>>() == core::mem::size_of::<u8>());
    assert!(
        core::mem::size_of::<Action<'static>>()
            <= core::mem::size_of::<Option<owned::Segments<'static>>>()
                + core::mem::size_of::<usize>()
    );
};
