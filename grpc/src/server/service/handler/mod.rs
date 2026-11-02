//! Handler contracts and typed adapters.

use o3::buffer;

use crate::{
    message, metadata,
    protocol::request,
    server::service::{
        Request, Response, Stream,
        reply::{Reply, Write},
        timers,
    },
};

pub mod modes;
pub mod output;
pub(in crate::server) mod unary;

/// Synchronous callbacks on the owning driver thread.
/// Return promptly: blocking a callback also stalls that worker's other connections.
pub trait Handler<C = ()> {
    type Call: Default;
    type Timer: timers::Policy;

    fn start<W: Write>(
        &mut self,
        _: &mut C,
        _: &mut Stream,
        _: &request::Head,
        _: &mut Reply<'_, W>,
    ) -> Start<Self::Call> {
        Start::buffered(Self::Call::default())
    }

    fn message<S: buffer::raw::Segment, W: Write>(
        &mut self,
        _: &mut C,
        _: &mut Self::Call,
        _: &mut Stream,
        _: message::Frame<S>,
        reply: &mut Reply<'_, W>,
    ) {
        reply.finish_with(crate::Status::from_code(crate::Code::Internal));
    }

    fn trailers<W: Write>(
        &mut self,
        _: &mut C,
        _: &mut Self::Call,
        _: &mut Stream,
        _: metadata::Decoded,
        reply: &mut Reply<'_, W>,
    ) {
        reply.finish_with(crate::Status::from_code(crate::Code::Internal));
    }

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        context: &mut C,
        call: &mut Self::Call,
        request: Request<'_, S>,
        response: &mut Response<'_, W>,
    ) -> RequestState;

    fn output<W: Write>(
        &mut self,
        _: &mut C,
        _: &mut Self::Call,
        _: &mut timers::Output<'_, '_, '_, Self::Timer>,
        reply: &mut Reply<'_, W>,
    ) -> OutputState {
        reply.finish_with(crate::Status::from_code(crate::Code::Internal));
        OutputState::Complete
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Production {
    Idle,
    Ready,
}

const _: () = assert!(core::mem::size_of::<Production>() == core::mem::size_of::<bool>());
const _: () = assert!(core::mem::align_of::<Production>() == core::mem::align_of::<bool>());

pub struct Start<T> {
    pub call: T,
    pub mode: StreamMode,
    pub output: Production,
}

impl<T> Start<T> {
    pub fn buffered(call: T) -> Self {
        Self {
            call,
            mode: StreamMode::Buffered,
            output: Production::Idle,
        }
    }

    pub fn live(call: T) -> Self {
        Self {
            call,
            mode: StreamMode::Live,
            output: Production::Idle,
        }
    }

    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> Start<U> {
        Start {
            call: map(self.call),
            mode: self.mode,
            output: self.output,
        }
    }

    pub fn producing(mut self) -> Self {
        self.output = Production::Ready;
        self
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum RequestState {
    Complete,
    Produce,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum OutputState {
    Produced,
    Blocked,
    Pending,
    Complete,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum StreamMode {
    Buffered,
    Live,
}
