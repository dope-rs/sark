use std::{convert::Infallible, pin, task};

use dope::core::driver::schedule::clock::timer;
use dope_fiber::{abi, context};
use o3::buffer::resident;
use sark_h2::stream::Id;

use crate::{metadata, protocol::request};

pub(in crate::server) mod exchange;

/// One decoded request-side event delivered to a streaming task.
pub enum Input<T> {
    Message(T),
    Closed(metadata::Decoded),
}

/// Final status and trailers produced by a streaming task.
pub struct Finish {
    pub trailers: metadata::Metadata,
    pub status: crate::Status,
}

impl Finish {
    pub fn ok() -> Self {
        Self {
            trailers: metadata::Metadata::new(),
            status: crate::Status::ok(),
        }
    }
}

pub struct Request {
    pub stream_id: Id,
    pub head: request::Head,
}

pub struct Call<T> {
    pub metadata: metadata::Metadata,
    pub task: T,
}

impl<T> Call<T> {
    pub fn new(task: T) -> Self {
        Self {
            metadata: metadata::Metadata::new(),
            task,
        }
    }
}

pub struct Context<'poll, 'd, Request, Response, Codec = ()> {
    exchange: &'poll exchange::Exchange<Request, Response, Codec>,
    timer: timer::Timer<'d>,
}

impl<'poll, 'd, Request, Response, Codec> Context<'poll, 'd, Request, Response, Codec> {
    pub(in crate::server) const fn new(
        exchange: &'poll exchange::Exchange<Request, Response, Codec>,
        timer: timer::Timer<'d>,
    ) -> Self {
        Self { exchange, timer }
    }

    pub fn poll_receive(&mut self) -> task::Poll<Input<Request>> {
        self.exchange.receive()
    }

    pub fn try_send(&mut self, message: Response) -> Result<(), Response> {
        self.exchange.send(message)
    }

    pub const fn timer(&self) -> timer::Timer<'d> {
        self.timer
    }
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    Unary,
    Client,
    Server,
    Bidi,
}

pub trait Route {
    fn shape(&self) -> Shape;
}

impl Route for () {
    fn shape(&self) -> Shape {
        Shape::Bidi
    }
}

const _: () = assert!(size_of::<Shape>() == 1);

pub trait Handler {
    type Request<'d>: 'd;
    type Response;
    type Codec: for<'d> crate::Codec<
            Encode = Self::Response,
            Owned<resident::Shared<'d>> = Self::Request<'d>,
        > + Default;
    type Route: Route;
    type Task<'life, 'd>: Work<'d, Self::Request<'d>, Self::Response, Self::Codec> + 'life
    where
        'd: 'life,
        Self: 'life;

    fn codec(&self, request: &Request) -> Result<(Self::Codec, Self::Route), crate::Status>;

    fn stream<'life, 'd>(
        &'life self,
        request: Request,
        route: Self::Route,
    ) -> Call<Self::Task<'life, 'd>>
    where
        'd: 'life;
}

/// A streaming task polled with access tied to the current scheduler borrow.
pub trait Work<'d, Request, Response, Codec>: Sized {
    fn poll(
        self: pin::Pin<&mut Self>,
        context: Context<'_, 'd, Request, Response, Codec>,
        fiber: context::Admission<'_, '_, 'd>,
    ) -> task::Poll<Finish>;
}

/// Scheduler-owned task and its exact request/response exchange.
#[doc(hidden)]
#[pin_project::pin_project]
pub struct Fiber<'d, T, Request, Response, Codec> {
    #[pin]
    task: T,
    exchange: exchange::Exchange<Request, Response, Codec>,
    timer: timer::Timer<'d>,
}

impl<'d, T, Request, Response, Codec> Fiber<'d, T, Request, Response, Codec> {
    pub(in crate::server) const fn new(
        task: T,
        codec: Codec,
        shape: Shape,
        timer: timer::Timer<'d>,
    ) -> Self {
        Self {
            task,
            exchange: exchange::Exchange::new(codec, shape),
            timer,
        }
    }

    pub(in crate::server) fn exchange(
        self: pin::Pin<&mut Self>,
    ) -> &mut exchange::Exchange<Request, Response, Codec> {
        self.project().exchange
    }
}

impl<'d, T, Request, Response, Codec> abi::Fiber<'d> for Fiber<'d, T, Request, Response, Codec>
where
    T: Work<'d, Request, Response, Codec>,
{
    type Output = Infallible;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (fiber, admit) = call.into_admit();
        let this = fiber.project();
        if matches!(this.exchange.phase, exchange::Phase::Running) {
            let io = Context::new(this.exchange, *this.timer);
            if let task::Poll::Ready(finish) = T::poll(this.task, io, admit) {
                this.exchange.phase = exchange::Phase::Finished(finish);
                this.exchange.wake();
            }
        }
        task::Poll::Pending
    }
}
