use std::{
    io,
    net::{SocketAddr, TcpListener},
    task::Poll,
};

use dope_fiber::{abi, context};
use sark_client::protocol::codec::output;

pub const CHUNKS: usize = 64;
pub const BODY: usize = CHUNKS * output::DATA_LIMIT;
pub const INVALID: &str = "http://example.invalid/";
pub const REJECTED: &str = "http://127.0.0.2/";
pub const OVERSIZED: [u8; 256] = [0; 256];

/// Completes the second pipelined response before resuming collection, proving
/// that the first response's entire event sequence is already resident. Each
/// full codec block guarantees a data event despite transport fragmentation.
#[pin_project::pin_project]
pub struct Queued<F, G, O> {
    #[pin]
    first: abi::Task<F>,
    #[pin]
    second: abi::Task<G>,
    output: Option<O>,
    started: bool,
    turns: usize,
}

impl<F, G, O> Queued<F, G, O> {
    pub fn new<'d>(first: F, second: G) -> Self
    where
        F: abi::Fiber<'d>,
        G: abi::Fiber<'d, Output = O>,
    {
        Self {
            first: abi::Task::new(first),
            second: abi::Task::new(second),
            output: None,
            started: false,
            turns: 0,
        }
    }
}

impl<'d, F: abi::Fiber<'d>, G: abi::Fiber<'d>> abi::Fiber<'d> for Queued<F, G, G::Output> {
    type Output = Result<(usize, F::Output, G::Output), F::Output>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, mut admit) = call.into_admit();
        let this = this.project();
        if let Some(second) = this.output.take() {
            while let context::Continue::Permit(_) = admit.context().as_ref().try_continue() {}
            *this.turns += 1;
            let (result, _) = admit.poll(this.first);
            return match result {
                Poll::Ready(first) => Poll::Ready(Ok((*this.turns, first, second))),
                Poll::Pending => {
                    *this.output = Some(second);
                    Poll::Pending
                }
            };
        }
        let context = if *this.started {
            let (result, context) = admit.poll(this.second);
            match result {
                Poll::Ready(second) => *this.output = Some(second),
                Poll::Pending => return Poll::Pending,
            }
            context
        } else {
            let (result, context) = admit.poll(this.first);
            if let Poll::Ready(first) = result {
                return Poll::Ready(Err(first));
            }
            *this.started = true;
            context
        };
        context.as_ref().get_ref().wake();
        Poll::Pending
    }
}

/// Starts the peer after a refused connection has woken the pending client.
#[pin_project::pin_project]
pub struct Reconnect<F> {
    #[pin]
    wait: abi::Task<F>,
    address: SocketAddr,
    listener: Option<TcpListener>,
    pending: bool,
}

impl<F> Reconnect<F> {
    pub fn new(wait: F, address: SocketAddr) -> Self {
        Self {
            wait: abi::Task::new(wait),
            address,
            listener: None,
            pending: false,
        }
    }
}

impl<'d, F: abi::Fiber<'d>> abi::Fiber<'d> for Reconnect<F> {
    type Output = io::Result<F::Output>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, admit) = call.into_admit();
        let this = this.project();
        let (poll, _) = admit.poll(this.wait);
        if let Poll::Ready(output) = poll {
            return Poll::Ready(Ok(output));
        }
        if *this.pending && this.listener.is_none() {
            *this.listener = Some(TcpListener::bind(*this.address)?);
        }
        *this.pending = true;
        Poll::Pending
    }
}

/// Cancels a notified task before its inner waiter can pass the wake onward.
#[pin_project::pin_project]
pub struct Cancel<F, C, S> {
    #[pin]
    wait: Option<abi::Task<F>>,
    cancel: C,
    start: Option<S>,
}

impl<F, C, S> Cancel<F, C, S> {
    pub fn new(wait: F, cancel: C, start: S) -> Self {
        Self {
            wait: Some(abi::Task::new(wait)),
            cancel,
            start: Some(start),
        }
    }
}

impl<'d, F: abi::Fiber<'d>, C: Fn() -> bool, S: FnOnce()> abi::Fiber<'d> for Cancel<F, C, S> {
    type Output = Option<F::Output>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, admit) = call.into_admit();
        let mut this = this.project();
        let Some(wait) = this.wait.as_mut().as_pin_mut() else {
            return Poll::Pending;
        };
        if this.start.is_none() && (this.cancel)() {
            this.wait.set(None);
            return Poll::Ready(None);
        }
        let (poll, context) = admit.poll(wait);
        match poll {
            Poll::Ready(output) => Poll::Ready(Some(output)),
            Poll::Pending => {
                // A budget yield does not prove that the inner waiter parked.
                if this.start.is_some()
                    && let context::Continue::Permit(_) = context.as_ref().try_continue()
                    && let Some(start) = this.start.take()
                {
                    start();
                }
                Poll::Pending
            }
        }
    }
}

pub const WAIT_PATHS: [&str; 2] = ["/first", "/waiting"];
pub const WAIT_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
