use std::{cell::Cell, pin::Pin, task::Poll};

use dope::{
    core::driver::{route::KeyTag, schedule::clock::timer::Timer},
    manifold::network::listener::conn,
    net::{
        link::pool::send,
        wire::{Identity, terminal},
    },
};
use dope_fiber::{abi, context, net::server};
use o3::buffer::{Source as _, write::consume::Consume as _};
use sark_grpc::{
    Code, Status,
    encode::Prost,
    server::service::{
        handler::modes,
        task::{stream, unary},
    },
};
use sark_h2::{
    conn::CLIENT_PREFACE,
    frame::{Ack, controls::Settings, numbers::ParseError, payload::Payload},
};

pub const ZERO_WINDOW: &[u8] = &[
    0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 4, 1, 0, 0, 0, 0,
];

pub const DEFAULT_SETTINGS: &[u8] = &[
    0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 4, 1, 0, 0, 0, 0,
];

pub const CANCELLATION_RESPONSES: &[u8] = b"\
\0\0\x14\x01\x04\0\0\0\x01\x88\x0f\x10\x10application/grpc\
\0\0\x06\0\0\0\0\0\x01\0\0\0\0\x01a\
\0\0\x14\x01\x04\0\0\0\x03\x88\x0f\x10\x10application/grpc\
\0\0\x06\0\0\0\0\0\x03\0\0\0\0\x01b\
\0\0\x14\x01\x04\0\0\0\x05\x88\x0f\x10\x10application/grpc\
\0\0\x07\0\0\0\0\0\x05\0\0\0\0\x02ok\
\0\0\x0f\x01\x05\0\0\0\x05\0\x0bgrpc-status\x010\
\0\0\x08\x06\0\0\0\0\0cancels!";

// Stream 3 ends with a valid trailers-only gRPC error; its request half stays open.
pub const UNIMPLEMENTED_RESPONSE: &[u8] = b"\
\0\0\x24\x01\x05\0\0\0\x03\x88\x0f\x10\x10application/grpc\
\0\x0bgrpc-status\x02\x31\x32";

pub const SPLIT_RETAINED_RESPONSE: &[u8] = b"\
\0\0\x23\x01\x05\0\0\0\x01\x88\x0f\x10\x10application/grpc\
\0\x0bgrpc-status\x010";

pub const SPLIT_REUSED_RESPONSE: &[u8] = b"\
\0\0\x23\x01\x05\0\0\0\x05\x88\x0f\x10\x10application/grpc\
\0\x0bgrpc-status\x010";

type Io<'scope, 'w, 'd> = dope_fiber::net::Io<'scope, 'w, 'd, Identity, conn::Id<'d, KeyTag<1>>>;

pub struct Peer<'scope, 'w: 'scope, 'd: 'w> {
    io: Io<'scope, 'w, 'd>,
    input: Vec<u8>,
}

impl<'scope, 'w: 'scope, 'd: 'w> Peer<'scope, 'w, 'd> {
    pub fn accept(
        listener: server::Handle<'scope, 'w, 'd, Identity, KeyTag<1>>,
        settings: &'d [u8],
    ) -> impl abi::Fiber<'d, Output = Self> + 'scope {
        dope_fiber::fiber!('_ => async move {
            let io = listener.accept().await.unwrap_or_else(|error| match error {
                server::accept::Error::Wait(cause) => panic!("native accept waiter: {:?}", cause.cause()),
                server::accept::Error::Listener(cause) => panic!("native accept: {cause:?}"),
            });
            let mut peer = Self { io, input: Vec::new() };
            while peer.input.len() < CLIENT_PREFACE.len() {
                peer.read().await;
            }
            assert_eq!(&peer.input[..CLIENT_PREFACE.len()], CLIENT_PREFACE);
            peer.input.drain(..CLIENT_PREFACE.len());
            let ack = Settings::new(Ack::Complete, &[]).expect("empty acknowledgment");
            loop {
                match peer.next(|frame| match frame {
                    Payload::Settings(settings) => Some(settings == ack),
                    _ => None,
                }).await {
                    Some(true) => break,
                    Some(false) => peer.write(settings).await,
                    None => {}
                }
            }
            peer
        })
    }

    pub fn write(&mut self, bytes: &'d [u8]) -> impl abi::Fiber<'d, Output = ()> + '_ {
        use dope_fiber::net::error::Write;

        dope_fiber::fiber!('_ => async move {
            self.io.write_all(bytes).await.unwrap_or_else(|error| match error {
                Write::PortShuttingDown { .. } => panic!("peer write: port shutting down"),
                Write::ConnectionUnavailable { .. } => panic!("peer write: connection unavailable"),
                Write::ConnectionClosed { .. } => panic!("peer write: connection closed"),
                Write::ConnectionFailed { .. } => panic!("peer write: connection failed"),
                Write::Rejected(send::Rejected::ConnectionUnavailable(_)) => panic!("peer write: unavailable send"),
                Write::Rejected(send::Rejected::PendingUnavailable(_)) => panic!("peer write: unavailable pending send"),
                Write::Rejected(send::Rejected::Egress(cause)) => panic!("peer write capacity: {:?}", cause.cause),
                Write::Invalid(cause) => panic!("peer write load: {:?}", cause.error()),
                Write::Admission(cause) => panic!("peer write admission: {:?}", cause.cause()),
            });
        })
    }

    pub fn next<'a, T: 'a>(
        &'a mut self,
        consume: impl for<'frame> FnOnce(Payload<'frame>) -> T + 'a,
    ) -> impl abi::Fiber<'d, Output = T> + 'a {
        dope_fiber::fiber!('_ => async move {
            loop {
                match Payload::parse(&self.input) {
                    Ok((frame, used)) => {
                        let value = consume(frame);
                        self.input.drain(..used);
                        return value;
                    }
                    Err(ParseError::Need { .. }) => self.read().await,
                    Err(error) => panic!("client frame: {error:?}"),
                }
            }
        })
    }

    fn read(&mut self) -> impl abi::Fiber<'d, Output = ()> + '_ {
        dope_fiber::fiber!('_ => async move {
            let mut input = self.io.read().await.unwrap_or_else(|error| {
                panic!("client read: {:?}", error.map_ref(terminal::Error::causes))
            }).expect("client EOF");
            while !input.is_empty() {
                let bytes = input.as_ref();
                self.input.extend_from_slice(bytes);
                let length = bytes.len();
                input.try_consume(length).expect("client input credit");
            }
        })
    }
}

#[derive(Default)]
pub struct Facts {
    pub polls: Cell<usize>,
    pub messages: Cell<usize>,
    pub closed: Cell<bool>,
    pub drops: Cell<usize>,
}

pub struct Handler<'a>(pub &'a Facts);

pub struct Task<'a> {
    facts: &'a Facts,
    yielded: bool,
}

impl unary::Handler for Handler<'_> {
    type Request<'d> = ();
    type Response = ();
    type Codec = Prost<()>;
    type Task<'life, 'd>
        = Task<'life>
    where
        'd: 'life,
        Self: 'life;

    fn unary<'life, 'd>(&'life self, _: Timer<'d>, _: modes::Request<()>) -> Self::Task<'life, 'd>
    where
        'd: 'life,
    {
        self.0.messages.set(self.0.messages.get() + 1);
        Task {
            facts: self.0,
            yielded: false,
        }
    }
}

impl<'d> abi::Fiber<'d> for Task<'_> {
    type Output = modes::Response<()>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (task, context) = call.into_parts();
        let task = task.get_mut();
        task.facts.polls.set(task.facts.polls.get() + 1);
        if task.yielded {
            Poll::Ready(modes::Response::new(()))
        } else {
            task.yielded = true;
            context.as_ref().completion().wake();
            Poll::Pending
        }
    }
}

impl stream::Handler for Handler<'_> {
    type Request<'d> = ();
    type Response = ();
    type Codec = Prost<()>;
    type Route = ();
    type Task<'life, 'd>
        = Task<'life>
    where
        'd: 'life,
        Self: 'life;

    fn codec(&self, _: &stream::Request) -> Result<(Self::Codec, Self::Route), Status> {
        Ok((Prost::new(), ()))
    }

    fn stream<'life, 'd>(
        &'life self,
        _: stream::Request,
        (): (),
    ) -> stream::Call<Self::Task<'life, 'd>>
    where
        'd: 'life,
    {
        stream::Call::new(Task {
            facts: self.0,
            yielded: false,
        })
    }
}

impl<'d> stream::Work<'d, (), (), Prost<()>> for Task<'_> {
    fn poll(
        self: Pin<&mut Self>,
        mut context: stream::Context<'_, 'd, (), (), Prost<()>>,
        _: context::Admission<'_, '_, 'd>,
    ) -> Poll<stream::Finish> {
        let facts = self.facts;
        facts.polls.set(facts.polls.get() + 1);
        match context.poll_receive() {
            Poll::Pending => Poll::Pending,
            Poll::Ready(stream::Input::Message(())) => {
                facts.messages.set(facts.messages.get() + 1);
                match context.try_send(()) {
                    Ok(()) => Poll::Pending,
                    Err(()) => Poll::Ready(stream::Finish {
                        status: Status::from_code(Code::Internal),
                        ..stream::Finish::ok()
                    }),
                }
            }
            Poll::Ready(stream::Input::Closed(_)) => {
                facts.closed.set(true);
                Poll::Ready(stream::Finish::ok())
            }
        }
    }
}

impl Drop for Task<'_> {
    fn drop(&mut self) {
        self.facts.drops.set(self.facts.drops.get() + 1);
    }
}

#[derive(Default)]
pub struct DueFacts {
    pub polls: [Cell<usize>; 2],
    pub drops: [Cell<usize>; 2],
    pub overlap: Cell<bool>,
}

pub struct DueHandler<'a>(pub &'a DueFacts);

pub struct DueTask<'a> {
    facts: &'a DueFacts,
    index: usize,
}

impl unary::Handler for DueHandler<'_> {
    type Request<'d> = ();
    type Response = ();
    type Codec = Prost<()>;
    type Task<'life, 'd>
        = DueTask<'life>
    where
        'd: 'life,
        Self: 'life;

    fn unary<'life, 'd>(
        &'life self,
        _: Timer<'d>,
        request: modes::Request<()>,
    ) -> Self::Task<'life, 'd>
    where
        'd: 'life,
    {
        DueTask {
            facts: self.0,
            index: usize::from(request.head.path() == b"/deadline/live"),
        }
    }
}

impl<'d> abi::Fiber<'d> for DueTask<'_> {
    type Output = modes::Response<()>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (task, context) = call.into_parts();
        let task = task.get_mut();
        task.facts.polls[task.index].set(task.facts.polls[task.index].get() + 1);
        if task.index == 1 {
            if task.facts.drops[0].get() == 1 {
                return Poll::Ready(modes::Response::new(()));
            }
            task.facts.overlap.set(true);
        }
        context.as_ref().completion().wake();
        Poll::Pending
    }
}

impl Drop for DueTask<'_> {
    fn drop(&mut self) {
        self.facts.drops[self.index].set(self.facts.drops[self.index].get() + 1);
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Sender {
    Unary,
    Stream,
    Borrowed,
}

/// Cancels the first notified sender before polling its admission again.
#[pin_project::pin_project]
pub struct CancelSender<F, S> {
    #[pin]
    wait: Option<abi::Task<F>>,
    cancel: bool,
    start: Option<S>,
}

impl<F, S> CancelSender<F, S> {
    pub fn new(wait: F, cancel: bool, start: S) -> Self {
        Self {
            wait: Some(abi::Task::new(wait)),
            cancel,
            start: Some(start),
        }
    }
}

impl<'d, F, S: FnOnce(), T> abi::Fiber<'d> for CancelSender<F, S>
where
    F: abi::Fiber<'d, Output = Result<T, sark_grpc::client::manifold::Error>>,
{
    type Output = Option<F::Output>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, admit) = call.into_admit();
        let mut this = this.project();
        if this.start.is_none() && *this.cancel {
            this.wait.set(None);
            return Poll::Ready(None);
        }
        let Some(wait) = this.wait.as_mut().as_pin_mut() else {
            return Poll::Pending;
        };
        let (poll, context) = admit.poll(wait);
        match poll {
            Poll::Ready(output) => {
                assert!(
                    this.start.is_none(),
                    "sender must first wait for an output owner: {:?}",
                    output.as_ref().err(),
                );
                Poll::Ready(Some(output))
            }
            Poll::Pending => {
                // A budget yield does not prove that the inner sender registered.
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
