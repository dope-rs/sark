use std::{cell::Cell, collections::VecDeque, iter, time::Duration};

use dope_test::harness::{observe::VecCellExt as _, signals::LocalGateRef};
use o3::buffer;
use sark_grpc::{
    Code, Codec, Status, message, metadata,
    protocol::request,
    server::service::{
        self,
        handler::{
            self, Handler,
            modes::Request,
            output::{Bidi, BidiFn, BidiRequest, Poll, Response, Stream, StreamFn},
        },
        reply::{Reply, Write},
        timers,
    },
};

use crate::common::codec::Bytes;

pub type Observed = Cell<Vec<(Vec<u8>, Option<Vec<u8>>, Result<(), metadata::Error>)>>;

pub struct Backpressure<'a>(pub &'a Observed);

pub struct Failure<'a>(pub &'a Cell<Vec<Vec<u8>>>);

pub struct AfterMessage;

impl Handler for AfterMessage {
    type Call = ();
    type Timer = timers::Off;

    fn start<W: Write>(
        &mut self,
        _: &mut (),
        _: &mut service::Stream,
        head: &request::Head,
        reply: &mut Reply<'_, W>,
    ) -> handler::Start<()> {
        if head.path() != b"/failure/initial" {
            reply.push_message(b"sent".to_vec());
        }
        handler::Start::live(())
    }

    fn message<S: buffer::raw::Segment, W: Write>(
        &mut self,
        _: &mut (),
        _: &mut (),
        _: &mut service::Stream,
        _: message::Frame<S>,
        reply: &mut Reply<'_, W>,
    ) {
        if let Err(error) = reply.metadata_mut().push(b"x-late", b"invalid") {
            reply.finish_with(error.into());
        }
    }

    fn request<S: buffer::raw::Segment, W: Write>(
        &mut self,
        _: &mut (),
        _: &mut (),
        _: service::Request<'_, S>,
        _: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        handler::RequestState::Complete
    }
}

pub struct Echo;

pub struct Early;

pub struct Wake;

pub struct Timed<'a>(pub &'a Cell<Vec<Vec<u8>>>);

pub struct Replies<'a> {
    next: u8,
    dropped: LocalGateRef<'a>,
}

impl Iterator for Replies<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.next;
        if value == 3 {
            return None;
        }
        self.next += 1;
        Some(vec![value; 8])
    }
}

impl Drop for Replies<'_> {
    fn drop(&mut self) {
        self.dropped.hit();
    }
}

impl<'d> StreamFn<LocalGateRef<'d>> for Backpressure<'_> {
    type Response = Vec<u8>;
    type Stream = Replies<'d>;
    type Codec = Bytes;

    fn stream<'a, S: buffer::raw::Segment + 'a>(
        &mut self,
        context: &mut LocalGateRef<'d>,
        request: Request<<Self::Codec as Codec>::Decode<'a, S>>,
    ) -> Response<Self::Stream> {
        let mut response = Response::new(Replies {
            next: 0,
            dropped: *context,
        });
        let fields = response
            .metadata
            .push(b"x-owner-initial", b"retained")
            .and_then(|()| response.trailers.push(b"x-owner-trailer", b"retained"));
        self.0.push((
            request.body,
            request
                .head
                .metadata()
                .values(b"x-owner-request")
                .next()
                .map(<[u8]>::to_vec),
            fields,
        ));
        response
    }
}

impl StreamFn<()> for Failure<'_> {
    type Response = Vec<u8>;
    type Stream = iter::Empty<Vec<u8>>;
    type Codec = Bytes;

    fn stream<'a, S: buffer::raw::Segment + 'a>(
        &mut self,
        _: &mut (),
        request: Request<<Self::Codec as Codec>::Decode<'a, S>>,
    ) -> Response<Self::Stream> {
        self.0.push(request.body);
        let mut response = Response::new(iter::empty());
        response.status = Status::from_code(Code::InvalidArgument);
        response
    }
}

pub struct Chat<'a> {
    messages: VecDeque<Vec<u8>>,
    input_closed: bool,
    dropped: Option<LocalGateRef<'a>>,
}

impl Bidi<Bytes> for Chat<'_> {
    fn message<'a, S: buffer::raw::Segment + 'a>(
        &mut self,
        mut message: <Bytes as Codec>::Decode<'a, S>,
    ) {
        message.reverse();
        self.messages.push_back(message);
    }

    fn trailers(&mut self, _: metadata::Decoded) {
        self.input_closed = true;
    }
}

impl Stream for Chat<'_> {
    type Response = Vec<u8>;
    type Timer = timers::Off;

    fn poll(&mut self, _: &mut timers::Output<'_, '_, '_, Self::Timer>) -> Poll<Self::Response> {
        match self.messages.pop_front() {
            Some(message) => Poll::Message(message),
            None if self.input_closed => Poll::Complete,
            None => Poll::Pending,
        }
    }
}

impl Drop for Chat<'_> {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped {
            dropped.hit();
        }
    }
}

impl<'d> BidiFn<Option<LocalGateRef<'d>>> for Echo {
    type Response = Vec<u8>;
    type Stream = Chat<'d>;
    type Codec = Bytes;

    fn bidi(
        &mut self,
        context: &mut Option<LocalGateRef<'d>>,
        _: BidiRequest<'_>,
    ) -> Response<Self::Stream> {
        Response::new(Chat {
            messages: VecDeque::new(),
            input_closed: false,
            dropped: *context,
        })
    }
}

pub struct Terminal<'a> {
    dropped: LocalGateRef<'a>,
    ingress: &'a Cell<usize>,
    polls: &'a Cell<usize>,
    error: &'a Cell<Option<timers::Error>>,
}

impl Bidi<Bytes> for Terminal<'_> {
    fn message<'a, S: buffer::raw::Segment + 'a>(&mut self, _: <Bytes as Codec>::Decode<'a, S>) {
        self.ingress.set(self.ingress.get() + 1);
    }

    fn trailers(&mut self, _: metadata::Decoded) {
        self.ingress.set(self.ingress.get() + 1);
    }
}

impl Stream for Terminal<'_> {
    type Response = Vec<u8>;
    type Timer = timers::On;

    fn poll(
        &mut self,
        output: &mut timers::Output<'_, '_, '_, Self::Timer>,
    ) -> Poll<Self::Response> {
        self.polls.set(self.polls.get() + 1);
        if let Err(error) = output.defer_for(Duration::from_millis(20)) {
            self.error.set(self.error.get().or(Some(error)));
        }
        Poll::Complete
    }
}

impl Drop for Terminal<'_> {
    fn drop(&mut self) {
        self.dropped.hit();
    }
}

impl<'d> BidiFn<BidiCounts<'d>> for Early {
    type Response = Vec<u8>;
    type Stream = Terminal<'d>;
    type Codec = Bytes;

    fn bidi(&mut self, context: &mut BidiCounts<'d>, _: BidiRequest<'_>) -> Response<Self::Stream> {
        Response::new(Terminal {
            dropped: context.dropped,
            ingress: context.ingress,
            polls: context.polls,
            error: context.error,
        })
    }
}

pub struct BidiCounts<'a> {
    dropped: LocalGateRef<'a>,
    ingress: &'a Cell<usize>,
    polls: &'a Cell<usize>,
    error: &'a Cell<Option<timers::Error>>,
}

impl<'a> BidiCounts<'a> {
    pub fn new(
        dropped: LocalGateRef<'a>,
        ingress: &'a Cell<usize>,
        polls: &'a Cell<usize>,
        error: &'a Cell<Option<timers::Error>>,
    ) -> Self {
        Self {
            dropped,
            ingress,
            polls,
            error,
        }
    }
}

pub struct Counts<'a> {
    dropped: LocalGateRef<'a>,
    polls: &'a Cell<usize>,
    error: &'a Cell<Option<timers::Error>>,
}

impl<'a> Counts<'a> {
    pub fn new(
        dropped: LocalGateRef<'a>,
        polls: &'a Cell<usize>,
        error: &'a Cell<Option<timers::Error>>,
    ) -> Self {
        Self {
            dropped,
            polls,
            error,
        }
    }
}

pub struct Deferred<'a> {
    stage: u8,
    dropped: LocalGateRef<'a>,
    polls: &'a Cell<usize>,
    error: &'a Cell<Option<timers::Error>>,
}

impl<'a> Deferred<'a> {
    fn new(context: &Counts<'a>) -> Self {
        Self {
            stage: 0,
            dropped: context.dropped,
            polls: context.polls,
            error: context.error,
        }
    }
}

pub struct BidiDeferred<'a> {
    ingress: &'a Cell<usize>,
    output: Deferred<'a>,
}

impl Bidi<Bytes> for BidiDeferred<'_> {
    fn message<'a, S: buffer::raw::Segment + 'a>(&mut self, _: <Bytes as Codec>::Decode<'a, S>) {
        self.ingress.set(self.ingress.get() + 1);
    }

    fn trailers(&mut self, _: metadata::Decoded) {
        self.ingress.set(self.ingress.get() + 1);
    }
}

impl Stream for BidiDeferred<'_> {
    type Response = Vec<u8>;
    type Timer = timers::On;

    fn poll(
        &mut self,
        output: &mut timers::Output<'_, '_, '_, Self::Timer>,
    ) -> Poll<Self::Response> {
        self.output.poll(output)
    }
}

impl Stream for Deferred<'_> {
    type Response = Vec<u8>;
    type Timer = timers::On;

    fn poll(
        &mut self,
        output: &mut timers::Output<'_, '_, '_, Self::Timer>,
    ) -> Poll<Self::Response> {
        self.polls.set(self.polls.get() + 1);
        match self.stage {
            0 => {
                self.stage = 1;
                for _ in 0..1_024 {
                    if let Err(error) = output.defer_for(Duration::from_millis(10)) {
                        self.error.set(self.error.get().or(Some(error)));
                        return Poll::Complete;
                    }
                }
                Poll::Pending
            }
            1 => {
                self.stage = 2;
                Poll::Message(b"wake".to_vec())
            }
            _ => Poll::Complete,
        }
    }
}

impl Drop for Deferred<'_> {
    fn drop(&mut self) {
        self.dropped.hit();
    }
}

impl<'d> BidiFn<BidiCounts<'d>> for Wake {
    type Response = Vec<u8>;
    type Stream = BidiDeferred<'d>;
    type Codec = Bytes;

    fn bidi(&mut self, context: &mut BidiCounts<'d>, _: BidiRequest<'_>) -> Response<Self::Stream> {
        Response::new(BidiDeferred {
            ingress: context.ingress,
            output: Deferred {
                stage: 0,
                dropped: context.dropped,
                polls: context.polls,
                error: context.error,
            },
        })
    }
}

impl<'d> StreamFn<Counts<'d>> for Timed<'_> {
    type Response = Vec<u8>;
    type Stream = Deferred<'d>;
    type Codec = Bytes;

    fn stream<'a, S: buffer::raw::Segment + 'a>(
        &mut self,
        context: &mut Counts<'d>,
        request: Request<<Self::Codec as Codec>::Decode<'a, S>>,
    ) -> Response<Self::Stream> {
        self.0.push(request.body);
        Response::new(Deferred::new(context))
    }
}
