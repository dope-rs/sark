use std::marker;

use receive::policies;
use sark_core::{
    life::session::Storage,
    manifold::{
        client::{Connection, domain::Plan},
        stream::Boundary,
    },
};
use sark_h2::{conn, stream};

use crate::{
    client::{
        call::{self, response},
        receive, session,
    },
    metadata,
    protocol::section,
};

#[must_use = "a bidirectional stream must be drained or explicitly cancelled"]
pub struct Stream<'scope, K> {
    pub(in crate::client) response: response::Stream<'scope, K>,
}

impl<'scope, K: Copy> Stream<'scope, K> {
    pub fn split(self) -> (Send<'scope, K>, response::Stream<'scope, K>) {
        let send = Send {
            brand: self.response.brand,
            stream: self.response.stream,
        };
        (send, self.response)
    }
}

#[must_use = "a bidirectional send half must be finished or explicitly cancelled"]
pub struct Send<'scope, K> {
    pub(in crate::client) brand: marker::PhantomData<fn(&'scope ()) -> &'scope ()>,
    pub(in crate::client) stream: K,
}

pub trait Session: response::Session {
    fn open(
        &mut self,
        path: &[u8],
        peer: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<Self::Bidi, crate::Status>;

    fn try_send<D: crate::Codec>(
        &mut self,
        stream: &mut Self::Bidi,
        codec: &mut D,
        message: &D::Encode,
    ) -> call::Admission<()>;

    fn finish(&mut self, stream: Self::Bidi) -> Result<Self::Response, crate::Status>;

    fn try_half<D: crate::Codec>(
        &mut self,
        send: &mut Self::Send,
        codec: &mut D,
        message: &D::Encode,
    ) -> call::Admission<()>;

    fn finish_half(&mut self, send: Self::Send) -> Result<(), crate::Status>;
    fn cancel_half(&mut self, send: Self::Send);
    fn cancel(&mut self, stream: Self::Bidi);

    fn send_raw(&mut self, stream: &mut Self::Bidi, payload: &[u8]) -> Result<(), crate::Status>;

    fn try_poll(&mut self, stream: &Self::Bidi) -> Result<Option<Self::Event>, crate::Status>;
}

impl<'scope, P, C> Session for session::Session<'scope, P, C>
where
    C: Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    fn open(
        &mut self,
        path: &[u8],
        peer: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<Stream<'scope, P::Key>, crate::Status> {
        let started = self
            .state
            .start_reply::<receive::StreamKey>(path, peer, metadata)?;
        Ok(Stream {
            response: response::Stream {
                brand: self.brand,
                stream: started.stream,
                completion: started.completion,
            },
        })
    }

    fn try_send<D: crate::Codec>(
        &mut self,
        stream: &mut Stream<'scope, P::Key>,
        codec: &mut D,
        message: &D::Encode,
    ) -> call::Admission<()> {
        let mut policy = policies::Direct;
        self.state
            .send(stream.response.stream, codec, message, &mut policy)
    }

    fn finish(
        &mut self,
        stream: Stream<'scope, P::Key>,
    ) -> Result<response::Stream<'scope, P::Key>, crate::Status> {
        let mut policy = policies::Direct;
        self.state.finish(stream.response.stream, &mut policy)?;
        Ok(stream.response)
    }

    fn try_half<D: crate::Codec>(
        &mut self,
        send: &mut Send<'scope, P::Key>,
        codec: &mut D,
        message: &D::Encode,
    ) -> call::Admission<()> {
        let mut policy = policies::Direct;
        self.state.send(send.stream, codec, message, &mut policy)
    }

    fn finish_half(&mut self, send: Send<'scope, P::Key>) -> Result<(), crate::Status> {
        let mut policy = policies::Direct;
        self.state.finish(send.stream, &mut policy)
    }

    fn cancel_half(&mut self, send: Send<'scope, P::Key>) {
        let mut policy = policies::Direct;
        let _ = self.state.cancel(send.stream, &mut policy);
        self.state.refill(&mut policy);
    }

    fn cancel(&mut self, stream: Stream<'scope, P::Key>) {
        let mut policy = policies::Direct;
        self.state.cancel_feed(
            stream.response.stream,
            stream.response.completion,
            &mut policy,
        );
        self.state.refill(&mut policy);
    }

    fn send_raw(
        &mut self,
        stream: &mut Stream<'scope, P::Key>,
        payload: &[u8],
    ) -> Result<(), crate::Status> {
        let mut policy = policies::Direct;
        self.state
            .send_bytes(stream.response.stream, payload, Boundary::More, &mut policy)
    }

    fn try_poll(
        &mut self,
        stream: &Stream<'scope, P::Key>,
    ) -> Result<Option<receive::Response<C>>, crate::Status> {
        let mut policy = policies::Direct;
        self.state
            .poll_stream(stream.response.completion, &mut policy)
            .map(receive::Progress::into_option)
    }
}

impl<'scope, P, C> session::Session<'scope, P, C>
where
    C: Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    pub(in crate::client) fn try_bidi<'notify, D: crate::Codec>(
        &mut self,
        stream: &mut Stream<'scope, P::Key>,
        codec: &mut D,
        message: &D::Encode,
        done: &mut policies::Notify<'notify, '_, '_>,
    ) -> call::Admission<()> {
        self.state
            .send(stream.response.stream, codec, message, done)
    }

    pub(in crate::client) fn finish_bidi<'notify>(
        &mut self,
        stream: Stream<'scope, P::Key>,
        done: &mut policies::Notify<'notify, '_, '_>,
    ) -> Result<response::Stream<'scope, P::Key>, crate::Status> {
        self.state.finish(stream.response.stream, done)?;
        Ok(stream.response)
    }

    pub(in crate::client) fn try_send_half<'notify, D: crate::Codec>(
        &mut self,
        send: &mut Send<'scope, P::Key>,
        codec: &mut D,
        message: &D::Encode,
        done: &mut policies::Notify<'notify, '_, '_>,
    ) -> call::Admission<()> {
        self.state.send(send.stream, codec, message, done)
    }

    pub(in crate::client) fn finish_send<'notify>(
        &mut self,
        send: Send<'scope, P::Key>,
        done: &mut policies::Notify<'notify, '_, '_>,
    ) -> Result<(), crate::Status> {
        self.state.finish(send.stream, done)
    }
}
