use std::marker;

use receive::policies;
use sark_core::{
    life::session::Storage,
    manifold::client::{Connection, domain::Plan},
};
use sark_h2::{conn, stream};

use crate::{
    client::{call, receive, session},
    metadata,
    protocol::section,
};

#[must_use = "a response stream must be drained or explicitly cancelled"]
pub struct Stream<'scope, K> {
    pub(in crate::client) brand: marker::PhantomData<fn(&'scope ()) -> &'scope ()>,
    pub(in crate::client) stream: K,
    pub(in crate::client) completion: receive::StreamKey,
}

pub trait Session: call::Session {
    fn try_start<D: crate::Codec>(
        &mut self,
        path: &[u8],
        target: Option<&[u8]>,
        metadata: &metadata::Metadata,
        codec: &mut D,
        message: &D::Encode,
    ) -> call::Admission<Self::Response>;

    fn try_poll(&mut self, stream: &Self::Response) -> Result<Option<Self::Event>, crate::Status>;

    fn cancel(&mut self, stream: Self::Response);
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
    fn try_start<D: crate::Codec>(
        &mut self,
        path: &[u8],
        target: Option<&[u8]>,
        metadata: &metadata::Metadata,
        codec: &mut D,
        message: &D::Encode,
    ) -> call::Admission<Stream<'scope, P::Key>> {
        let mut policy = policies::Direct;
        self.state
            .start_single::<receive::StreamKey, _, _>(
                path,
                target,
                metadata,
                codec,
                message,
                &mut policy,
            )
            .map(|started| Stream {
                brand: self.brand,
                stream: started.stream,
                completion: started.completion,
            })
    }

    fn try_poll(
        &mut self,
        stream: &Stream<'scope, P::Key>,
    ) -> Result<Option<receive::Response<C>>, crate::Status> {
        let mut policy = policies::Direct;
        self.state
            .poll_stream(stream.completion, &mut policy)
            .map(receive::Progress::into_option)
    }

    fn cancel(&mut self, stream: Stream<'scope, P::Key>) {
        let mut policy = policies::Direct;
        self.state
            .cancel_feed(stream.stream, stream.completion, &mut policy);
        self.state.refill(&mut policy);
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
    pub(in crate::client) fn try_response<'notify, D: crate::Codec>(
        &mut self,
        path: &[u8],
        target: Option<&[u8]>,
        metadata: &metadata::Metadata,
        codec: &mut D,
        message: &D::Encode,
        done: &mut policies::Notify<'notify, '_, '_>,
    ) -> call::Admission<Stream<'scope, P::Key>> {
        self.state
            .start_single::<receive::StreamKey, _, _>(path, target, metadata, codec, message, done)
            .map(|started| Stream {
                brand: self.brand,
                stream: started.stream,
                completion: started.completion,
            })
    }

    pub(in crate::client) fn poll_response<'notify>(
        &mut self,
        stream: &Stream<'scope, P::Key>,
        done: &mut policies::Notify<'notify, '_, '_>,
    ) -> Result<receive::Progress<receive::Response<C>>, crate::Status> {
        self.state.poll_stream(stream.completion, done)
    }
}
