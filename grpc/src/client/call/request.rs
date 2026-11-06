use receive::policies;
use sark_core::{
    life::session::Storage,
    manifold::client::{Connection, domain::Plan},
};
use sark_h2::{conn, stream};

use crate::{
    client::{
        call::{self, unary},
        receive, session,
    },
    metadata,
    protocol::section,
};

#[must_use = "a request stream must be finished or explicitly cancelled"]
pub struct Stream<'scope, K> {
    pub(in crate::client) response: unary::Handle<'scope, K>,
}

pub trait Session: unary::Session {
    fn open(
        &mut self,
        path: &[u8],
        peer: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<Self::Request, crate::Status>;

    fn try_send<D: crate::Codec>(
        &mut self,
        stream: &mut Self::Request,
        codec: &mut D,
        message: &D::Encode,
    ) -> call::Admission<()>;

    fn finish(&mut self, stream: Self::Request) -> Result<Self::Call, crate::Status>;

    fn cancel(&mut self, stream: Self::Request);
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
            .start_reply::<receive::UnaryKey>(path, peer, metadata)?;
        Ok(Stream {
            response: unary::Handle {
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
    ) -> Result<unary::Handle<'scope, P::Key>, crate::Status> {
        let mut policy = policies::Direct;
        self.state.finish(stream.response.stream, &mut policy)?;
        Ok(stream.response)
    }

    fn cancel(&mut self, stream: Stream<'scope, P::Key>) {
        let unary::Handle {
            stream,
            completion: done,
            ..
        } = stream.response;
        let mut policy = policies::Direct;
        self.state.cancel_unary(stream, done, &mut policy);
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
    pub(in crate::client) fn try_request<'notify, D: crate::Codec>(
        &mut self,
        stream: &mut Stream<'scope, P::Key>,
        codec: &mut D,
        message: &D::Encode,
        done: &mut policies::Notify<'notify, '_, '_>,
    ) -> call::Admission<()> {
        self.state
            .send(stream.response.stream, codec, message, done)
    }

    pub(in crate::client) fn finish_request<'notify>(
        &mut self,
        stream: Stream<'scope, P::Key>,
        done: &mut policies::Notify<'notify, '_, '_>,
    ) -> Result<unary::Handle<'scope, P::Key>, crate::Status> {
        self.state.finish(stream.response.stream, done)?;
        Ok(stream.response)
    }
}
