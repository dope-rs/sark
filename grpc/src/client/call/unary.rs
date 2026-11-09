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
    client::{call, receive, session},
    metadata,
    protocol::section,
};

#[must_use = "a unary call must be awaited or explicitly cancelled"]
pub struct Handle<'scope, K> {
    pub(in crate::client) brand: marker::PhantomData<fn(&'scope ()) -> &'scope ()>,
    pub(in crate::client) stream: K,
    pub(in crate::client) completion: receive::UnaryKey,
}

pub trait Session: call::Session {
    fn start_raw(
        &mut self,
        path: &[u8],
        peer: Option<&[u8]>,
        metadata: &metadata::Metadata,
        payload: &[u8],
    ) -> Result<Self::Call, crate::Status>;

    fn try_start<D: crate::Codec>(
        &mut self,
        path: &[u8],
        peer: Option<&[u8]>,
        metadata: &metadata::Metadata,
        codec: &mut D,
        message: &D::Encode,
    ) -> call::Admission<Self::Call>;

    fn poll(&mut self, call: &Self::Call) -> Result<Option<Self::Unary>, crate::Status>;

    fn try_poll(&mut self, call: &Self::Call) -> Result<Option<Self::Unary>, crate::Status>;

    fn cancel(&mut self, call: Self::Call);
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
    fn start_raw(
        &mut self,
        path: &[u8],
        peer: Option<&[u8]>,
        metadata: &metadata::Metadata,
        payload: &[u8],
    ) -> Result<Handle<'scope, P::Key>, crate::Status> {
        let started = self
            .state
            .start_reply::<receive::UnaryKey>(path, peer, metadata)?;
        let mut policy = policies::Direct;
        self.state
            .send_bytes(started.stream, payload, Boundary::End, &mut policy)?;
        Ok(Handle {
            brand: self.brand,
            stream: started.stream,
            completion: started.completion,
        })
    }

    fn try_start<D: crate::Codec>(
        &mut self,
        path: &[u8],
        peer: Option<&[u8]>,
        metadata: &metadata::Metadata,
        codec: &mut D,
        message: &D::Encode,
    ) -> call::Admission<Handle<'scope, P::Key>> {
        let mut policy = policies::Direct;
        self.state
            .start_single::<receive::UnaryKey, _, _>(
                path,
                peer,
                metadata,
                codec,
                message,
                &mut policy,
            )
            .map(|started| Handle {
                brand: self.brand,
                stream: started.stream,
                completion: started.completion,
            })
    }

    fn poll(
        &mut self,
        call: &Handle<'scope, P::Key>,
    ) -> Result<Option<receive::UnaryResult<C>>, crate::Status> {
        let mut policy = policies::Direct;
        self.state
            .poll_unary(call.completion, &mut policy)
            .map(receive::Progress::into_option)
    }

    fn try_poll(
        &mut self,
        call: &Handle<'scope, P::Key>,
    ) -> Result<Option<receive::UnaryResult<C>>, crate::Status> {
        if let Some(result) = Session::poll(self, call)? {
            return Ok(Some(result));
        }
        self.state.ingress.take_error()?;
        Ok(None)
    }

    fn cancel(&mut self, call: Handle<'scope, P::Key>) {
        let mut policy = policies::Direct;
        self.state
            .cancel_unary(call.stream, call.completion, &mut policy);
        self.state.refill(&mut policy);
    }
}
