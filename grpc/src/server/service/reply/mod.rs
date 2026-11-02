//! Compile-time selected response writers.
use core::{marker, task::Poll};

use crate::{Reason, metadata, server::service::handler::modes};

pub(super) mod parts;
pub(in crate::server) mod proof;
mod write;

pub use write::Write;

pub struct Reply<'d, W> {
    parts: parts::Parts<Option<crate::Status>, W>,
    scope: marker::PhantomData<fn(&'d mut ()) -> &'d mut ()>,
}

impl<'d, W> Reply<'d, W> {
    pub(in crate::server) fn with_writer(writer: W) -> Self {
        Self {
            parts: parts::Parts::new(None, writer),
            scope: marker::PhantomData,
        }
    }

    pub(in crate::server) fn into_parts(
        self,
    ) -> (
        metadata::Metadata,
        metadata::Metadata,
        Option<crate::Status>,
        W,
    ) {
        self.parts.into_inner()
    }

    pub fn metadata_mut(&mut self) -> &mut metadata::Metadata {
        &mut self.parts.metadata
    }

    pub fn trailers_mut(&mut self) -> &mut metadata::Metadata {
        &mut self.parts.trailers
    }

    pub fn finish_with(&mut self, status: crate::Status) {
        match self.parts.state {
            parts::State::Ready => self.parts.status = Some(status),
            parts::State::Failed => {}
        }
    }

    pub fn is_finished(&self) -> bool {
        self.parts.status.is_some()
    }

    pub(in crate::server) fn is_failed(&self) -> bool {
        matches!(self.parts.state, parts::State::Failed)
    }
}

impl<W: Write> Reply<'_, W> {
    /// Transfers an already materialized payload owner into the reply.
    pub fn push_message(&mut self, payload: Vec<u8>) {
        if matches!(self.parts.state, parts::State::Ready)
            && let Err(status) = self.parts.writer.push_message(payload)
        {
            self.parts.fail(Some(status));
        }
    }

    pub fn len(&self) -> usize {
        self.parts.writer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.parts.writer.is_empty()
    }

    pub fn poll_ready(&self) -> Result<Poll<()>, crate::Status> {
        match self.parts.state {
            parts::State::Ready => self.parts.writer.poll_ready(),
            parts::State::Failed => Err(crate::Status::new(
                crate::Code::Internal,
                Reason::ReplyFailed,
            )),
        }
    }

    pub fn try_encode<C: crate::Codec>(
        &mut self,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<Poll<()>, crate::Status> {
        match self.parts.state {
            parts::State::Ready => self.parts.writer.try_encode(codec, message),
            parts::State::Failed => Err(crate::Status::new(
                crate::Code::Internal,
                Reason::ReplyFailed,
            )),
        }
    }

    pub fn apply_live<C: crate::Codec>(
        &mut self,
        response: modes::LiveResponse<C::Encode>,
        codec: &mut C,
    ) {
        match self.parts.state {
            parts::State::Ready => {}
            parts::State::Failed => return,
        }
        self.parts.metadata = response.metadata;
        self.parts.trailers = response.trailers;
        self.parts.status = response.status;
        if let Some(message) = response.message
            && let Err(status) = self.parts.writer.encode(codec, &message)
        {
            self.parts.fail(Some(status));
        }
    }
}
