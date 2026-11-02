use core::marker;

use o3::buffer::storage;

use crate::{
    metadata,
    server::service::reply::{
        self,
        parts::{Parts, State},
    },
};

pub struct Response<'d, W> {
    parts: Parts<crate::Status, W>,
    scope: marker::PhantomData<fn(&'d mut ()) -> &'d mut ()>,
}

impl<'d, W> Response<'d, W> {
    pub(in crate::server) fn with_writer(writer: W) -> Self {
        Self {
            parts: Parts::new(crate::Status::ok(), writer),
            scope: marker::PhantomData,
        }
    }

    pub(in crate::server) fn into_parts(
        self,
    ) -> (metadata::Metadata, metadata::Metadata, crate::Status, W) {
        self.parts.into_inner()
    }

    pub(in crate::server) fn try_open(
        self,
    ) -> Result<(metadata::Metadata, metadata::Metadata, W), crate::Status> {
        match self.parts.state {
            State::Ready => {
                let (metadata, trailers, _, writer) = self.parts.into_inner();
                Ok((metadata, trailers, writer))
            }
            State::Failed => Err(self.parts.status),
        }
    }

    pub fn metadata_mut(&mut self) -> &mut metadata::Metadata {
        &mut self.parts.metadata
    }

    pub fn trailers_mut(&mut self) -> &mut metadata::Metadata {
        &mut self.parts.trailers
    }

    pub fn status(&self) -> &crate::Status {
        &self.parts.status
    }

    pub fn finish_with(&mut self, status: crate::Status) {
        match self.parts.state {
            State::Ready => self.parts.status = status,
            State::Failed => {}
        }
    }
}

impl<W: reply::Write> Response<'_, W> {
    /// Transfers an already materialized payload owner into the response.
    pub fn push_message(&mut self, payload: Vec<u8>) {
        if matches!(self.parts.state, State::Ready)
            && let Err(status) = self.parts.writer.push_message(payload)
        {
            self.parts.fail(status);
        }
    }

    /// Transfers a retained payload without copying its bytes.
    pub fn push(&mut self, payload: storage::Shared) {
        if matches!(self.parts.state, State::Ready)
            && let Err(status) = self.parts.writer.push(payload)
        {
            self.parts.fail(status);
        }
    }

    pub(super) fn push_encoded<C: crate::Codec>(&mut self, codec: &mut C, message: &C::Encode) {
        if matches!(self.parts.state, State::Ready)
            && let Err(status) = self.parts.writer.encode(codec, message)
        {
            self.parts.fail(status);
        }
    }
}
