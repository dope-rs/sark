use std::mem;

use field::Encode as _;
use sark_http::{
    field,
    field::{base64::Encoder, text::percent},
};

use crate::{
    Reason,
    protocol::{fields, trailer::lengths::Lengths},
};

#[derive(Clone, Copy)]
pub struct Fields<'a> {
    status: &'a crate::Status,
    metadata: &'a crate::metadata::Metadata,
    lengths: Lengths,
}

impl<'a> Fields<'a> {
    pub fn new(
        status: &'a crate::Status,
        metadata: &'a crate::metadata::Metadata,
    ) -> Result<Self, crate::Status> {
        use percent::raw;

        let message = match status.message() {
            Some(message) => raw::Encoder::new(message)
                .map_err(crate::Status::from)?
                .length(),
            None => percent::Length::ZERO,
        };
        let details = match status.details() {
            Some(details) if !details.is_empty() => Encoder::new(details).encoded_len(),
            Some(_) | None => 0,
        };
        let details = if details > u32::MAX as usize {
            return Err(crate::Status::length(Reason::DetailsLength, details));
        } else {
            details as u32
        };
        Ok(Self {
            status,
            metadata,
            lengths: Lengths::new(message, details),
        })
    }

    pub(crate) const fn from_lengths(
        status: &'a crate::Status,
        metadata: &'a crate::metadata::Metadata,
        lengths: Lengths,
    ) -> Self {
        Self {
            status,
            metadata,
            lengths,
        }
    }

    fn message(&self) -> Option<impl field::Encode + '_> {
        use percent::raw;

        let length = self.lengths.message();
        if length.get() == 0 {
            return None;
        }
        let text = self.status.message()?;
        // SAFETY: Fields::new measures this Status's immutable message.
        // The sole from_lengths caller borrows the same Status stored with its
        // Lengths by server::egress::pending::Trailer::new; replacements move
        // that whole pair, and the shared Status borrow forbids mutation here.
        Some(unsafe { raw::Encoder::from_length(text, length) })
    }

    pub(crate) const fn lengths(self) -> Lengths {
        self.lengths
    }
}

impl field::Source for Fields<'_> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        let [scode, smessage, sdetails] = self.status.indexing();
        const NAMES: [field::Lower<&[u8]>; 3] = [
            field::literal!(field::Lower::try_bytes(b"grpc-status")),
            field::literal!(field::Lower::try_bytes(b"grpc-message")),
            field::literal!(field::Lower::try_bytes(b"grpc-status-details-bin")),
        ];
        sink.encoded_with(NAMES[0], self.status.code().as_field(), scode);
        if let Some(message) = self.message() {
            sink.encoded_with(NAMES[1], message, smessage);
        }
        if self.lengths.details() != 0
            && let Some(details) = self.status.details()
        {
            sink.encoded_with(NAMES[2], Encoder::new(details), sdetails);
        }
        fields::emit(self.metadata, sink);
    }
}

const _: () = assert!(mem::size_of::<Fields<'_>>() == 3 * mem::size_of::<usize>());
