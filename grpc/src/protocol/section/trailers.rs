use std::str;

use sark_core::manifold::stream::Section;
use sark_http::{
    field::{Indexing, metadata::section::rules},
    payload::response::status,
};

use crate::{
    Reason,
    protocol::section::{Owner, Valid, seal, view::View},
};

#[repr(transparent)]
#[derive(Debug)]
pub(crate) struct Trailers(Valid);

pub(crate) enum Terminal {
    Trailers(Trailers),
    Http(status::Code),
}

impl Terminal {
    pub(crate) fn new(
        owner: Owner,
        section: Section,
        initial: Option<status::Code>,
    ) -> Result<Self, crate::Status> {
        let kind = match section {
            Section::Initial => rules::Kind::Response,
            Section::Trailing => rules::Kind::ResponseTrailers,
        };
        let owner = owner.validate(kind)?;
        let Some(code) = View::<Valid>::new(&owner).code() else {
            if let Some(status) = View::<Valid>::new(&owner).http_status().or(initial) {
                return Ok(Self::Http(status));
            }
            return Err(crate::Status::new(
                crate::Code::Internal,
                Reason::StatusMissing,
            ));
        };
        if let Some(bytes) = View::<Valid>::new(&owner).field(seal::Field::Second) {
            crate::details::RichStatus::check(code, bytes)?;
        }
        Ok(Self::Trailers(Trailers(owner)))
    }
}

impl Trailers {
    pub(crate) fn code(&self) -> crate::Code {
        // SAFETY: Trailers retains a validated, present status in the immutable prefix.
        unsafe {
            self.owner()
                .bytes()
                .as_ptr()
                .add(seal::CODE)
                .cast::<crate::Code>()
                .read()
        }
    }

    pub(crate) fn message(&self) -> Option<&str> {
        let bytes = View::<Trailers>::new(self).field(seal::Field::First)?;
        // SAFETY: response admission captures this range only from the repairing
        // text decoder. Valid freezes those bytes, preserving its UTF-8 proof.
        Some(unsafe { str::from_utf8_unchecked(bytes) })
    }

    pub(crate) fn indexing(&self) -> [Indexing; 3] {
        [
            View::<Trailers>::new(self).indexing(),
            View::<Trailers>::new(self).indexing_at(seal::Field::First),
            View::<Trailers>::new(self).indexing_at(seal::Field::Second),
        ]
    }

    pub(crate) fn details(&self) -> Option<&[u8]> {
        View::<Trailers>::new(self).field(seal::Field::Second)
    }

    pub(crate) const fn owner(&self) -> &Valid {
        &self.0
    }
}

const _: () = assert!(core::mem::size_of::<Trailers>() == core::mem::size_of::<Valid>());
