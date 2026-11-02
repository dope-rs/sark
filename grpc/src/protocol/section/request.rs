use sark_http::field::metadata::section::rules;

use crate::protocol::section::{Owner, Valid, seal, view::View};

#[repr(transparent)]
#[derive(PartialEq, Eq)]
pub(crate) struct Request(Valid);

impl Request {
    pub(crate) fn new(owner: Owner) -> Result<Self, crate::Status> {
        Ok(Self(owner.validate(rules::Kind::Request)?))
    }

    pub(crate) fn authority(&self) -> Option<&[u8]> {
        View::<Request>::new(self).field(seal::Field::Second)
    }

    pub(crate) const fn owner(&self) -> &Valid {
        &self.0
    }
}

const _: () = assert!(core::mem::size_of::<Request>() == core::mem::size_of::<Valid>());
