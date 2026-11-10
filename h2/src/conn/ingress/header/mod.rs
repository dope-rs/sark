use sark_http::field::metadata::block;

use crate::{conn::event, hpack::decode::limit};

mod input;
mod prepared;
pub(in crate::conn::ingress) mod sink;

pub(crate) use input::Input;
pub(crate) use prepared::{Prepared, Reset};

#[repr(u8)]
#[derive(Clone, Copy)]
pub(crate) enum Validation {
    Valid,
    Rejected(event::Rejected),
}

impl Validation {
    pub(in crate::conn::ingress) fn new(
        admit: limit::Admission,
        sealed: block::Validation,
        finished: block::Validation,
    ) -> Self {
        match (admit, sealed, finished) {
            (limit::Admission::Admitted, block::Validation::Valid, block::Validation::Valid) => {
                Self::Valid
            }
            (admit, sealed, finished) => Self::Rejected(event::Rejected {
                admit,
                sealed,
                finished,
            }),
        }
    }
}

const _: () = assert!(std::mem::size_of::<Validation>() == 4 * std::mem::size_of::<bool>());
