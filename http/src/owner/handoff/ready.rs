use o3::buffer::{Residency as _, hold, storage};

use crate::{
    owner::{
        evidence,
        handoff::{self, copy, stage},
    },
    payload::{fixed, owned, response::Body as _},
};

/// Response payload with no handler-borrow lifetime.
#[doc(hidden)]
pub enum Ready<'d> {
    Empty,
    Owned(Vec<u8>),
    Copied(copy::Body),
    Retained(storage::Shared),
    Segmented(owned::Segments<'d>),
    /// Retained segments projected from the live request owner.
    Linked(owned::Segments<'d>),
    Fixed(fixed::Body<'d>),
}

impl<'d> Ready<'d> {
    pub fn body_len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Owned(body) => body.len(),
            Self::Copied(body) => body.len(),
            Self::Retained(body) => body.len(),
            Self::Segmented(body) | Self::Linked(body) => body.len(),
            Self::Fixed(body) => body.len(),
        }
    }

    pub fn into_copy(self) -> (Self, Option<evidence::Copy>) {
        let copy = match &self {
            Self::Copied(body) => Some(body.copy()),
            _ => None,
        };
        (self, copy)
    }

    pub fn retention(&self) -> hold::Charge {
        match self {
            Self::Empty => hold::Charge::default(),
            Self::Owned(body) => hold::Charge::from_owner(body.len(), body.capacity()),
            Self::Copied(body) => hold::Charge::from_owner(body.body.len(), body.body.resident()),
            Self::Retained(body) => body.backing().map_or_else(hold::Charge::default, |owner| {
                hold::Charge::from_owner(body.len(), owner.resident())
            }),
            Self::Segmented(body) | Self::Linked(body) => body.charge(),
            Self::Fixed(body) => hold::Charge::from_owner(body.len(), 0),
        }
    }

    pub(in crate::owner::handoff) fn try_linked<'req, B: stage::Body<'req, 'd>>(
        &self,
        origin: handoff::Origin<'req, 'd, B>,
    ) -> Result<hold::Charge, hold::Overflow> {
        let charge = match self {
            Self::Empty => hold::Charge::default(),
            Self::Owned(body) => hold::Charge::from_owner(body.len(), 0),
            Self::Copied(body) => hold::Charge::from_owner(body.len(), 0),
            Self::Retained(body) => body.backing().map_or_else(hold::Charge::default, |owner| {
                if origin.source.is_linked(owner) {
                    hold::Charge::from_owner(owner.len(), owner.resident())
                } else {
                    hold::Charge::default()
                }
            }),
            Self::Segmented(body) => {
                let mut total = hold::Charge::default();
                for owner in body.backings() {
                    if origin.source.is_linked(owner) {
                        let charge = hold::Charge::from_owner(owner.len(), owner.resident());
                        total = total.try_add(charge)?;
                    }
                }
                total
            }
            Self::Linked(body) => body.charge(),
            Self::Fixed(body) => hold::Charge::from_owner(body.len(), 0),
        };
        Ok(charge)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Ready<'static>>() <= 7 * std::mem::size_of::<usize>());
};
