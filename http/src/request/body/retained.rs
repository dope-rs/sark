use std::{borrow::Borrow, convert::Infallible, ptr};

use o3::buffer::{
    self, hold, resident, segments,
    storage::{self, backing},
};

use crate::{
    owner::handoff::stage,
    payload::{owned, response::Body as _},
    request::body,
};

#[derive(Clone, Copy, Debug)]
pub enum Retained<'req, 'd> {
    Segments(&'req owned::Segments<'d>),
    Resident(&'req resident::Shared<'d>),
    Shared(&'req storage::Shared),
}

impl<'req, 'd> Retained<'req, 'd> {
    pub(crate) fn count(self) -> segments::Count {
        match self {
            Self::Segments(owner) => owner.count(),
            Self::Resident(owner) => segments::Count::new(u32::from(!owner.is_empty())),
            Self::Shared(owner) => segments::Count::new(u32::from(!owner.is_empty())),
        }
    }

    pub(crate) fn segment(self, index: usize) -> Option<&'req [u8]> {
        match self {
            Self::Segments(owner) => owner.segment(index),
            Self::Resident(owner) => (index == 0 && !owner.is_empty()).then(|| owner.as_slice()),
            Self::Shared(owner) => (index == 0 && !owner.is_empty()).then(|| owner.as_ref()),
        }
    }

    pub(crate) fn as_slice(&self) -> Option<&'req [u8]> {
        match *self {
            Self::Segments(owner) => owner.as_slice(),
            Self::Resident(owner) => Some(owner.as_slice()),
            Self::Shared(owner) => Some(owner.as_ref()),
        }
    }

    pub(crate) fn contiguous(self) -> Option<body::View<'req, 'd>> {
        match self {
            Self::Segments(owner) => match owner.storage() {
                segments::Shape::Empty => Some(body::View::Slice(&[])),
                segments::Shape::One(owner) => Some(body::View::Resident(owner.as_ref())),
                segments::Shape::Many(_) => None,
            },
            Self::Resident(owner) => Some(body::View::Resident(owner)),
            Self::Shared(owner) => Some(body::View::Shared(owner)),
        }
    }

    pub(crate) fn is_same(self, other: Self) -> bool {
        match (self, other) {
            (Self::Segments(left), Self::Segments(right)) => ptr::eq(left, right),
            (Self::Resident(left), Self::Resident(right)) => ptr::eq(left, right),
            (Self::Shared(left), Self::Shared(right)) => ptr::eq(left, right),
            _ => false,
        }
    }

    pub(crate) fn charge(self) -> hold::Charge {
        match self {
            Self::Segments(owner) => owner.charge(),
            Self::Resident(owner) => owner
                .backing()
                .map_or_else(hold::Charge::default, |backing| {
                    hold::Charge::from_owner(owner.len(), backing.resident())
                }),
            Self::Shared(owner) => owner
                .backing()
                .map_or_else(hold::Charge::default, |backing| {
                    hold::Charge::from_owner(owner.len(), backing.resident())
                }),
        }
    }

    pub(crate) fn contains(self, response: backing::View<'_, '_>) -> bool {
        match self {
            Self::Segments(owner) => owner.backings().any(|backing| backing.contains(&response)),
            Self::Resident(owner) => owner
                .backing()
                .is_some_and(|backing| backing.contains(&response)),
            Self::Shared(owner) => owner
                .backing()
                .is_some_and(|backing| backing.contains(&response)),
        }
    }

    pub(crate) fn try_clone(self) -> Result<owned::Segments<'d>, owned::Error> {
        match self {
            Self::Segments(owner) => owner.try_clone(),
            Self::Resident(owner) => owner
                .try_clone()
                .map(owned::Segments::from)
                .map_err(owned::Error::Retain),
            Self::Shared(owner) => owner
                .try_clone()
                .map(owned::Segments::from)
                .map_err(owned::Error::Retain),
        }
    }

    pub(crate) fn try_slice(
        self,
        bytes: &[u8],
    ) -> Result<Option<owned::Segments<'d>>, owned::Error> {
        match self {
            Self::Segments(owner) => {
                let select =
                    |owner: &'req resident::Shared<'d>| buffer::View::new(owner).select(bytes);
                let view = match owner.storage() {
                    segments::Shape::Empty => None,
                    segments::Shape::One(owner) => select(owner),
                    segments::Shape::Many(owners) => owners.as_chain().segments().find_map(select),
                };
                view.map(|view| {
                    view.try_retain()
                        .map(owned::Segments::from)
                        .map_err(owned::Error::Retain)
                })
                .transpose()
            }
            Self::Resident(owner) => buffer::View::new(owner)
                .select(bytes)
                .map(|view| {
                    view.try_retain()
                        .map(owned::Segments::from)
                        .map_err(owned::Error::Retain)
                })
                .transpose(),
            Self::Shared(owner) => buffer::View::new(owner)
                .select(bytes)
                .map(|view| {
                    view.try_retain()
                        .map(owned::Segments::from)
                        .map_err(owned::Error::Retain)
                })
                .transpose(),
        }
    }
}

const _: () = assert!(size_of::<Retained<'static, 'static>>() == 2 * size_of::<usize>());

impl<'req, 'd: 'req> stage::Body<'req, 'd> for Retained<'req, 'd> {
    type Deferred = usize;

    fn defer(self) -> usize {
        match self {
            Self::Segments(owner) => owner.len(),
            Self::Resident(owner) => owner.len(),
            Self::Shared(owner) => owner.len(),
        }
    }
}
impl<'req, 'd: 'req> Borrow<Retained<'req, 'd>> for Infallible {
    fn borrow(&self) -> &Retained<'req, 'd> {
        match *self {}
    }
}
