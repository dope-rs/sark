use std::{iter::FusedIterator, ops};

use o3::{
    buffer::{
        Residency as _,
        bytes::scope,
        hold, resident, segments,
        segments::{Nonempty, Shape, assembly},
        storage,
    },
    mem,
};

use crate::{
    owner::{handoff, handoff::stage},
    payload::{
        owned::{Error, Segment, iter},
        response,
    },
    request::body,
};

/// Retained payload ownership preserving transport segmentation.
#[repr(transparent)]
#[derive(Debug)]
pub struct Segments<'d> {
    pub(in crate::payload::owned) storage: Shape<resident::Shared<'d>>,
}

impl<'d> Segments<'d> {
    pub const fn new() -> Self {
        Self {
            storage: Shape::Empty,
        }
    }

    pub fn try_push(
        self,
        segment: Segment<'d>,
    ) -> Result<Self, mem::Rejected<(Self, Segment<'d>), assembly::Error>> {
        let ops::ControlFlow::Continue(item) = segments::Item::new(resident::Shared::from(segment))
        else {
            return Ok(self);
        };
        match self.storage {
            Shape::Empty => Ok(Self {
                storage: Shape::One(item),
            }),
            Shape::One(first) => Self::try_merge((first, item), Segment::from_item),
            Shape::Many(chain) => Self::try_merge((chain, item), Segment::from_item),
        }
    }

    pub fn try_extend(
        self,
        source: Self,
    ) -> Result<Self, mem::Rejected<(Self, Self), assembly::Error>> {
        match (self.storage, source.storage) {
            (storage, Shape::Empty) | (Shape::Empty, storage) => Ok(Self { storage }),
            (Shape::One(first), Shape::One(second)) => Self::try_merge((first, second), Self::from),
            (Shape::Many(first), Shape::One(second)) => {
                Self::try_merge((first, second), Self::from)
            }
            (Shape::One(first), Shape::Many(second)) => {
                Self::try_merge((first, second), Self::from)
            }
            (Shape::Many(first), Shape::Many(second)) => {
                Self::try_merge((first, second), Self::from)
            }
        }
    }

    fn try_merge<A, B, T>(
        parts: (A, B),
        restore: impl FnOnce(B) -> T,
    ) -> Result<Self, mem::Rejected<(Self, T), assembly::Error>>
    where
        Self: From<A>,
        segments::Many<resident::Shared<'d>>:
            TryFrom<(A, B), Error = mem::Rejected<(A, B), assembly::Error>>,
    {
        segments::Many::try_from(parts)
            .map(|chain| Self {
                storage: Shape::Many(chain),
            })
            .map_err(|rejected| {
                rejected.map(|(first, second)| (Self::from(first), restore(second)))
            })
    }

    pub fn charge(&self) -> hold::Charge {
        match &self.storage {
            Shape::Empty => hold::Charge::default(),
            Shape::One(owner) => hold::Charge::from_owner(owner.len(), owner.resident()),
            Shape::Many(owners) => hold::Charge::from(owners.as_chain()),
        }
    }

    pub fn segment(&self, index: usize) -> Option<&[u8]> {
        match &self.storage {
            Shape::Empty => None,
            Shape::One(segment) => (index == 0).then(|| segment.as_slice()),
            Shape::Many(segments) => segments.as_chain().segments().nth(index).map(AsRef::as_ref),
        }
    }

    #[doc(hidden)]
    pub fn backings(&self) -> iter::Backings<'_, 'd> {
        iter::Backings {
            owners: self.storage.owners(),
        }
    }

    pub fn into_shape(self) -> Shape<resident::Shared<'d>> {
        self.storage
    }

    pub(crate) const fn storage(&self) -> &Shape<resident::Shared<'d>> {
        &self.storage
    }

    pub fn take(self) -> Option<Nonempty<resident::Shared<'d>>> {
        match self.storage {
            Shape::Empty => None,
            Shape::One(owner) => Some(Nonempty::One(owner)),
            Shape::Many(owners) => Some(Nonempty::Many(owners)),
        }
    }

    pub fn try_clone(&self) -> Result<Self, Error> {
        let storage = match &self.storage {
            Shape::Empty => Shape::Empty,
            Shape::One(owner) => Shape::One(owner.try_clone().map_err(Error::Retain)?),
            Shape::Many(source) => Shape::Many(
                segments::Exact::try_new(source)
                    .map_err(Error::Metadata)?
                    .try_collect()
                    .map_err(Error::Retain)?,
            ),
        };
        Ok(Self { storage })
    }
}

impl<'d> body::Source<'d> for Segments<'d> {
    type Body<'a>
        = &'a Self
    where
        Self: 'a,
        'd: 'a;

    fn try_project<V, T>(
        &mut self,
        value: V,
        project: impl for<'a> FnOnce(V, body::View<'a, 'd>) -> T,
    ) -> Result<T, (V, body::Error)> {
        let plan = match &self.storage {
            Shape::Empty => return Ok(project(value, body::View::Slice(&[]))),
            Shape::One(owner) => {
                return Ok(project(value, body::View::Resident(owner.as_ref())));
            }
            Shape::Many(chain) => chain.as_chain().try_flatten(),
        };
        match plan {
            Ok(store) => {
                let owner: resident::Shared<'d> = store.into();
                let output = project(value, body::View::Resident(&owner));
                *self = Self::from(owner);
                Ok(output)
            }
            Err(error) => Err((value, body::Error::Owned(Error::Storage(error)))),
        }
    }

    fn body<'a>(&'a self) -> Self::Body<'a>
    where
        'd: 'a,
    {
        self
    }
}

impl<'d> response::Body<'d> for Segments<'d> {
    fn len(&self) -> usize {
        self.storage.len()
    }

    fn as_slice(&self) -> Option<&[u8]> {
        self.storage.contiguous()
    }

    fn segments(&self) -> impl ExactSizeIterator<Item = &[u8]> + FusedIterator {
        self.storage.segments()
    }

    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, Error> {
        Ok(handoff::Plan::new(slot.origin().segmented(self)))
    }
}

impl<'d> IntoIterator for Segments<'d> {
    type Item = Segment<'d>;
    type IntoIter = iter::Segments<'d>;

    fn into_iter(self) -> Self::IntoIter {
        iter::Segments { state: self }
    }
}

impl<'d> From<segments::Chain<resident::Shared<'d>>> for Segments<'d> {
    fn from(chain: segments::Chain<resident::Shared<'d>>) -> Self {
        Self {
            storage: chain.into_shape(),
        }
    }
}

impl<'d> From<segments::Many<resident::Shared<'d>>> for Segments<'d> {
    fn from(chain: segments::Many<resident::Shared<'d>>) -> Self {
        Self {
            storage: Shape::Many(chain),
        }
    }
}

impl<'d> From<Shape<resident::Shared<'d>>> for Segments<'d> {
    fn from(storage: Shape<resident::Shared<'d>>) -> Self {
        Self { storage }
    }
}

impl<'d> From<Nonempty<resident::Shared<'d>>> for Segments<'d> {
    fn from(owners: Nonempty<resident::Shared<'d>>) -> Self {
        Self {
            storage: owners.into_shape(),
        }
    }
}

impl<'d> ops::Deref for Segments<'d> {
    type Target = Shape<resident::Shared<'d>>;
    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl<'d> From<segments::Item<resident::Shared<'d>>> for Segments<'d> {
    fn from(owner: segments::Item<resident::Shared<'d>>) -> Self {
        Self {
            storage: Shape::One(owner),
        }
    }
}

impl<'d> From<storage::Shared> for Segments<'d> {
    fn from(data: storage::Shared) -> Self {
        Self::from(resident::Shared::from(data))
    }
}

impl<'d> From<Segment<'d>> for Segments<'d> {
    fn from(data: Segment<'d>) -> Self {
        Self::from(resident::Shared::from(data))
    }
}

impl<'d> From<resident::Shared<'d>> for Segments<'d> {
    fn from(data: resident::Shared<'d>) -> Self {
        Self {
            storage: match segments::Item::new(data) {
                ops::ControlFlow::Continue(owner) => Shape::One(owner),
                ops::ControlFlow::Break(_) => Shape::Empty,
            },
        }
    }
}

impl Default for Segments<'_> {
    fn default() -> Self {
        Self::new()
    }
}

const _: () = {
    assert!(
        size_of::<<Segments<'static> as body::Source<'static>>::Body<'static>>()
            == size_of::<&Segments<'static>>()
    );
    assert!(size_of::<Segments<'static>>() == size_of::<Shape<resident::Shared<'static>>>());
    assert!(align_of::<Segments<'static>>() == align_of::<Shape<resident::Shared<'static>>>());
    assert!(
        size_of::<Option<Segments<'static>>>()
            == size_of::<Option<Shape<resident::Shared<'static>>>>()
    );
};
