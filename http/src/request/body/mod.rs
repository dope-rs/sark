use std::iter::FusedIterator;

use o3::buffer::{
    bytes::scope,
    hold::ranges,
    segments, storage,
    write::{Sink as _, consume},
};

use crate::{
    owner::{handoff, handoff::stage},
    payload::{
        owned::{self, Segments},
        response,
        response::Body as _,
    },
};

mod error;
pub mod iter;
mod retained;
mod view;

pub use error::Error;
#[doc(hidden)]
pub use retained::Retained;
pub use view::View;

/// Transport storage that can expose request-body projections without changing ownership.
///
pub trait Source<'d> {
    type Body<'a>: Into<Ref<'a, 'd>>
    where
        Self: 'a,
        'd: 'a;

    fn try_project<V, T>(
        &mut self,
        value: V,
        project: impl for<'a> FnOnce(V, View<'a, 'd>) -> T,
    ) -> Result<T, (V, Error)>;

    fn body<'a>(&'a self) -> Self::Body<'a>
    where
        'd: 'a;
}

#[derive(Clone, Copy, Debug)]
enum RefStorage<'req, 'd> {
    Discarded,
    Empty(&'req [u8]),
    /// One nonempty slice, admitted by `Ref::from_slice`.
    Slice(&'req [u8]),
    Retained(Retained<'req, 'd>),
    Ranges(&'req ranges::Set<'req, 'req, [u8]>),
}

/// Borrowed logical body preserving transport segmentation.
#[derive(Clone, Copy, Debug)]
pub struct Ref<'req, 'd> {
    storage: RefStorage<'req, 'd>,
    len: usize,
}

impl<'req, 'd> Ref<'req, 'd> {
    /// Classifies an empty view or one nonempty borrowed segment.
    pub const fn from_slice(bytes: &'req [u8]) -> Self {
        let storage = if bytes.is_empty() {
            RefStorage::Empty(bytes)
        } else {
            RefStorage::Slice(bytes)
        };
        Self {
            storage,
            len: bytes.len(),
        }
    }

    #[doc(hidden)]
    pub const fn discarded(len: usize) -> Self {
        Self {
            storage: RefStorage::Discarded,
            len,
        }
    }

    #[doc(hidden)]
    pub const fn from_ranges(ranges: &'req ranges::Set<'req, 'req, [u8]>) -> Self {
        Self {
            storage: RefStorage::Ranges(ranges),
            len: ranges.len(),
        }
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn count(&self) -> segments::Count {
        match self.storage {
            RefStorage::Discarded | RefStorage::Empty(_) => segments::Count::EMPTY,
            RefStorage::Slice(_) => segments::Count::ONE,
            RefStorage::Retained(body) => body.count(),
            RefStorage::Ranges(ranges) => ranges.count(),
        }
    }

    pub fn segment(&self, index: usize) -> Option<&'req [u8]> {
        match self.storage {
            RefStorage::Discarded | RefStorage::Empty(_) => None,
            RefStorage::Slice(bytes) => (index == 0).then_some(bytes),
            RefStorage::Retained(body) => body.segment(index),
            RefStorage::Ranges(ranges) => ranges.get(index).map(|view| view.as_slice()),
        }
    }

    pub fn segments(self) -> iter::Slices<'req, 'd> {
        iter::Slices::new(self)
    }

    pub fn as_slice(&self) -> Option<&'req [u8]> {
        match self.storage {
            RefStorage::Discarded => None,
            RefStorage::Empty(bytes) => Some(bytes),
            RefStorage::Slice(bytes) => Some(bytes),
            RefStorage::Retained(body) => body.as_slice(),
            RefStorage::Ranges(ranges) => ranges.contiguous(),
        }
    }

    /// Copies the logical body into one exact shared owner.
    pub fn try_copy(self) -> Result<storage::Shared, owned::Error> {
        if let RefStorage::Discarded = self.storage {
            return Err(owned::Error::Discarded);
        }
        storage::Exact::try_build(self.len(), |out| {
            for part in self.segments() {
                out.write_slice(part)?;
            }
            Ok::<(), consume::Error>(())
        })
        .map(storage::Exact::into_shared)
        .map_err(owned::Error::Build)
    }

    pub(crate) const fn try_retained(self) -> Result<Retained<'req, 'd>, Self> {
        match self.storage {
            RefStorage::Retained(body) => Ok(body),
            _ => Err(self),
        }
    }
}

impl<'src, 'd> response::Body<'d> for Ref<'src, 'd> {
    const INLINE: bool = true;

    fn len(&self) -> usize {
        self.len
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Self::as_slice(self)
    }

    fn segments(&self) -> impl ExactSizeIterator<Item = &[u8]> + FusedIterator {
        iter::Slices::new(*self)
    }

    fn try_resolve<'req, H: scope::Source, O: stage::Body<'req, 'd>>(
        self,
        slot: &handoff::Slot<'req, 'd, H, O>,
    ) -> Result<handoff::Plan<'req, 'd, H::Selection, O>, owned::Error> {
        let origin = slot.origin();
        if !self.is_empty() {
            if let RefStorage::Retained(view) = self.storage {
                return handoff::Plan::try_retain(origin, view);
            }
            return handoff::Plan::try_view(origin, self);
        }
        Ok(handoff::Plan::new(origin.clear()))
    }
}

impl<'d> Source<'d> for &[u8] {
    type Body<'a>
        = Ref<'a, 'd>
    where
        Self: 'a,
        'd: 'a;

    fn try_project<V, T>(
        &mut self,
        value: V,
        project: impl for<'a> FnOnce(V, View<'a, 'd>) -> T,
    ) -> Result<T, (V, Error)> {
        Ok(project(value, View::Slice(self)))
    }

    fn body<'a>(&'a self) -> Self::Body<'a>
    where
        'd: 'a,
    {
        Ref::from_slice(self)
    }
}

impl<'req, 'd> Source<'d> for Ref<'req, 'd> {
    type Body<'a>
        = Ref<'a, 'd>
    where
        Self: 'a,
        'd: 'a;

    fn try_project<V, T>(
        &mut self,
        value: V,
        project: impl for<'a> FnOnce(V, View<'a, 'd>) -> T,
    ) -> Result<T, (V, Error)> {
        let view = match self.storage {
            RefStorage::Retained(owner) => owner.contiguous(),
            _ => self.as_slice().map(View::Slice),
        };
        match view {
            Some(view) => Ok(project(value, view)),
            None => Err((
                value,
                Error::Borrowed {
                    len: self.len(),
                    count: self.count(),
                },
            )),
        }
    }

    fn body<'a>(&'a self) -> Self::Body<'a>
    where
        'd: 'a,
    {
        *self
    }
}

const _: () = {
    assert!(core::mem::size_of::<Ref<'static, 'static>>() == 4 * core::mem::size_of::<usize>());
};

impl<'req, 'd> From<&'req Segments<'d>> for Ref<'req, 'd> {
    fn from(body: &'req Segments<'d>) -> Self {
        Self {
            storage: RefStorage::Retained(Retained::Segments(body)),
            len: body.len(),
        }
    }
}
