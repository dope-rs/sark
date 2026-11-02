use std::{convert::Infallible, mem, num::NonZero, ops};

use o3::buffer::{self, Cursor as _, bytes::scope, hold, storage, write::consume::Consume};
use storage::range;

use crate::wire::InputError;

/// A request window retaining its source borrow and validated bounds.
#[derive(Clone, Copy)]
pub enum Input<'a> {
    Borrowed(&'a [u8]),
    Buffered(buffer::View<'a, storage::Shared>),
}

impl<'a> Input<'a> {
    pub fn as_slice(&self) -> &'a [u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Buffered(view) => view.as_slice(),
        }
    }

    pub(crate) fn try_get(&self, range: ops::Range<usize>) -> Result<Self, range::Error> {
        match self {
            Self::Borrowed(bytes) => {
                let bytes = bytes.try_get(range)?;
                Ok(Self::Borrowed(bytes))
            }
            Self::Buffered(view) => Ok(Self::Buffered(view.try_get(range)?)),
        }
    }

    /// Advances the retained window by the largest fitting prefix.
    pub(crate) fn consume_max(&mut self, max: usize) -> usize {
        match self {
            Self::Borrowed(bytes) => {
                let used = bytes.len().min(max);
                *bytes = bytes.split_at(used).1;
                used
            }
            Self::Buffered(view) => Consume::consume_max(view, max),
        }
    }

    pub(crate) fn try_retain(self) -> Result<storage::Shared, InputError> {
        match self {
            Self::Borrowed(bytes) => storage::Shared::try_copy(bytes).map_err(InputError::Storage),
            Self::Buffered(view) => view.try_retain().map_err(InputError::Share),
        }
    }

    pub(crate) fn try_head(self, len: NonZero<u32>) -> Result<storage::Shared, InputError> {
        use crate::wire::body::ingress;

        match self
            .try_get(0..len.get() as usize)
            .map_err(InputError::Range)?
        {
            Self::Buffered(view)
                if <storage::Shared as hold::raw::Retain<'_>>::charge(&view)
                    .resident_bytes
                    .saturating_sub(view.len())
                    > ingress::PAGE_BYTES =>
            {
                storage::Shared::try_copy(view.as_slice()).map_err(InputError::Storage)
            }
            input => input.try_retain(),
        }
    }
}

impl scope::Source for Input<'_> {
    type Selection = Self;

    fn select(self, bytes: &[u8]) -> Option<Self> {
        match self {
            Self::Borrowed(source) => {
                Some(Self::Borrowed(source.get(source.subslice_range(bytes)?)?))
            }
            Self::Buffered(view) => Some(Self::Buffered(view.select(bytes)?)),
        }
    }
}

impl From<Infallible> for Input<'_> {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

const _: () = {
    assert!(mem::size_of::<Input<'_>>() == 3 * mem::size_of::<usize>());
    assert!(mem::align_of::<Input<'_>>() == mem::align_of::<usize>());
    assert!(!mem::needs_drop::<Input<'_>>());
    assert!(mem::size_of::<InputError>() <= 4 * mem::size_of::<usize>());
};
