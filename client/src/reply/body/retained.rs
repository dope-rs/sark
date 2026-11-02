use std::mem;

use o3::buffer::{segments, storage};

/// Lifetime-independent HTTP body preserving its physical segments.
pub struct Retained {
    storage: segments::Shape<storage::Shared>,
}

impl Retained {
    pub(in crate::reply) const fn empty() -> Self {
        Self {
            storage: segments::Shape::Empty,
        }
    }

    pub(in crate::reply) const fn one(owner: segments::Item<storage::Shared>) -> Self {
        Self {
            storage: segments::Shape::One(owner),
        }
    }

    pub(in crate::reply) fn many(segments: segments::Chain<storage::Shared>) -> Self {
        Self {
            storage: segments.into_shape(),
        }
    }

    pub fn len(&self) -> usize {
        self.storage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn segment(&self, index: usize) -> Option<&[u8]> {
        self.segments().nth(index)
    }

    pub fn segments<'a>(&'a self) -> impl ExactSizeIterator<Item = &'a [u8]> + 'a {
        self.storage.segments()
    }

    pub fn contiguous(&self) -> Option<&[u8]> {
        self.storage.contiguous()
    }
}

const _: () = assert!(mem::size_of::<Retained>() <= 4 * mem::size_of::<usize>());
