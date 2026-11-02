use std::iter::FusedIterator;

use o3::buffer::{hold::ranges, resident, segments};

use crate::request::body;

pub struct Slices<'req, 'd> {
    storage: Storage<'req, 'd>,
}

enum Storage<'req, 'd> {
    Empty,
    /// One nonempty slice, admitted by the source or `Self::from_slice`.
    Slice(&'req [u8]),
    Retained(segments::Owners<'req, resident::Shared<'d>>),
    Ranges(ranges::Iter<'req, 'req, 'req, [u8]>),
}

impl<'req> Storage<'req, '_> {
    /// Classifies retained storage without yielding empty segments.
    fn from_slice(bytes: &'req [u8]) -> Self {
        if bytes.is_empty() {
            Self::Empty
        } else {
            Self::Slice(bytes)
        }
    }
}

impl<'req, 'd> Slices<'req, 'd> {
    pub(super) fn new(body: body::Ref<'req, 'd>) -> Self {
        let data = match body.storage {
            body::RefStorage::Discarded | body::RefStorage::Empty(_) => Storage::Empty,
            body::RefStorage::Slice(slice) => Storage::Slice(slice),
            body::RefStorage::Retained(body::Retained::Segments(owner)) => {
                Storage::Retained(owner.owners())
            }
            body::RefStorage::Retained(body::Retained::Resident(owner)) => {
                Storage::from_slice(owner.as_slice())
            }
            body::RefStorage::Retained(body::Retained::Shared(owner)) => {
                Storage::from_slice(owner.as_ref())
            }
            body::RefStorage::Ranges(ranges) => Storage::Ranges(ranges.iter()),
        };
        Self { storage: data }
    }
}

impl<'req> Iterator for Slices<'req, '_> {
    type Item = &'req [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.storage {
            Storage::Empty => None,
            Storage::Slice(bytes) => {
                let bytes = *bytes;
                self.storage = Storage::Empty;
                Some(bytes)
            }
            Storage::Retained(parts) => parts.next().map(AsRef::as_ref),
            Storage::Ranges(parts) => parts.next().map(|view| view.as_slice()),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = match &self.storage {
            Storage::Empty => 0,
            Storage::Slice(_) => 1,
            Storage::Retained(parts) => parts.len(),
            Storage::Ranges(parts) => parts.len(),
        };
        (left, Some(left))
    }
}

impl ExactSizeIterator for Slices<'_, '_> {}
impl FusedIterator for Slices<'_, '_> {}

const _: () = {
    assert!(size_of::<Slices<'_, '_>>() == 4 * size_of::<usize>());
    assert!(size_of::<Option<Slices<'_, '_>>>() == size_of::<Slices<'_, '_>>());
};
