use std::{iter, mem};

use o3::buffer::{hold, resident, segments, storage};
use segments::Shape;
use storage::backing;

use crate::payload::owned::{self, Segment};

pub struct Segments<'d> {
    pub(super) state: owned::Segments<'d>,
}

impl<'d> Iterator for Segments<'d> {
    type Item = Segment<'d>;

    fn next(&mut self) -> Option<Self::Item> {
        let (storage, owner) = match mem::replace(&mut self.state.storage, Shape::Empty) {
            Shape::Empty => (Shape::Empty, None),
            Shape::One(owner) => (Shape::Empty, Some(owner.into_inner())),
            Shape::Many(owners) => {
                let (owner, rest) = owners.take_front();
                (rest.into_shape(), Some(owner))
            }
        };
        self.state.storage = storage;
        owner.map(Segment::from)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.state.count().into_raw() as usize;
        (left, Some(left))
    }
}

impl ExactSizeIterator for Segments<'_> {}
impl iter::FusedIterator for Segments<'_> {}

impl Segments<'_> {
    pub fn is_empty(&self) -> bool {
        self.state.storage.is_empty()
    }

    pub fn byte_len(&self) -> usize {
        self.state.len()
    }

    pub fn charge(&self) -> hold::Charge {
        self.state.charge()
    }
}

pub struct Backings<'a, 'd> {
    pub(super) owners: segments::Owners<'a, resident::Shared<'d>>,
}

impl<'a, 'd> Iterator for Backings<'a, 'd> {
    type Item = backing::View<'a, 'd>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let owner = self.owners.next()?;
            if let Some(backing) = owner.backing() {
                return Some(backing);
            }
        }
    }
}

impl iter::FusedIterator for Backings<'_, '_> {}
