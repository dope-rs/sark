//! Views and transferable ownership over one call's decoded message lane.

use std::{iter, mem};

use o3::{
    buffer,
    buffer::storage,
    depot::{arena::linked, queue::pool},
};

use crate::{details, message};

pub struct Messages<'a, C: buffer::raw::Segment = storage::Shared> {
    lane: pool::Lane<'a, message::Frame<C>>,
}

impl<'a, C: buffer::raw::Segment> Messages<'a, C> {
    pub(in crate::server) fn new(lane: pool::Lane<'a, message::Frame<C>>) -> Self {
        Self { lane }
    }

    pub(in crate::server) fn into_frame(mut self) -> Result<message::Frame<C>, crate::Status> {
        self.check_one()?;
        Ok(self
            .lane
            .pop_front()
            .ok_or(details::Buffered::Cardinality { actual: 0 })?)
    }

    pub fn try_one(self) -> Result<&'a message::Frame<C>, crate::Status> {
        self.check_one()?;
        Ok(pool::View::from(self.lane)
            .front()
            .ok_or(details::Buffered::Cardinality { actual: 0 })?)
    }

    fn check_one(&self) -> Result<(), details::Buffered> {
        let actual = self.len();
        if actual != 1 {
            return Err(details::Buffered::Cardinality { actual });
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.lane.count().get()
    }

    pub fn is_empty(&self) -> bool {
        self.lane.is_empty()
    }

    pub fn iter(&self) -> Iter<'_, C> {
        Iter::new(pool::View::from(&self.lane))
    }
}

impl<'a, C: buffer::raw::Segment> IntoIterator for &'a Messages<'_, C> {
    type Item = &'a message::Frame<C>;
    type IntoIter = Iter<'a, C>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, C: buffer::raw::Segment> IntoIterator for Messages<'a, C> {
    type Item = &'a message::Frame<C>;
    type IntoIter = Iter<'a, C>;

    fn into_iter(self) -> Self::IntoIter {
        Iter::new(pool::View::from(self.lane))
    }
}

pub struct Iter<'a, C: buffer::raw::Segment = storage::Shared> {
    cursor: linked::Iter<'a, message::Frame<C>>,
    remaining: usize,
}

impl<'a, C: buffer::raw::Segment> Iter<'a, C> {
    fn new(view: pool::View<'a, message::Frame<C>>) -> Self {
        Self {
            cursor: view.iter(),
            remaining: view.len(),
        }
    }
}

impl<'a, C: buffer::raw::Segment> Iterator for Iter<'a, C> {
    type Item = &'a message::Frame<C>;

    fn next(&mut self) -> Option<Self::Item> {
        let message = self.cursor.next()?;
        self.remaining -= 1;
        Some(message)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.remaining))
    }
}

impl<C: buffer::raw::Segment> iter::FusedIterator for Iter<'_, C> {}

const _: () = {
    assert!(mem::size_of::<Messages<'_>>() == 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Iter<'_>>() == 3 * mem::size_of::<usize>());
};
