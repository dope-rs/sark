use std::mem;

use o3::buffer::{raw, resident, segments};
use segments::scatter;

use crate::capacity;

/// HTTP body retaining its metadata-pool borrow and receive-owner lifetime.
#[repr(transparent)]
pub struct Scoped<'a, 'd> {
    pub(in crate::reply) segments:
        segments::Chain<resident::Shared<'d>, &'a segments::Header<resident::Shared<'d>>>,
}

impl<'a, 'd> Scoped<'a, 'd> {
    pub(in crate::reply) fn new(pool: &'a segments::Pool<resident::Shared<'d>>) -> Self {
        Self {
            segments: pool.scoped(),
        }
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn segment(&self, index: usize) -> Option<&[u8]> {
        self.segments.segments().nth(index).map(AsRef::as_ref)
    }

    pub fn segments(&self) -> impl ExactSizeIterator<Item = &[u8]> {
        self.segments.segments().map(AsRef::as_ref)
    }

    pub fn contiguous(&self) -> Option<&[u8]> {
        self.segments.contiguous()
    }

    pub fn reader(&self) -> scatter::Reader<'_, segments::Iter<'_, resident::Shared<'d>>> {
        scatter::Reader::new(self.segments.segments())
    }

    pub fn suffix<const N: usize>(&self) -> Option<[u8; N]> {
        raw::Chunks::suffix(&self.segments)
    }

    pub(in crate::reply) fn pop(&mut self) -> Option<resident::Shared<'d>> {
        self.segments.pop_segment()
    }

    pub(in crate::reply) fn replace_with(
        &mut self,
        owner: resident::Shared<'d>,
    ) -> Result<(), capacity::Error> {
        self.segments.clear();
        self.push(owner)
    }

    pub(in crate::reply) fn push(
        &mut self,
        data: resident::Shared<'d>,
    ) -> Result<(), capacity::Error> {
        match self.segments.try_push(data) {
            Ok(_) => Ok(()),
            Err(rejected) => Err(capacity::Error::ResponseSegments(rejected.discard())),
        }
    }
}

const _: () = assert!(
    mem::size_of::<Scoped<'static, 'static>>()
        == mem::size_of::<
            segments::Chain<
                resident::Shared<'static>,
                &'static segments::Header<resident::Shared<'static>>,
            >,
        >()
);
