use o3::{
    buffer,
    buffer::{segments, storage},
};

use crate::message;

#[derive(Clone, Copy)]
pub struct View<'a, C: buffer::raw::Segment> {
    payload: &'a message::Payload<C>,
}

impl<'a, C: buffer::raw::Segment> View<'a, C> {
    pub(super) fn new(payload: &'a message::Payload<C>) -> Self {
        Self { payload }
    }

    pub fn len(self) -> usize {
        self.payload.len()
    }

    pub fn is_empty(self) -> bool {
        self.payload.is_empty()
    }

    pub fn contiguous(self) -> Option<&'a [u8]> {
        self.payload.contiguous()
    }

    pub fn segments(self) -> segments::Iter<'a, C> {
        self.payload.segments()
    }

    pub fn cursor(self) -> message::Cursor<'a, C> {
        message::Cursor::new(self.payload)
    }
}

const _: () = assert!(
    size_of::<View<'static, storage::Shared>>() == size_of::<usize>()
        && size_of::<Option<View<'static, storage::Shared>>>() == size_of::<usize>()
);
