use o3::buffer::{
    Bytes, CapacityError, Owned, PrefixLength, Retained, SegmentQueue, Shared, ValidatedPrefix,
};

pub(crate) struct RetainedSegments {
    chunks: SegmentQueue<Bytes<Retained>>,
    byte_capacity: usize,
    chunk_capacity: usize,
}

impl RetainedSegments {
    pub(crate) fn new(byte_capacity: usize, chunk_capacity: usize) -> Self {
        debug_assert!(
            byte_capacity != 0,
            "retained byte capacity must be positive"
        );
        debug_assert!(
            chunk_capacity != 0,
            "retained chunk capacity must be positive"
        );
        Self {
            chunks: SegmentQueue::new(),
            byte_capacity,
            chunk_capacity,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.chunks.len()
    }

    pub(crate) fn remaining(&self) -> usize {
        self.byte_capacity - self.len()
    }

    pub(crate) fn push(&mut self, chunk: Bytes<Retained>) -> Result<(), Bytes<Retained>> {
        if chunk.is_empty() {
            return Ok(());
        }
        if chunk.len() > self.remaining() || self.chunks.segment_count() == self.chunk_capacity {
            return Err(chunk);
        }
        self.chunks.try_push_back(chunk)
    }

    pub(crate) fn copy_range_into(&self, offset: usize, output: &mut [u8]) -> bool {
        self.chunks.copy_range_into(0, offset, output)
    }

    pub(crate) fn contiguous_range(&self, offset: usize, len: usize) -> Option<&[u8]> {
        if len == 0 {
            return self.chunks.range_available(0, offset, 0).then_some(&[]);
        }
        let (chunk, range) = self.chunks.contiguous_segment(0, offset, len)?;
        Some(&chunk.as_slice()[range])
    }

    pub(crate) fn for_each_range(
        &self,
        offset: usize,
        len: usize,
        visit: impl FnMut(&[u8]),
    ) -> bool {
        self.chunks.for_each_range(0, offset, len, visit)
    }

    pub(crate) fn retained_range(
        &self,
        offset: usize,
        len: usize,
    ) -> Result<Option<Bytes<Retained>>, CapacityError> {
        if !self.chunks.range_available(0, offset, len) {
            return Ok(None);
        }
        if len == 0 {
            return Ok(Some(Bytes::from(Shared::new())));
        }

        if let Some((chunk, range)) = self.chunks.contiguous_segment(0, offset, len) {
            return Ok(chunk.clone().get(range));
        }

        let mut owned = Owned::try_with_capacity(len)?;
        let mut copy_error = None;
        let copied = self.chunks.for_each_range(0, offset, len, |bytes| {
            if copy_error.is_none()
                && let Err(error) = owned.try_extend_from_slice(bytes)
            {
                copy_error = Some(error);
            }
        });
        if let Some(error) = copy_error {
            return Err(error);
        }
        debug_assert!(copied);
        Ok(copied.then(|| Bytes::from(owned.freeze())))
    }

    pub(crate) fn try_consume(&mut self, amount: usize) -> bool {
        self.chunks.try_consume_front(amount)
    }

    pub(crate) fn prepare_consume(
        &mut self,
        amount: usize,
    ) -> Result<ValidatedPrefix<'_, Self, impl FnOnce(&mut Self, usize)>, CapacityError> {
        ValidatedPrefix::try_new(self, amount, Self::consume_valid)
    }

    fn consume_valid(&mut self, amount: usize) {
        let consumed = self.chunks.try_consume_front(amount);
        debug_assert!(consumed);
    }
}

impl PrefixLength for RetainedSegments {
    fn prefix_len(&self) -> usize {
        self.len()
    }
}

#[cfg(all(test, target_pointer_width = "64"))]
#[test]
fn retained_segments_preserve_their_layout() {
    assert_eq!(std::mem::size_of::<RetainedSegments>(), 56);
}
