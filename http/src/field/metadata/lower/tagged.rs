use core::ops;

use o3::buffer::{pool, write};
use write::SpareExt as _;

use crate::field::{
    head,
    metadata::{
        block,
        huffman::decoder,
        lower,
        map::record,
        section::{
            rules,
            state::{self, fixed},
            storage,
        },
        strings,
    },
};

#[must_use = "finish, continue, or roll back the tagged field"]
pub(super) struct Tagged {
    start: usize,
    known: head::Name,
    tag: storage::Tag,
    indexing: crate::field::Indexing,
    scan: block::Value,
    stored: bool,
}

impl Tagged {
    pub(super) fn begin(
        append: pool::Append<'_, ([u8; storage::Tag::PREFIX_LEN], [&[u8]; 0]), pool::Span>,
        known: head::Name,
        tag: storage::Tag,
        indexing: crate::field::Indexing,
    ) -> Self {
        let start = append.as_slice().len();
        append.commit();
        Self {
            start,
            known,
            tag,
            indexing,
            scan: block::Value::new(),
            stored: false,
        }
    }

    pub(super) fn stored(&self) -> bool {
        self.stored
    }

    pub(super) fn state(&self) -> lower::State {
        if self.stored {
            lower::State::Stored
        } else {
            lower::State::Tagged
        }
    }

    fn value_at(&self) -> usize {
        self.start + storage::Tag::PREFIX_LEN
    }

    pub(super) fn try_string(
        &mut self,
        lease: &mut pool::Span,
        cursor: &mut strings::Cursor,
        input: &[u8],
    ) -> Result<(), lower::StringError> {
        let current = lease.len();
        let edit = lease
            .try_prefix::<{ storage::Tag::PREFIX_LEN }>(self.start)
            .map_err(|cause| lower::StringError::Store(lower::Error::Edit(cause)))?;
        let remaining = cursor.remaining();
        match cursor.feed_into(input, edit) {
            Ok((_, mut edit)) => {
                if matches!(cursor.completion(), strings::Completion::Complete) {
                    let len = current - self.value_at() + edit.len();
                    edit.prefix()
                        .copy_from_slice(&self.tag.prefix_u32(len as u32, self.indexing));
                    self.stored = true;
                }
                edit.commit();
                // SAFETY: this edit only appends to the exclusively held span.
                self.scan
                    .extend(unsafe { lease.as_slice().get_unchecked(current..) });
                Ok(())
            }
            Err(decoder::Failure {
                sink,
                cause,
                progress,
            }) => match cause {
                decoder::DecodeError::InvalidEncoding(cause) => Err(lower::StringError::Decode {
                    cause: decoder::DecodeError::InvalidEncoding(cause),
                    progress,
                }),
                decoder::DecodeError::Sink(cause) => {
                    Err(lower::StringError::Store(lower::Error::SinkProgress {
                        cause,
                        stored: [current, current + sink.len()],
                        remaining,
                        input: [input.len(), progress.consumed(), progress.decoded()],
                    }))
                }
            },
        }
    }

    fn try_range(&self, lease: &mut pool::Span) -> Result<ops::Range<usize>, lower::Error> {
        let range = self.value_at()..lease.len();
        if !self.stored && range.start != range.end {
            lease.truncate(self.start);
            return Err(lower::Error::State(lower::State::Tagged));
        }
        Ok(range)
    }

    fn try_field<'a>(
        &self,
        bytes: &'a [u8],
        range: &ops::Range<usize>,
    ) -> Result<record::View<'a>, lower::Error> {
        let value = bytes.get(range.clone()).ok_or(lower::Error::Layout([
            range.start,
            range.end,
            bytes.len(),
        ]))?;
        Ok(record::View::new(
            record::Span::from_slice(&[]),
            record::Span::from_slice(value),
        )
        .with_index(self.indexing))
    }

    pub(super) fn try_finish<P: rules::Plan, A: rules::Push>(
        self,
        lease: &mut pool::Span,
        scratch: &storage::Scratch,
        state: &mut state::Decoder<P>,
    ) -> Result<(), lower::Error> {
        let range = self.try_range(lease)?;
        let bytes = lease.as_slice();
        let field = self.try_field(bytes, &range)?;
        rules::Validate::value(state, self.scan.finish());
        match state.retain::<A>(self.known, field, bytes, scratch.authority()) {
            rules::Retention::Discard => self.rollback(lease),
            rules::Retention::Retain => {
                state.store(self.known, rules::Store::Tagged(self.tag), range, bytes)
            }
        }
        Ok(())
    }

    pub(super) fn try_omit<A: rules::Push>(
        self,
        lease: &mut pool::Span,
        scratch: &storage::Scratch,
        state: &mut fixed::Discard,
    ) -> Result<(), lower::Error> {
        let range = self.try_range(lease)?;
        let bytes = lease.as_slice();
        let field = self.try_field(bytes, &range)?;
        rules::Validate::value(state, self.scan.finish());
        match state.retain::<A>(self.known, field, bytes, scratch.authority()) {
            rules::Retention::Discard => self.rollback(lease),
            rules::Retention::Retain => state.store(range),
        }
        Ok(())
    }

    pub(super) fn rollback(self, lease: &mut pool::Span) {
        lease.truncate(self.start);
    }
}
