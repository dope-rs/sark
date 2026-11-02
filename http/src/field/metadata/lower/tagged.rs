use core::ops;

use o3::buffer::{pool, write};
use write::{SpareExt as _, consume, edit};

use crate::field::{
    head,
    metadata::{
        block,
        huffman::decoder,
        lower::{self, stage},
        map::record,
        section::{
            rules,
            state::{self, fixed},
            storage,
        },
        strings,
    },
};

#[must_use = "continue or roll back the open tagged field"]
struct Value {
    start: usize,
    value_at: usize,
    known: head::Name,
    tag: storage::Tag,
    indexing: crate::field::Indexing,
    scan: block::Value,
}

#[must_use = "finish or roll back the stored tagged field"]
struct Stored {
    value: Value,
    end: usize,
}

#[must_use = "finish, continue, or roll back the tagged field"]
#[repr(transparent)]
pub(super) struct Tagged(State);

enum State {
    Open(Value),
    Stored(Stored),
}

#[must_use = "apply the tagged field transition"]
enum Next {
    Open {
        field: Value,
        step: lower::Step,
    },
    Stored {
        field: Stored,
        cursor: strings::Cursor,
    },
}

impl Tagged {
    pub(super) fn begin(
        append: pool::Append<'_, ([u8; storage::Tag::PREFIX_LEN], [&[u8]; 0]), pool::Span>,
        known: head::Name,
        tag: storage::Tag,
        indexing: crate::field::Indexing,
    ) -> Self {
        let start = append.as_slice().len();
        let value_at = start + storage::Tag::PREFIX_LEN;
        append.commit();
        Self(State::Open(Value {
            start,
            value_at,
            known,
            tag,
            indexing,
            scan: block::Value::new(),
        }))
    }

    pub(super) fn string(
        self,
        lease: &mut pool::Span,
        cursor: strings::Cursor,
        input: &[u8],
    ) -> (stage::Stage, lower::Step) {
        match self.0 {
            State::Open(field) => match field.string(lease, cursor, input) {
                Next::Open {
                    field,
                    step: lower::Step::Store { cause },
                } => {
                    lease.truncate(field.start);
                    (stage::Stage::Ready, lower::Step::Store { cause })
                }
                Next::Open { field, step } => {
                    (stage::Stage::Tagged(Self(State::Open(field))), step)
                }
                Next::Stored { field, cursor } => (
                    stage::Stage::Tagged(Self(State::Stored(field))),
                    lower::Step::Ready { cursor },
                ),
            },
            State::Stored(field) => (
                stage::Stage::Tagged(Self(State::Stored(field))),
                lower::Step::Store {
                    cause: lower::Error::State(lower::State::Stored),
                },
            ),
        }
    }

    pub(super) fn state(&self) -> lower::State {
        match self.0 {
            State::Open(_) => lower::State::Tagged,
            State::Stored(_) => lower::State::Stored,
        }
    }

    fn try_stored(self, lease: &mut pool::Span) -> Result<Stored, lower::Error> {
        match self.0 {
            State::Open(field) => {
                if lease.len() != field.value_at {
                    lease.truncate(field.start);
                    return Err(lower::Error::State(lower::State::Tagged));
                }
                Ok(field.stored())
            }
            State::Stored(field) => Ok(field),
        }
    }

    pub(super) fn try_finish<P, A>(
        self,
        lease: &mut pool::Span,
        scratch: &storage::Scratch,
        state: &mut state::Decoder<P>,
    ) -> Result<(), lower::Error>
    where
        P: rules::Plan,
        A: rules::Push,
    {
        self.try_stored(lease)?
            .try_finish::<P, A>(lease, scratch, state)
    }

    pub(super) fn try_omit<A: rules::Push>(
        self,
        lease: &mut pool::Span,
        scratch: &storage::Scratch,
        state: &mut fixed::Discard,
    ) -> Result<(), lower::Error> {
        self.try_stored(lease)?.try_omit::<A>(lease, scratch, state)
    }

    pub(super) fn rollback(self, lease: &mut pool::Span) {
        match self.0 {
            State::Open(field) => lease.truncate(field.start),
            State::Stored(field) => lease.truncate(field.value.start),
        }
    }
}

impl Value {
    fn stored(self) -> Stored {
        Stored {
            end: self.value_at,
            value: self,
        }
    }

    /// The edit admits bytes within the pool capacity.
    fn string(self, lease: &mut pool::Span, mut cursor: strings::Cursor, input: &[u8]) -> Next {
        let current = lease.len();
        let edit = match lease.try_prefix::<{ storage::Tag::PREFIX_LEN }>(self.start) {
            Ok(edit) => edit,
            Err(cause) => return self.store(lower::Error::Edit(cause)),
        };
        let before = cursor;
        let mut next = match cursor.feed_into(input, edit) {
            Ok((_, edit)) => self.ready(current, cursor, edit),
            Err(failure) => self.failed(current, before, input.len(), failure),
        };
        let scan = match &mut next {
            Next::Open { field, .. } => &mut field.scan,
            Next::Stored { field, .. } => &mut field.value.scan,
        };
        // SAFETY: this owner's edit only appends on commit; a failed edit leaves
        // its original length intact. Rollback occurs after this scan returns.
        scan.extend(unsafe { lease.as_slice().get_unchecked(current..) });
        next
    }

    fn ready(
        self,
        current: usize,
        cursor: strings::Cursor,
        mut edit: edit::Edit<'_, { storage::Tag::PREFIX_LEN }>,
    ) -> Next {
        let added = edit.len();
        match cursor.completion() {
            strings::Completion::Pending => {
                edit.commit();
                return self.open(lower::Step::Ready { cursor });
            }
            strings::Completion::Complete => {}
        }
        edit.prefix().copy_from_slice(
            &self
                .tag
                .prefix_u32((current - self.value_at + added) as u32, self.indexing),
        );
        let stored = Stored {
            end: current + added,
            value: self,
        };
        edit.commit();
        Next::Stored {
            field: stored,
            cursor,
        }
    }

    fn failed(
        self,
        current: usize,
        before: strings::Cursor,
        len: usize,
        failure: decoder::Failure<edit::Edit<'_, { storage::Tag::PREFIX_LEN }>, consume::Error>,
    ) -> Next {
        let decoder::Failure {
            sink,
            cause,
            progress,
        } = failure;
        let stored = [current, current + sink.len()];
        let input = [len, progress.consumed(), progress.decoded()];
        match cause {
            decoder::DecodeError::InvalidEncoding(cause) => {
                let cause = decoder::DecodeError::InvalidEncoding(cause);
                self.open(lower::Step::Decode { cause, progress })
            }
            decoder::DecodeError::Sink(cause) => self.store(lower::Error::SinkProgress {
                cause,
                stored,
                remaining: before.remaining(),
                input,
            }),
        }
    }

    fn open(self, step: lower::Step) -> Next {
        Next::Open { field: self, step }
    }

    fn store(self, cause: lower::Error) -> Next {
        self.open(lower::Step::Store { cause })
    }
}

impl Stored {
    fn try_finish<P, A>(
        self,
        lease: &mut pool::Span,
        scratch: &storage::Scratch,
        state: &mut state::Decoder<P>,
    ) -> Result<(), lower::Error>
    where
        P: rules::Plan,
        A: rules::Push,
    {
        let range = self.range();
        let stored = self.value;
        let Some(value) = lease.as_slice().get(range.clone()) else {
            return Err(lower::Error::Layout([
                stored.value_at,
                self.end,
                lease.len(),
            ]));
        };
        let value = record::Span::from_slice(value);
        rules::Validate::value(state, stored.scan.finish());
        let field =
            record::View::new(record::Span::from_slice(&[]), value).with_index(stored.indexing);
        let keep = state.retain::<A>(stored.known, field, lease.as_slice(), scratch.authority());
        match keep {
            rules::Retention::Discard => lease.truncate(stored.start),
            rules::Retention::Retain => state.store(
                stored.known,
                rules::Store::Tagged(stored.tag),
                range,
                lease.as_slice(),
            ),
        }
        Ok(())
    }

    fn try_omit<A: rules::Push>(
        self,
        lease: &mut pool::Span,
        scratch: &storage::Scratch,
        state: &mut fixed::Discard,
    ) -> Result<(), lower::Error> {
        let range = self.range();
        let stored = self.value;
        let Some(value) = lease.as_slice().get(range.clone()) else {
            return Err(lower::Error::Layout([
                stored.value_at,
                self.end,
                lease.len(),
            ]));
        };
        let value = record::Span::from_slice(value);
        rules::Validate::value(state, stored.scan.finish());
        let field =
            record::View::new(record::Span::from_slice(&[]), value).with_index(stored.indexing);
        let keep = state.retain::<A>(stored.known, field, lease.as_slice(), scratch.authority());
        match keep {
            rules::Retention::Discard => lease.truncate(stored.start),
            rules::Retention::Retain => state.store(range),
        }
        Ok(())
    }

    fn range(&self) -> ops::Range<usize> {
        self.value.value_at..self.end
    }
}
