use core::{marker, mem};

use lower::{scratch, stage, tagged};
use o3::buffer::pool;

use crate::field::{
    head,
    metadata::{
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

#[doc(hidden)]
pub struct Input<B: storage::Block> {
    pub(in crate::field::metadata) lease: pool::Span,
    block: marker::PhantomData<B>,
    pub(super) phase: stage::Stage,
    pub(in crate::field::metadata) indexing: crate::field::Indexing,
}

impl<B: storage::Block> Input<B> {
    pub fn new(writer: storage::Writer<B>) -> Self {
        Self {
            lease: writer.lease,
            block: marker::PhantomData,
            phase: stage::Stage::Ready,
            indexing: crate::field::Indexing::Allowed,
        }
    }

    pub fn try_ready(&self) -> Result<(), lower::State> {
        self.phase.try_ready()
    }

    pub fn try_scratch(&mut self, scratch: &mut storage::Scratch) -> Result<(), lower::State> {
        self.try_ready()?;
        let mut bytes = mem::take(&mut scratch.authority);
        bytes.clear();
        self.phase = stage::Stage::Scratch(scratch::Scratch::new(bytes));
        Ok(())
    }

    pub(in crate::field::metadata) fn try_tagged(
        &mut self,
        known: head::Name,
        tag: storage::Tag,
        indexing: crate::field::Indexing,
    ) -> Result<(), lower::Error> {
        self.try_ready()?;
        let append = pool::Append::try_prefixed(&mut self.lease, tag.prefix_u32(0, indexing), [])
            .map_err(|cause| lower::Error::Capacity { cause })?;
        self.phase = stage::Stage::Tagged(tagged::Tagged::begin(append, known, tag, indexing));
        Ok(())
    }

    pub fn string(
        &mut self,
        scratch: &mut storage::Scratch,
        cursor: strings::Cursor,
        input: &[u8],
    ) -> lower::Step {
        let lease = &mut self.lease;
        let (phase, step) = match mem::replace(&mut self.phase, stage::Stage::Ready) {
            stage::Stage::Scratch(bytes) => {
                let (bytes, step) = bytes.string(cursor, input);
                match step {
                    lower::Step::Store { cause } => {
                        let mut bytes = bytes.into_vec();
                        bytes.clear();
                        mem::swap(&mut bytes, &mut scratch.authority);
                        (stage::Stage::Ready, lower::Step::Store { cause })
                    }
                    step => (stage::Stage::Scratch(bytes), step),
                }
            }
            stage::Stage::Tagged(field) => field.string(lease, cursor, input),
            phase => {
                let cause = lower::Error::State(phase.state());
                (phase, lower::Step::Store { cause })
            }
        };
        self.phase = phase;
        step
    }

    /// The retained class selects validation; direct completion only observes the value.
    pub(in crate::field::metadata) fn try_finish<P, A>(
        &mut self,
        state: &mut state::Decoder<P>,
        scratch: &mut storage::Scratch,
    ) -> Result<(), lower::Error>
    where
        P: rules::Plan,
        A: rules::Push,
    {
        let indexing = self.indexing;
        match mem::replace(&mut self.phase, stage::Stage::Ready) {
            stage::Stage::Scratch(bytes) => {
                let (mut bytes, check) = bytes.into_parts();
                rules::Validate::value(state, check);
                let value = record::Span::from_slice(&bytes);
                let keep = state.retain::<A>(
                    head::Name::Authority,
                    record::View::new(record::Span::from_slice(&[]), value).with_index(indexing),
                    self.lease.as_slice(),
                    &bytes,
                );
                match keep {
                    rules::Retention::Discard => bytes.clear(),
                    rules::Retention::Retain => state.store(
                        head::Name::Authority,
                        rules::Store::Scratch,
                        0..bytes.len(),
                        self.lease.as_slice(),
                    ),
                }
                mem::swap(&mut bytes, &mut scratch.authority);
                scratch.indexing = indexing;
                Ok(())
            }
            stage::Stage::Tagged(field) => {
                field.try_finish::<P, A>(&mut self.lease, scratch, state)
            }
            stage::Stage::Ready => Err(lower::Error::State(lower::State::Ready)),
        }
    }

    pub(in crate::field::metadata) fn try_omit<A: rules::Push>(
        &mut self,
        state: &mut fixed::Discard,
        scratch: &mut storage::Scratch,
    ) -> Result<(), lower::Error> {
        match mem::replace(&mut self.phase, stage::Stage::Ready) {
            stage::Stage::Tagged(field) => field.try_omit::<A>(&mut self.lease, scratch, state),
            phase => {
                let cause = lower::Error::State(phase.state());
                self.phase = phase;
                Err(cause)
            }
        }
    }

    pub fn rollback(&mut self, scratch: &mut storage::Scratch) {
        let lease = &mut self.lease;
        match mem::replace(&mut self.phase, stage::Stage::Ready) {
            stage::Stage::Scratch(bytes) => {
                let mut bytes = bytes.into_vec();
                bytes.clear();
                mem::swap(&mut bytes, &mut scratch.authority);
            }
            stage::Stage::Tagged(field) => field.rollback(lease),
            stage::Stage::Ready => {}
        }
    }

    pub fn try_seal(self) -> Result<storage::Writer<B>, lower::State> {
        let Self { lease, phase, .. } = self;
        phase.try_ready()?;
        Ok(storage::Writer::from_lease(lease))
    }

    pub fn recycle(mut self, scratch: &mut storage::Scratch) {
        self.rollback(scratch);
        self.lease.recycle();
    }
}
