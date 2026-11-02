use core::{convert::Infallible, mem};

use crate::field::metadata::{map::record, strings};

#[doc(hidden)]
pub mod fixed;
#[doc(hidden)]
pub use input::Input;
mod input;
pub mod plain;
mod plan;
mod scratch;
mod stage;
mod state;
pub use state::State;
mod tagged;

use o3::buffer::{
    pool,
    write::{self, consume},
};
use plan::Plan;
use write::edit;

use crate::field::{
    head,
    metadata::{
        block,
        huffman::decoder,
        section::{self, rules, state::Decoder, storage},
    },
};

/// Failure while lowering a decoded compressed field into request-owned storage.
#[derive(Clone, PartialEq, Eq)]
#[doc(hidden)]
#[derive(Debug)]
pub enum Error {
    State(State),
    Name {
        expected: head::Name,
        actual: head::Name,
    },
    Plain(plain::Error),
    Layout([usize; 3]),
    Prefix(storage::Tag, usize),
    Capacity {
        cause: consume::Error,
    },
    Scratch(storage::Error),
    Edit(edit::Error),
    Write(consume::Error),
    SinkProgress {
        cause: consume::Error,
        stored: [usize; 2],
        remaining: usize,
        input: [usize; 3],
    },
}

impl From<State> for Error {
    fn from(error: State) -> Self {
        Self::State(error)
    }
}

impl From<plain::Error> for Error {
    fn from(error: plain::Error) -> Self {
        Self::Plain(error)
    }
}

#[doc(hidden)]
#[must_use = "handle the direct string transition"]
pub enum Step {
    Ready {
        cursor: strings::Cursor,
    },
    Bound {
        cause: decoder::Error,
    },
    Decode {
        cause: decoder::DecodeError<Infallible>,
        progress: decoder::Progress,
    },
    Store {
        cause: Error,
    },
}

const _: () = assert!(mem::size_of::<Result<(), Error>>() == mem::size_of::<Error>());
const _: () = assert!(mem::size_of::<Step>() == mem::size_of::<Error>());

#[doc(hidden)]
pub struct Lower<'a, P: section::Policy> {
    lease: &'a mut pool::Span,
    scratch: &'a mut storage::Scratch,
    consumer: &'a mut Decoder<P>,
}

#[doc(hidden)]
#[must_use = "commit the prepared field"]
pub struct Push<'a, 'field, P: rules::Plan> {
    scratch: &'a mut storage::Scratch,
    consumer: &'a mut Decoder<P>,
    plan: Plan<'a, 'field, P::Inspect>,
}

impl<'a, P: section::Policy> Lower<'a, P> {
    pub fn new(
        lease: &'a mut pool::Span,
        scratch: &'a mut storage::Scratch,
        consumer: &'a mut Decoder<P>,
    ) -> Self {
        Self {
            lease,
            scratch,
            consumer,
        }
    }
}

impl<'a, P: rules::Plan> Lower<'a, P> {
    pub fn disposition(
        &mut self,
        name: record::Span<'_>,
        proof: block::Name,
    ) -> rules::Decision<P::Inspect> {
        self.consumer.decide(name, proof)
    }

    /// Validates a decoded field whose storage decision is discard.
    pub fn discard<A: rules::Push>(
        &mut self,
        known: head::Name,
        name: record::Span<'_>,
        value: record::Span<'_>,
    ) {
        self.consumer.discard::<A>(
            known,
            record::View::new(name, value),
            self.lease.as_mut_slice(),
            self.scratch.authority(),
        );
    }

    /// Validates a decoded field through its plan-owned inspection token.
    pub fn inspect<A: rules::Push>(
        &mut self,
        token: P::Inspect,
        known: head::Name,
        name: record::Span<'_>,
        value: record::Span<'_>,
    ) {
        self.consumer.inspect::<A>(
            token,
            known,
            record::View::new(name, value),
            self.lease.as_mut_slice(),
            self.scratch.authority(),
        );
    }

    /// Validates and prepares a field whose disposition was already chosen.
    pub fn try_push<'field>(
        self,
        decision: rules::Decision<P::Inspect>,
        name: record::Span<'field>,
        value: record::Span<'field>,
    ) -> Result<Push<'a, 'field, P>, Error> {
        self.try_push_as(decision, name, value, crate::field::Indexing::Allowed)
    }

    pub fn try_push_as<'field>(
        self,
        decision: rules::Decision<P::Inspect>,
        name: record::Span<'field>,
        value: record::Span<'field>,
        indexing: crate::field::Indexing,
    ) -> Result<Push<'a, 'field, P>, Error> {
        let Self {
            lease,
            scratch,
            consumer,
        } = self;
        let field = record::View::new(name, value).with_index(indexing);
        let plan = match decision {
            rules::Decision::Discard(known) => Plan::Discard {
                lease,
                field,
                known,
            },
            rules::Decision::Inspect(token, known) => Plan::Inspect {
                lease,
                field,
                token,
                known,
            },
            rules::Decision::Skip => Plan::Skip,
            rules::Decision::Scratch => {
                let (first, second) = value.parts();
                scratch
                    .try_replace([first, second])
                    .map_err(Error::Scratch)?;
                scratch.indexing = indexing;
                Plan::Scratch { lease, name }
            }
            rules::Decision::Tagged(known, tag) => {
                let len = value.len();
                let prefix = tag
                    .prefix_with(len, indexing)
                    .ok_or(Error::Prefix(tag, len))?;
                let start = lease.len();
                start
                    .checked_add(storage::Tag::PREFIX_LEN)
                    .and_then(|offset| offset.checked_add(len))
                    .ok_or(Error::Layout([start, name.len(), len]))?;
                let (first, second) = value.parts();
                let append = pool::Append::try_prefixed(lease, prefix, [first, second])
                    .map_err(Error::Write)?;
                Plan::Tagged {
                    append,
                    name,
                    known,
                    tag,
                    indexing,
                }
            }
        };
        Ok(Push {
            scratch,
            consumer,
            plan,
        })
    }
}

impl<P: rules::Plan> Push<'_, '_, P> {
    pub fn commit<A: rules::Push>(self) {
        let Self {
            scratch,
            consumer,
            plan,
        } = self;
        match plan {
            Plan::Scratch { lease, name } => {
                let keep = consumer.retain::<A>(
                    head::Name::Authority,
                    record::View::new(name, record::Span::from_slice(scratch.authority()))
                        .with_index(scratch.indexing),
                    lease.as_mut_slice(),
                    scratch.authority(),
                );
                match keep {
                    rules::Retention::Retain => {
                        consumer.store(
                            head::Name::Authority,
                            rules::Store::Scratch,
                            0..scratch.authority().len(),
                            lease.as_mut_slice(),
                        );
                    }
                    rules::Retention::Discard => {
                        scratch.clear();
                    }
                }
            }
            Plan::Tagged {
                append,
                name,
                known,
                tag,
                indexing,
            } => {
                let start = append.as_slice().len();
                let value_at = start + storage::Tag::PREFIX_LEN;
                let (lease, [first, second]) = append.commit();
                let end = lease.len();
                let value = record::Span::new(first, second);
                let keep = consumer.retain::<A>(
                    known,
                    record::View::new(name, value).with_index(indexing),
                    lease.as_slice(),
                    scratch.authority(),
                );
                match keep {
                    rules::Retention::Discard => lease.truncate(start),
                    rules::Retention::Retain => consumer.store(
                        known,
                        rules::Store::Tagged(tag),
                        value_at..end,
                        lease.as_slice(),
                    ),
                }
            }
            Plan::Discard {
                lease,
                field,
                known,
            } => {
                consumer.discard::<A>(known, field, lease.as_mut_slice(), scratch.authority());
            }
            Plan::Inspect {
                lease,
                field,
                token,
                known,
            } => {
                consumer.inspect::<A>(
                    token,
                    known,
                    field,
                    lease.as_mut_slice(),
                    scratch.authority(),
                );
            }
            Plan::Skip => {}
        }
    }
}
