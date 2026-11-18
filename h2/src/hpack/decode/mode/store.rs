use core::mem;

use decode::mode::{self, Active, Facts, Field, Flow, Kind, Name, NameRoute, Route, Value};
use sark_http::field::metadata::{
    block,
    section::state::decode::{Indexed, Start},
    strings::Cursor,
};

use crate::hpack::decode::{self, limit, sink, strings};

#[derive(Clone, Copy)]
enum State {
    PlainSkip,
    PlainBuffered,
    PlainDirect,
    IndexedSkip,
    IndexedBuffered,
}

#[derive(Clone, Copy)]
pub(crate) struct Store;

#[derive(Clone, Copy)]
pub(crate) struct Storage {
    facts: Facts,
    state: State,
}

#[derive(Clone, Copy)]
pub(crate) struct Direct(());

const _: () = assert!(mem::size_of::<Store>() == 0);
const _: () = assert!(mem::size_of::<Direct>() == 0);
const _: () = assert!(mem::size_of::<Storage>() == 3 * mem::size_of::<usize>());

impl Store {
    fn skip<S: sink::Store>(
        mut sink: S,
        budget: &mut limit::Budget,
        name_len: usize,
        value: block::Valued<'_>,
    ) -> Result<S, (S, S::Error)> {
        let result = sink.validate(value.validation());
        let ((), sink) = mode::carry(sink, result)?;
        budget.admit(name_len, value.span().len());
        Ok(sink)
    }

    fn buffered<S: sink::Store>(
        mut sink: S,
        budget: &mut limit::Budget,
        facts: Facts,
        name: block::Named<'_>,
        value: block::Valued<'_>,
    ) -> Result<S, (S, S::Error)> {
        match budget.admit(facts.len, value.span().len()) {
            limit::Admission::Admitted => sink.field(value, facts.index.indexing(), name),
            limit::Admission::Exceeded => {
                sink.rollback();
                Ok(sink)
            }
        }
    }
}

impl Kind for Store {
    type Direct = Direct;
    type Active = Storage;

    fn new() -> Self {
        Self
    }

    fn name(self) -> NameRoute {
        NameRoute::Retain
    }
}

impl Active for Storage {
    type Direct = Direct;

    fn route(self) -> Route<Self::Direct> {
        match self.state {
            State::PlainSkip => Route::Validate,
            State::PlainBuffered | State::IndexedSkip | State::IndexedBuffered => Route::Retain,
            State::PlainDirect => Route::Direct(Direct(())),
        }
    }

    fn facts(self) -> Facts {
        self.facts
    }
}

impl<S: sink::Direct> mode::Direct<S> for Direct {
    type Error = S::Error;

    fn literal(
        self,
        sink: &mut S,
        input: strings::Encoded<'_>,
    ) -> Result<strings::Decoded, Self::Error> {
        let Self(()) = self;
        input.decode_to(sink)
    }

    fn string(self, sink: &mut S, string: &mut Cursor, input: &[u8]) -> Result<(), Self::Error> {
        let Self(()) = self;
        sink.string(string, input)
    }
}

impl<S: sink::Store> Flow<S> for Store {
    type Error = S::Error;

    fn validate(sink: &mut S, value: block::Validation) -> Result<(), Self::Error> {
        match value {
            block::Validation::Valid => Ok(()),
            block::Validation::Malformed | block::Validation::RejectPush => sink.validate(value),
        }
    }

    fn begin(self, sink: S, name: Name<'_>) -> Result<(Self::Active, S), (S, Self::Error)> {
        let Name { span, facts } = name;
        let (state, sink) = match facts.index {
            mode::Index::Indexed => {
                let (sink, indexed) = sink.indexed(span)?;
                (
                    match indexed {
                        Indexed::Skip => State::IndexedSkip,
                        Indexed::Buffered(()) => State::IndexedBuffered,
                    },
                    sink,
                )
            }
            mode::Index::Plain | mode::Index::Never => {
                let (sink, begin) = sink.begin(span, facts.index.indexing())?;
                (
                    match begin {
                        Start::Skip => State::PlainSkip,
                        Start::Buffered(()) => State::PlainBuffered,
                        Start::Direct(()) | Start::Scratch(()) => State::PlainDirect,
                    },
                    sink,
                )
            }
        };
        Ok((Storage { facts, state }, sink))
    }

    fn field(
        sink: S,
        budget: &mut limit::Budget,
        name: block::Named<'_>,
        value: block::Valued<'_>,
    ) -> Result<S, (S, Self::Error)> {
        let facts = Facts {
            len: name.span().len(),
            index: mode::Index::Indexed,
        };
        let (sink, indexed) = sink.indexed(Some(name))?;
        match indexed {
            Indexed::Skip => Self::skip(sink, budget, facts.len, value),
            Indexed::Buffered(()) => Self::buffered(sink, budget, facts, name, value),
        }
    }

    fn finish(
        active: Self::Active,
        mut sink: S,
        budget: &mut limit::Budget,
        field: Field<'_>,
    ) -> Result<S, (S, Self::Error)> {
        let facts = active.facts;
        match active.state {
            State::PlainSkip | State::IndexedSkip => match field.value {
                Value::Retained(value) => return Self::skip(sink, budget, facts.len, value),
                Value::Counted(len) => {
                    budget.admit(facts.len, len);
                }
            },
            State::PlainBuffered | State::IndexedBuffered => match field.value {
                Value::Retained(value) => {
                    return Self::buffered(sink, budget, facts, field.name, value);
                }
                Value::Counted(len) => {
                    budget.exceed(facts.len, len);
                    sink.rollback();
                }
            },
            State::PlainDirect => match budget.admit(facts.len, field.value.len()) {
                limit::Admission::Admitted => return sink.direct(),
                limit::Admission::Exceeded => sink.rollback(),
            },
        }
        Ok(sink)
    }
}
