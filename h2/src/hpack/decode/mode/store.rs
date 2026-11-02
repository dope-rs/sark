use core::{marker::PhantomData, mem};

use decode::mode::{self, Active, Facts, Field, Flow, Kind, Name, NameRoute, Route, Value};
use sark_http::field::metadata::{block, strings::Cursor};

use crate::hpack::decode::{self, limit, sink, strings};

#[derive(Clone, Copy)]
enum State<R, D> {
    PlainSkip,
    PlainBuffered(R),
    PlainDirect(D),
    IndexedSkip,
    IndexedBuffered(R),
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Store<R, D>(PhantomData<fn() -> (R, D)>);

#[derive(Clone, Copy)]
pub(crate) struct Storage<R, D> {
    facts: Facts,
    state: State<R, D>,
}

#[derive(Clone, Copy)]
pub(crate) struct Direct(());

const _: () = assert!(mem::size_of::<Store<usize, ()>>() == 0);
const _: () = assert!(mem::size_of::<Direct>() == 0);
const _: () = assert!(mem::size_of::<Storage<usize, ()>>() == 4 * mem::size_of::<usize>());

impl<R: Copy, D: Copy> Store<R, D> {
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

    fn buffered<S>(
        mut sink: S,
        budget: &mut limit::Budget,
        rule: R,
        facts: Facts,
        name: block::Named<'_>,
        value: block::Valued<'_>,
    ) -> Result<S, (S, S::Error)>
    where
        S: sink::Store<Rule = R, Completion = D>,
    {
        match budget.admit(facts.len, value.span().len()) {
            limit::Admission::Admitted => sink.field(rule, value, facts.index.indexing(), name),
            limit::Admission::Exceeded => {
                sink.rollback();
                Ok(sink)
            }
        }
    }
}

impl<R: Copy, D: Copy> Kind for Store<R, D> {
    type Direct = Direct;
    type Active = Storage<R, D>;

    fn new() -> Self {
        Self(PhantomData)
    }

    fn name(self) -> NameRoute {
        NameRoute::Retain
    }
}

impl<R: Copy, D: Copy> Active for Storage<R, D> {
    type Direct = Direct;

    fn route(self) -> Route<Self::Direct> {
        match self.state {
            State::PlainSkip => Route::Validate,
            State::PlainBuffered(_) | State::IndexedSkip | State::IndexedBuffered(_) => {
                Route::Retain
            }
            State::PlainDirect(_) => Route::Direct(Direct(())),
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

impl<R, D, S> Flow<S> for Store<R, D>
where
    R: Copy,
    D: Copy,
    S: sink::Store<Rule = R, Completion = D>,
{
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
                        sink::Indexed::Skip => State::IndexedSkip,
                        sink::Indexed::Buffered(rule) => State::IndexedBuffered(rule),
                    },
                    sink,
                )
            }
            mode::Index::Plain | mode::Index::Never => {
                let (sink, begin) = sink.begin(span, facts.index.indexing())?;
                (
                    match begin {
                        sink::Begin::Skip => State::PlainSkip,
                        sink::Begin::Buffered(rule) => State::PlainBuffered(rule),
                        sink::Begin::Direct(done) => State::PlainDirect(done),
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
            sink::Indexed::Skip => Self::skip(sink, budget, facts.len, value),
            sink::Indexed::Buffered(rule) => Self::buffered(sink, budget, rule, facts, name, value),
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
            State::PlainBuffered(rule) | State::IndexedBuffered(rule) => match field.value {
                Value::Retained(value) => {
                    return Self::buffered(sink, budget, rule, facts, field.name, value);
                }
                Value::Counted(len) => {
                    budget.exceed(facts.len, len);
                    sink.rollback();
                }
            },
            State::PlainDirect(done) => match budget.admit(facts.len, field.value.len()) {
                limit::Admission::Admitted => return sink.direct(done),
                limit::Admission::Exceeded => sink.rollback(),
            },
        }
        Ok(sink)
    }
}
