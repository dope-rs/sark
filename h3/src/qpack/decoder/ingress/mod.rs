use core::num::NonZero;
use std::ops::ControlFlow;

use sark_http::{
    field::metadata::{huffman::decoder, integer, strings},
    payload::compress::dynamic::{insert, insert::stages},
};

use crate::qpack::{self, decoder::state, dynamics, statics, stream};

pub mod error;
mod phase;

use error::{Error, Overflow};
use phase::{Action, Location, Phase, Ready, Value};
use stages::{Borrowed, View, Write};

type Loan<'a, S> = stages::Loan<'a, S, Ready, strings::Cursor, Value>;

pub(super) struct State {
    core: dynamics::Core,
    phase: Phase,
    /// Bytes this ingest has committed into its caller's feedback wire.
    growth: qpack::Growth,
}

enum Notify<'a> {
    Ready(state::Candidate<'a>),
    Blocked(stream::Capacity),
}

enum Step {
    Consumed(usize),
    Committed(qpack::Growth),
    Blocked(stream::Capacity),
    Empty,
}

impl State {
    pub(super) fn new(table: dynamics::Table) -> Self {
        let (table, core) = table.into_parts();
        Self {
            core,
            phase: Phase::new(table, Ready::Instruction),
            growth: qpack::Growth::new(0),
        }
    }

    pub(super) fn table(&self) -> dynamics::View<'_> {
        self.core.view(self.phase.table())
    }

    /// Consumes each transport fragment once while its phase retains the table owner.
    pub(super) fn ingest(
        &mut self,
        feedback: &mut state::State,
        limit: usize,
        input: &[u8],
    ) -> (Result<qpack::Progress, Error>, qpack::Growth) {
        self.growth = qpack::Growth::new(0);
        match self.ingest_inner(feedback, limit, input) {
            Ok(progress) => (Ok(progress), self.growth),
            Err(error) => {
                self.phase.reset(Ready::Instruction);
                (Err(error), self.growth)
            }
        }
    }

    fn byte<T>(
        owner: T,
        input: &[u8],
        read: impl FnOnce(T, u8) -> Result<(), Error>,
    ) -> Result<Step, Error> {
        match input.first() {
            Some(&first) => {
                read(owner, first)?;
                Ok(Step::Consumed(1))
            }
            None => Ok(Step::Empty),
        }
    }

    fn commit<T>(
        owner: T,
        feedback: &mut state::State,
        limit: usize,
        apply: impl FnOnce(T) -> Result<(), Error>,
    ) -> Result<Step, Error> {
        let update = match Self::try_notify(feedback, limit)? {
            Notify::Ready(update) => update,
            Notify::Blocked(capacity) => return Ok(Step::Blocked(capacity)),
        };
        apply(owner)?;
        let growth = qpack::Growth::new(update.growth());
        update.commit();
        Ok(Step::Committed(growth))
    }

    fn partial(&mut self, input: &[u8]) -> Option<Result<strings::Progress, Error>> {
        match self.phase.writing() {
            View::Name((string, mut owner)) if string.remaining() > input.len() => Some(
                Self::feed(string, input, owner.writer(), Error::NameEncoding),
            ),
            View::Value((Value::Literal(string), mut owner))
                if string.remaining() > input.len() =>
            {
                Some(Self::feed(
                    string,
                    input,
                    owner.writer(),
                    Error::ValueEncoding,
                ))
            }
            _ => None,
        }
    }

    fn feed(
        string: &mut strings::Cursor,
        input: &[u8],
        writer: insert::Writer<'_>,
        error: Error,
    ) -> Result<strings::Progress, Error> {
        match string.feed_into(input, writer) {
            Ok((progress, _)) => Ok(progress),
            Err(failure) => Err(match failure.cause {
                decoder::DecodeError::InvalidEncoding(decoder::Error) => error,
                decoder::DecodeError::Sink(error) => Error::LiteralWrite(error),
            }),
        }
    }

    fn finish_name(table: Loan<'_, stages::Name>, string: strings::Cursor) -> Result<(), Error> {
        string.finish().map_err(|_| Error::NameEncoding)?;
        table.finish(Value::Header).map_err(Error::TableInsert)
    }

    fn finish_value(table: Loan<'_, stages::Value>, string: strings::Cursor) -> Result<(), Error> {
        string.finish().map_err(|_| Error::ValueEncoding)?;
        table.pending(Value::Commit);
        Ok(())
    }

    fn ingest_inner(
        &mut self,
        feedback: &mut state::State,
        limit: usize,
        input: &[u8],
    ) -> Result<qpack::Progress, Error> {
        let mut position = 0usize;
        let mut insert = qpack::Insertion::None;
        loop {
            let tail = input.get(position..).ok_or(Error::Position {
                position,
                additional: 0,
                available: input.len(),
            })?;
            if !tail.is_empty()
                && let Some(progress) = self.partial(tail)
            {
                progress?;
                position = input.len();
                break;
            }
            let core = &mut self.core;
            let step = match self.phase.view() {
                Borrowed::Ready(Ready::Commit(relative), table) => {
                    Self::commit(table, feedback, limit, |mut table| {
                        core.duplicate(table.table_mut(), relative)
                            .map_err(Error::from)?;
                        table.pending(Ready::Instruction);
                        Ok(())
                    })
                }
                Borrowed::Value(Value::Commit, table) => {
                    Self::commit(table, feedback, limit, |table| {
                        let next = core.next().map_err(Error::from)?;
                        table.commit(Ready::Instruction);
                        core.commit(next);
                        Ok(())
                    })
                }
                Borrowed::Ready(Ready::Instruction, table) => {
                    Self::byte(table, tail, |table, first| {
                        Self::dispatch(core, table, first)
                    })
                }
                Borrowed::Value(Value::Header, table) => Self::byte(table, tail, |table, first| {
                    let encoding = if first & 0x80 == 0 {
                        strings::Encoding::Plain
                    } else {
                        strings::Encoding::Huffman
                    };
                    Self::read(core, first, 0x7f, Action::LiteralValue { table, encoding })
                }),
                Borrowed::Ready(Ready::SetCapacity(number), table) => {
                    Self::push(core, number, Action::SetCapacity(table), tail, position)
                }
                Borrowed::Ready(Ready::Duplicate(number), table) => {
                    Self::push(core, number, Action::Duplicate(table), tail, position)
                }
                Borrowed::Ready(Ready::DynamicName(number), table) => Self::push(
                    core,
                    number,
                    Action::NameReference {
                        table,
                        location: Location::Dynamic,
                    },
                    tail,
                    position,
                ),
                Borrowed::Ready(Ready::StaticName(number), table) => Self::push(
                    core,
                    number,
                    Action::NameReference {
                        table,
                        location: Location::Static,
                    },
                    tail,
                    position,
                ),
                Borrowed::Ready(Ready::PlainName(number), table) => Self::push(
                    core,
                    number,
                    Action::LiteralName {
                        table,
                        encoding: strings::Encoding::Plain,
                    },
                    tail,
                    position,
                ),
                Borrowed::Ready(Ready::HuffmanName(number), table) => Self::push(
                    core,
                    number,
                    Action::LiteralName {
                        table,
                        encoding: strings::Encoding::Huffman,
                    },
                    tail,
                    position,
                ),
                Borrowed::Value(Value::PlainLength(number), table) => Self::push(
                    core,
                    number,
                    Action::LiteralValue {
                        table,
                        encoding: strings::Encoding::Plain,
                    },
                    tail,
                    position,
                ),
                Borrowed::Value(Value::HuffmanLength(number), table) => Self::push(
                    core,
                    number,
                    Action::LiteralValue {
                        table,
                        encoding: strings::Encoding::Huffman,
                    },
                    tail,
                    position,
                ),
                Borrowed::Name(..) | Borrowed::Value(Value::Literal(_), _) if tail.is_empty() => {
                    Ok(Step::Empty)
                }
                Borrowed::Name(mut string, mut table) => {
                    let progress =
                        Self::feed(&mut string, tail, table.writer(), Error::NameEncoding)?;
                    Self::finish_name(table, string)?;
                    Ok(Step::Consumed(progress.consumed()))
                }
                Borrowed::Value(Value::Literal(mut string), mut table) => {
                    let progress =
                        Self::feed(&mut string, tail, table.writer(), Error::ValueEncoding)?;
                    Self::finish_value(table, string)?;
                    Ok(Step::Consumed(progress.consumed()))
                }
            }?;
            match step {
                Step::Consumed(consumed) => {
                    position = Self::advance(position, consumed, input.len())?;
                }
                Step::Committed(growth) => {
                    self.growth = self.growth.add(growth);
                    insert = qpack::Insertion::Committed;
                }
                Step::Blocked(capacity) => {
                    return Ok(qpack::Progress {
                        consumed: position,
                        insertion: insert,
                        feedback: state::Status::Blocked(capacity),
                    });
                }
                Step::Empty => break,
            }
        }
        Ok(qpack::Progress {
            consumed: position,
            insertion: insert,
            feedback: state::Status::Clear,
        })
    }

    fn dispatch(
        core: &dynamics::Core,
        table: Loan<'_, stages::Ready>,
        first: u8,
    ) -> Result<(), Error> {
        if first & 0x80 != 0 {
            return Self::read(
                core,
                first,
                0x3f,
                Action::NameReference {
                    table,
                    location: if first & 0x40 == 0 {
                        Location::Dynamic
                    } else {
                        Location::Static
                    },
                },
            );
        }
        if first & 0xc0 == 0x40 {
            let encoding = if first & 0x20 == 0 {
                strings::Encoding::Plain
            } else {
                strings::Encoding::Huffman
            };
            return Self::read(core, first, 0x1f, Action::LiteralName { table, encoding });
        }
        let action = if first & 0xe0 == 0x20 {
            Action::SetCapacity(table)
        } else {
            Action::Duplicate(table)
        };
        Self::read(core, first, 0x1f, action)
    }

    fn read(core: &dynamics::Core, first: u8, mask: u8, action: Action<'_>) -> Result<(), Error> {
        let prefix = first & mask;
        if prefix < mask {
            Self::apply(core, action, u64::from(prefix))
        } else {
            action.pending(integer::Continuation::new(u64::from(mask)));
            Ok(())
        }
    }

    fn push(
        core: &dynamics::Core,
        mut number: integer::Continuation,
        action: Action<'_>,
        input: &[u8],
        position: usize,
    ) -> Result<Step, Error> {
        let Some(&byte) = input.first() else {
            return Ok(Step::Empty);
        };
        match number.try_push(byte).map_err(|cause| {
            Error::Integer(Overflow {
                position,
                value: cause.value,
                shift: cause.shift,
                byte: cause.byte,
            })
        })? {
            ControlFlow::Break(value) => Self::apply(core, action, value)?,
            ControlFlow::Continue(()) => action.pending(number),
        }
        Ok(Step::Consumed(1))
    }

    fn apply(core: &dynamics::Core, action: Action<'_>, value: u64) -> Result<(), Error> {
        match action {
            Action::SetCapacity(mut table) => {
                if value > usize::MAX as u64 {
                    return Err(Error::CapacityRange { value });
                }
                let capacity = value as usize;
                core.check_limit(capacity, table.table().capacity())
                    .map_err(Error::from)?;
                table.table_mut().edit().set_limit(capacity);
                table.pending(Ready::Instruction);
                Ok(())
            }
            Action::Duplicate(table) => {
                table.pending(Ready::Commit(value));
                Ok(())
            }
            Action::NameReference { table, location } => {
                let name = table.begin().map_err(Error::TableCapacity)?;
                match location {
                    Location::Dynamic => {
                        if value > usize::MAX as u64 {
                            return Err(Error::DynamicReference {
                                relative: value,
                                resident: name.table().entry_count(),
                            });
                        }
                        name.copy(value as usize, Value::Header)
                            .map_err(|error| match error {
                                insert::Error::Index { len, .. } => Error::DynamicReference {
                                    relative: value,
                                    resident: len,
                                },
                                error => Error::TableInsert(error),
                            })
                    }
                    Location::Static => {
                        let entry = statics::STATIC.resolve(value).map_err(Error::from)?;
                        name.set_name(entry.name(), Value::Header)
                            .map_err(Error::TableInsert)
                    }
                }
            }
            Action::LiteralName { table, encoding } => {
                if value > usize::MAX as u64 {
                    return Err(Error::NameRange { value });
                }
                let name = table.begin().map_err(Error::TableCapacity)?;
                let string = strings::Cursor::new(value as usize, encoding);
                if value == 0 {
                    Self::finish_name(name, string)
                } else {
                    name.pending(string);
                    Ok(())
                }
            }
            Action::LiteralValue { table, encoding } => {
                if value > usize::MAX as u64 {
                    return Err(Error::ValueRange { value });
                }
                let string = strings::Cursor::new(value as usize, encoding);
                if value == 0 {
                    Self::finish_value(table, string)
                } else {
                    table.pending(Value::Literal(string));
                    Ok(())
                }
            }
        }
    }

    fn try_notify(feedback: &mut state::State, limit: usize) -> Result<Notify<'_>, Error> {
        match feedback.try_prepare(
            stream::Instruction::InsertCountIncrement {
                increment: NonZero::<u64>::MIN,
            },
            limit,
        ) {
            Ok(update) => Ok(Notify::Ready(update)),
            Err(state::Error::Capacity(capacity)) => Ok(Notify::Blocked(capacity)),
            Err(state::Error::Increment {
                pending,
                additional: added,
            }) => Err(Error::FeedbackIncrement {
                pending,
                additional: added,
            }),
        }
    }

    fn advance(position: usize, added: usize, limit: usize) -> Result<usize, Error> {
        position
            .checked_add(added)
            .filter(|next| *next <= limit)
            .ok_or(Error::Position {
                position,
                additional: added,
                available: limit,
            })
    }
}

const _: () = assert!(core::mem::size_of::<qpack::Progress>() <= 2 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Error>() <= 4 * core::mem::size_of::<usize>());

const _: [(); size_of::<[usize; 8]>()] = [(); size_of::<State>()];
