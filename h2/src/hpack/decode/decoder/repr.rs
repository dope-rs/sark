use sark_http::{
    field::metadata::{
        block::Value,
        strings::{Cursor, Encoding},
    },
    payload::compress::dynamic::insert::stages,
};

use crate::hpack::decode::{
    self,
    decoder::{Machine, literal, lookup},
    limit, mode,
    phase::Phase,
    strings,
};

pub(in crate::hpack::decode) struct Repr<T> {
    decoder: T,
}

impl<T> Repr<T> {
    pub(in crate::hpack::decode) fn new(decoder: T) -> Self {
        Self { decoder }
    }
}

impl Repr<decode::Decoder> {
    pub(in crate::hpack::decode) fn start<M, S>(
        self,
        budget: &mut limit::Budget,
        first: u8,
        mask: u8,
        target: limit::Integer,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let sink = match target {
            limit::Integer::Indexed | limit::Integer::LiteralName(_) => {
                mode::carry(sink, self.decoder.0.core.field(budget))?.1
            }
            limit::Integer::DynamicSize => sink,
        };
        let prefix = first & mask;
        if prefix < mask {
            self.apply(budget, u64::from(prefix), target, sink)
        } else {
            Ok((
                Phase::Integer(self.decoder, limit::Number::new(u64::from(mask)), target),
                sink,
            ))
        }
    }

    pub(in crate::hpack::decode) fn apply<M, S>(
        mut self,
        budget: &mut limit::Budget,
        value: u64,
        target: limit::Integer,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        match target {
            limit::Integer::Indexed => {
                let (field, sink) =
                    mode::carry(sink, lookup::get(&self.decoder.0.dyn_table, value as usize))?;
                let sink = M::field(sink, budget, field.name(), field.valued())?;
                Ok((Phase::Representation(self.decoder), sink))
            }
            limit::Integer::LiteralName(index) => {
                literal::Literal::new(self.decoder).start(budget, value as usize, index, sink)
            }
            limit::Integer::DynamicSize => {
                let size = value as usize;
                let ((), sink) = mode::carry(sink, self.decoder.resize(budget, size))?;
                Ok((Phase::Representation(self.decoder), sink))
            }
        }
    }

    pub(in crate::hpack::decode) fn name_prefix<M, S>(
        self,
        context: limit::Literal<M>,
        first: u8,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let value = first & 0x7f;
        let encoding = match first & 0x80 {
            0 => Encoding::Plain,
            _ => Encoding::Huffman,
        };
        if value < 0x7f {
            self.name_length(context, u64::from(value), encoding, sink)
        } else {
            Ok((
                Phase::ReadyNameLength(
                    self.decoder,
                    context,
                    limit::LengthState {
                        number: limit::Number::new(0x7f),
                        encoding,
                    },
                ),
                sink,
            ))
        }
    }

    pub(in crate::hpack::decode) fn name_length<M, S>(
        mut self,
        context: limit::Literal<M>,
        value: u64,
        encoding: Encoding,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        if value > strings::MAX_LITERAL_LEN as u64 {
            return Err((sink, decode::Error::BadString.into()));
        }
        let len = value as usize;
        let retain = match (context.route(), encoding) {
            (mode::NameRoute::Retain, Encoding::Huffman) => strings::Retention::Retained,
            (mode::NameRoute::Retain, Encoding::Plain) => match context.limit().admit(len) {
                limit::Admission::Admitted => strings::Retention::Retained,
                limit::Admission::Exceeded => strings::Retention::Counted,
            },
            (mode::NameRoute::Count, _) => strings::Retention::Counted,
        };
        self.decoder.0.core.name_scratch.clear();
        let string = limit::StringState {
            string: Cursor::new(len, encoding),
            validation: Value::new(),
            retention: retain,
        };
        if len != 0 {
            return Ok((Phase::ReadyNameString(self.decoder, context, string), sink));
        }
        let (decoder, context, sink) =
            literal::Literal::new(self.decoder).name(context, 0, retain, sink)?;
        Ok((Phase::ReadyValuePrefix(decoder, context), sink))
    }

    pub(in crate::hpack::decode) fn value_prefix<M, S>(
        self,
        budget: &mut limit::Budget,
        context: limit::Named<M>,
        first: u8,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let value = first & 0x7f;
        let encoding = match first & 0x80 {
            0 => Encoding::Plain,
            _ => Encoding::Huffman,
        };
        if value < 0x7f {
            self.value_length(budget, context, u64::from(value), encoding, sink)
        } else {
            Ok((
                Phase::ReadyValueLength(
                    self.decoder,
                    context,
                    limit::LengthState {
                        number: limit::Number::new(0x7f),
                        encoding,
                    },
                ),
                sink,
            ))
        }
    }

    pub(in crate::hpack::decode) fn value_length<M, S>(
        mut self,
        budget: &mut limit::Budget,
        context: limit::Named<M>,
        value: u64,
        encoding: Encoding,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        if value > strings::MAX_LITERAL_LEN as u64 {
            return Err((sink, decode::Error::BadString.into()));
        }
        let len = value as usize;
        let retain = match (context.route(), encoding) {
            (mode::Route::Retain, Encoding::Huffman) => strings::Retention::Retained,
            (mode::Route::Retain, Encoding::Plain) => match context.limit().admit(len) {
                limit::Admission::Admitted => strings::Retention::Retained,
                limit::Admission::Exceeded => strings::Retention::Counted,
            },
            (mode::Route::Count | mode::Route::Validate | mode::Route::Direct(_), _) => {
                strings::Retention::Counted
            }
        };
        self.decoder.0.core.value_scratch.clear();
        let string = limit::StringState {
            string: Cursor::new(len, encoding),
            validation: Value::new(),
            retention: retain,
        };
        if len != 0 {
            return Ok((Phase::ReadyValueString(self.decoder, context, string), sink));
        }
        let (decoder, sink) =
            literal::Literal::new(self.decoder).value(budget, context, 0, retain, sink)?;
        Ok((Phase::Representation(decoder), sink))
    }
}

impl Repr<Machine<stages::Name>> {
    pub(in crate::hpack::decode) fn prefix<M, S>(
        self,
        context: limit::Literal<M>,
        first: u8,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let value = first & 0x7f;
        let encoding = match first & 0x80 {
            0 => Encoding::Plain,
            _ => Encoding::Huffman,
        };
        if value < 0x7f {
            self.length(context, u64::from(value), encoding, sink)
        } else {
            Ok((
                Phase::StagedNameLength(
                    self.decoder,
                    context,
                    limit::LengthState {
                        number: limit::Number::new(0x7f),
                        encoding,
                    },
                ),
                sink,
            ))
        }
    }

    pub(in crate::hpack::decode) fn length<M, S>(
        mut self,
        context: limit::Literal<M>,
        value: u64,
        encoding: Encoding,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        if value > strings::MAX_LITERAL_LEN as u64 {
            return Err((sink, decode::Error::BadString.into()));
        }
        self.decoder.name_scratch.clear();
        let len = value as usize;
        let string = limit::StringState {
            string: Cursor::new(len, encoding),
            validation: Value::new(),
            retention: strings::Retention::Retained,
        };
        if len != 0 {
            return Ok((Phase::StagedNameString(self.decoder, context, string), sink));
        }
        let (decoder, context, sink) = literal::Literal::new(self.decoder).name(context, sink)?;
        Ok((Phase::StagedValuePrefix(decoder, context), sink))
    }
}

impl Repr<Machine<stages::Value>> {
    pub(in crate::hpack::decode) fn prefix<M, S>(
        self,
        budget: &mut limit::Budget,
        context: limit::Named<M>,
        first: u8,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let value = first & 0x7f;
        let encoding = match first & 0x80 {
            0 => Encoding::Plain,
            _ => Encoding::Huffman,
        };
        if value < 0x7f {
            self.length(budget, context, u64::from(value), encoding, sink)
        } else {
            Ok((
                Phase::StagedValueLength(
                    self.decoder,
                    context,
                    limit::LengthState {
                        number: limit::Number::new(0x7f),
                        encoding,
                    },
                ),
                sink,
            ))
        }
    }

    pub(in crate::hpack::decode) fn length<M, S>(
        mut self,
        budget: &mut limit::Budget,
        context: limit::Named<M>,
        value: u64,
        encoding: Encoding,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        if value > strings::MAX_LITERAL_LEN as u64 {
            return Err((sink, decode::Error::BadString.into()));
        }
        self.decoder.value_scratch.clear();
        let len = value as usize;
        let string = limit::StringState {
            string: Cursor::new(len, encoding),
            validation: Value::new(),
            retention: strings::Retention::Retained,
        };
        if len != 0 {
            return Ok((
                Phase::StagedValueString(self.decoder, context, string),
                sink,
            ));
        }
        let (decoder, sink) = literal::Literal::new(self.decoder).value(budget, context, sink)?;
        Ok((Phase::Representation(decoder), sink))
    }
}

const _: () = assert!(core::mem::size_of::<usize>() >= core::mem::size_of::<u64>());
