use std::num::NonZero;

use sark_http::{
    field::metadata::block,
    payload::compress::{dynamic, dynamic::insert::stages},
};
use stages::Write as _;

use crate::hpack::decode::{
    self,
    decoder::{Insert, Machine, lookup},
    limit, mode,
    phase::Phase,
    strings,
};

type Named<D, M, S> = Result<(D, limit::Named<M>, S), (S, <M as mode::Flow<S>>::Error)>;

pub(in crate::hpack) struct Literal<T> {
    pub(in crate::hpack::decode) decoder: T,
}

pub(in crate::hpack::decode) enum Path {
    Ready(decode::Decoder),
    Staged(Machine<stages::Value>),
}

impl Path {
    pub(in crate::hpack::decode) fn name(
        &self,
        retain: strings::Retention,
    ) -> Result<Option<block::Named<'_>>, decode::Error> {
        let name = match (self, retain) {
            (Self::Ready(_), strings::Retention::Counted) => return Ok(None),
            (Self::Ready(decoder), strings::Retention::Retained) => {
                decoder.0.core.name_scratch.named()
            }
            (Self::Staged(decoder), _) => decoder.dyn_table.named().map_err(decode::Error::from)?,
        };
        Ok(match retain {
            strings::Retention::Retained => Some(name),
            strings::Retention::Counted => None,
        })
    }
}

impl<T> Literal<T> {
    pub(in crate::hpack) fn new(decoder: T) -> Self {
        Self { decoder }
    }
}

impl Literal<decode::Decoder> {
    pub(in crate::hpack) fn start<M, S>(
        mut self,
        budget: &limit::Budget,
        index: usize,
        kind: mode::Index,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let context = self.context(budget, kind);
        let Some(index) = NonZero::new(index) else {
            return Ok((
                match kind {
                    mode::Index::Plain | mode::Index::Never => {
                        Phase::ReadyNamePrefix(self.decoder, context)
                    }
                    mode::Index::Indexed => match self.decoder.0.begin() {
                        Insert::Name(decoder) => Phase::StagedNamePrefix(decoder, context),
                        Insert::Ready(decoder) => {
                            Phase::ReadyNamePrefix(decode::Decoder(decoder), context)
                        }
                    },
                },
                sink,
            ));
        };
        match kind {
            mode::Index::Plain | mode::Index::Never => {
                let ((len, retain), sink) =
                    mode::carry(sink, self.retain_name(index.get(), context.retention()))?;
                let (decoder, context, sink) = self.name(context, len, retain, sink)?;
                Ok((Phase::ReadyValuePrefix(decoder, context), sink))
            }
            mode::Index::Indexed => {
                let ((path, decoded), sink) =
                    mode::carry(sink, self.indexed(index, context.retention()))?;
                let (name, sink) = mode::carry(sink, path.name(decoded.retention))?;
                let (context, sink) = context.begin(sink, name, decoded.len)?;
                Ok((
                    match path {
                        Path::Ready(decoder) => Phase::ReadyValuePrefix(decoder, context),
                        Path::Staged(decoder) => Phase::StagedValuePrefix(decoder, context),
                    },
                    sink,
                ))
            }
        }
    }

    /// Copies only the indexed name into fresh insertion storage.
    /// A rejected plan leaves the table intact; fallback preserves its name and
    /// allocation failure before admitting the sink.
    pub(in crate::hpack::decode) fn indexed(
        self,
        index: NonZero<usize>,
        limit: limit::Bound,
    ) -> Result<(Path, strings::Decoded), decode::Error> {
        let path = match self.decoder.0.begin() {
            Insert::Name(decoder) => decoder.copy_name(index)?,
            Insert::Ready(decoder) => Path::Ready(decode::Decoder(decoder)),
        };
        let decoder = match path {
            Path::Staged(decoder) => {
                let len = decoder.dyn_table.written();
                return Ok((
                    Path::Staged(decoder),
                    strings::Decoded {
                        len,
                        retention: strings::Retention::Retained,
                    },
                ));
            }
            Path::Ready(decoder) => decoder,
        };
        let mut literal = Self::new(decoder);
        let (len, retain) = literal.retain_name(index.get(), limit)?;
        Ok((
            Path::Ready(literal.decoder),
            strings::Decoded {
                len,
                retention: retain,
            },
        ))
    }

    pub(in crate::hpack) fn context<M: mode::Kind>(
        &self,
        budget: &limit::Budget,
        index: mode::Index,
    ) -> limit::Literal<M> {
        let limit = budget.retention();
        let limit = match index {
            mode::Index::Plain | mode::Index::Never => limit,
            mode::Index::Indexed => match self
                .decoder
                .0
                .dyn_table
                .max_size()
                .checked_sub(dynamic::ENTRY_OVERHEAD)
            {
                Some(capacity) => limit.max(capacity),
                None => limit,
            },
        };
        limit::Literal::new(index, limit)
    }

    pub(in crate::hpack) fn retain_name(
        &mut self,
        index: usize,
        limit: limit::Bound,
    ) -> Result<(usize, strings::Retention), decode::Error> {
        let Machine {
            dyn_table: table,
            core,
        } = &mut self.decoder.0;
        let field = lookup::get(table, index)?;
        let scratch = &mut core.name_scratch;
        scratch.clear();
        let retain = match limit.admit(field.name().span().len()) {
            limit::Admission::Admitted => {
                scratch
                    .copy_from(field.name())
                    .map_err(decode::Error::Allocation)?;
                strings::Retention::Retained
            }
            limit::Admission::Exceeded => strings::Retention::Counted,
        };
        Ok((field.name().span().len(), retain))
    }

    pub(in crate::hpack) fn name<M, S>(
        self,
        context: limit::Literal<M>,
        len: usize,
        retain: strings::Retention,
        sink: S,
    ) -> Named<decode::Decoder, M, S>
    where
        M: mode::Flow<S>,
    {
        let span = match retain {
            strings::Retention::Retained => Some(self.decoder.0.core.name_scratch.named()),
            strings::Retention::Counted => None,
        };
        let (context, sink) = context.begin(sink, span, len)?;
        Ok((self.decoder, context, sink))
    }

    /// Indexed literals reach scratch only after native insertion cannot fit.
    pub(in crate::hpack) fn value<M, S>(
        mut self,
        budget: &mut limit::Budget,
        context: limit::Named<M>,
        len: usize,
        retain: strings::Retention,
        sink: S,
    ) -> Result<(decode::Decoder, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let retain = match (context.retention(), retain) {
            (mode::NameRoute::Retain, strings::Retention::Retained) => strings::Retention::Retained,
            (mode::NameRoute::Retain, strings::Retention::Counted)
            | (mode::NameRoute::Count, _) => strings::Retention::Counted,
        };
        let Machine {
            dyn_table: table,
            core,
        } = &mut self.decoder.0;
        match context.index() {
            mode::Index::Plain | mode::Index::Never => {}
            mode::Index::Indexed => {
                table.edit().reject();
            }
        }
        let name = core.name_scratch.named();
        let value = core.value_scratch.valued();
        let value = match retain {
            strings::Retention::Retained => Some(value),
            strings::Retention::Counted => None,
        };
        let sink = M::finish(
            context.mode,
            sink,
            budget,
            mode::Field {
                name,
                value: mode::Value::new(value, len),
            },
        )?;
        Ok((self.decoder, sink))
    }
}

impl Literal<Machine<stages::Name>> {
    pub(in crate::hpack) fn name<M, S>(
        self,
        context: limit::Literal<M>,
        sink: S,
    ) -> Named<Machine<stages::Value>, M, S>
    where
        M: mode::Flow<S>,
    {
        let (decoder, sink) = mode::carry(sink, self.decoder.finish())?;
        let (name, sink) =
            mode::carry(sink, decoder.dyn_table.named().map_err(decode::Error::from))?;
        let (context, sink) = context.begin(sink, Some(name), name.span().len())?;
        Ok((decoder, context, sink))
    }
}

impl Literal<Machine<stages::Value>> {
    /// Completes admitted native spans before committing their table entry.
    pub(in crate::hpack) fn value<M, S>(
        self,
        budget: &mut limit::Budget,
        context: limit::Named<M>,
        sink: S,
    ) -> Result<(decode::Decoder, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let (name, sink) = mode::carry(
            sink,
            self.decoder.dyn_table.named().map_err(decode::Error::from),
        )?;
        let (value, sink) = mode::carry(
            sink,
            self.decoder.dyn_table.valued().map_err(decode::Error::from),
        )?;
        let sink = M::finish(
            context.mode,
            sink,
            budget,
            mode::Field {
                name,
                value: mode::Value::Retained(value),
            },
        )?;
        Ok((decode::Decoder(self.decoder.commit()), sink))
    }
}

const _: () = assert!(core::mem::size_of::<usize>() >= core::mem::size_of::<u64>());
