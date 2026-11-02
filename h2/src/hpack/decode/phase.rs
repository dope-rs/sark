use limit::{Integer, LengthState, Literal, Named, Number, StringState};
use sark_http::{field::metadata::strings::Completion, payload::compress::dynamic::insert::stages};

use crate::hpack::decode::{
    self,
    decoder::{self, Machine, complete, literal, payload, repr},
    limit, mode, strings,
};

pub(crate) enum Phase<M: mode::Kind> {
    Representation(decode::Decoder),
    Integer(decode::Decoder, Number, Integer),
    ReadyNamePrefix(decode::Decoder, Literal<M>),
    StagedNamePrefix(decoder::Machine<stages::Name>, Literal<M>),
    ReadyValuePrefix(decode::Decoder, Named<M>),
    StagedValuePrefix(decoder::Machine<stages::Value>, Named<M>),
    ReadyNameLength(decode::Decoder, Literal<M>, LengthState),
    StagedNameLength(decoder::Machine<stages::Name>, Literal<M>, LengthState),
    ReadyValueLength(decode::Decoder, Named<M>, LengthState),
    StagedValueLength(decoder::Machine<stages::Value>, Named<M>, LengthState),
    ReadyNameString(decode::Decoder, Literal<M>, StringState),
    StagedNameString(decoder::Machine<stages::Name>, Literal<M>, StringState),
    ReadyValueString(decode::Decoder, Named<M>, StringState),
    StagedValueString(decoder::Machine<stages::Value>, Named<M>, StringState),
}

impl<M: mode::Kind> Phase<M> {
    pub(crate) fn decode<S, E>(
        self,
        budget: &mut limit::Budget,
        mut input: &[u8],
        mut sink: S,
    ) -> Result<(Self, S), (S, E)>
    where
        M: mode::Flow<S, Error = E>,
        E: From<decode::Error>,
    {
        let mut phase = self;
        while let Some((&first, rest)) = input.split_first() {
            (phase, sink) = match phase {
                Phase::Representation(decoder) => {
                    match complete::Complete::new(decoder).run::<M, _>(&mut input, budget, sink) {
                        complete::Outcome::Complete(decoder, sink) => {
                            (Phase::Representation(decoder), sink)
                        }
                        complete::Outcome::Failed(sink, cause) => return Err((sink, cause)),
                        complete::Outcome::NeedMore(first, owner, sink) => {
                            let decoder = owner.resume();
                            let (mask, target) = if first & 0x80 != 0 {
                                (0x7f, limit::Integer::Indexed)
                            } else if first & 0xC0 == 0x40 {
                                (0x3f, limit::Integer::LiteralName(mode::Index::Indexed))
                            } else if first & 0xE0 == 0x20 {
                                (0x1f, limit::Integer::DynamicSize)
                            } else {
                                let kind = if first & 0x10 != 0 {
                                    mode::Index::Never
                                } else {
                                    mode::Index::Plain
                                };
                                (0x0f, limit::Integer::LiteralName(kind))
                            };
                            repr::Repr::new(decoder)
                                .start::<M, S>(budget, first, mask, target, sink)?
                        }
                    }
                }
                Phase::Integer(decoder, mut number, target) => {
                    let (progress, sink) = mode::carry(sink, number.feed(&mut input))?;
                    match progress {
                        limit::Progress::Complete => repr::Repr::new(decoder).apply::<M, S>(
                            budget,
                            number.value(),
                            target,
                            sink,
                        )?,
                        limit::Progress::Pending => (Phase::Integer(decoder, number, target), sink),
                    }
                }
                Phase::ReadyNamePrefix(decoder, context) => {
                    input = rest;
                    repr::Repr::new(decoder).name_prefix(context, first, sink)?
                }
                Phase::StagedNamePrefix(decoder, context) => {
                    input = rest;
                    repr::Repr::new(decoder).prefix(context, first, sink)?
                }
                Phase::ReadyValuePrefix(decoder, context) => {
                    input = rest;
                    repr::Repr::new(decoder).value_prefix(budget, context, first, sink)?
                }
                Phase::StagedValuePrefix(decoder, context) => {
                    input = rest;
                    repr::Repr::new(decoder).prefix(budget, context, first, sink)?
                }
                Phase::ReadyNameLength(decoder, context, mut length) => {
                    let (progress, sink) = mode::carry(sink, length.number.feed(&mut input))?;
                    match progress {
                        limit::Progress::Complete => repr::Repr::new(decoder).name_length(
                            context,
                            length.number.value(),
                            length.encoding,
                            sink,
                        )?,
                        limit::Progress::Pending => {
                            (Phase::ReadyNameLength(decoder, context, length), sink)
                        }
                    }
                }
                Phase::StagedNameLength(decoder, context, mut length) => {
                    let (progress, sink) = mode::carry(sink, length.number.feed(&mut input))?;
                    match progress {
                        limit::Progress::Complete => repr::Repr::new(decoder).length(
                            context,
                            length.number.value(),
                            length.encoding,
                            sink,
                        )?,
                        limit::Progress::Pending => {
                            (Phase::StagedNameLength(decoder, context, length), sink)
                        }
                    }
                }
                Phase::ReadyValueLength(decoder, context, mut length) => {
                    let (progress, sink) = mode::carry(sink, length.number.feed(&mut input))?;
                    match progress {
                        limit::Progress::Complete => repr::Repr::new(decoder).value_length(
                            budget,
                            context,
                            length.number.value(),
                            length.encoding,
                            sink,
                        )?,
                        limit::Progress::Pending => {
                            (Phase::ReadyValueLength(decoder, context, length), sink)
                        }
                    }
                }
                Phase::StagedValueLength(decoder, context, mut length) => {
                    let (progress, sink) = mode::carry(sink, length.number.feed(&mut input))?;
                    match progress {
                        limit::Progress::Complete => repr::Repr::new(decoder).length(
                            budget,
                            context,
                            length.number.value(),
                            length.encoding,
                            sink,
                        )?,
                        limit::Progress::Pending => {
                            (Phase::StagedValueLength(decoder, context, length), sink)
                        }
                    }
                }
                Phase::ReadyNameString(mut decoder, context, mut string) => {
                    let amount = input.len().min(string.string.remaining());
                    let bytes = &input[..amount];
                    input = &input[amount..];
                    let ((), sink) = mode::carry(
                        sink,
                        payload::Scratch::new(&mut decoder.0).feed_name(
                            &mut string,
                            &context,
                            bytes,
                        ),
                    )?;
                    Self::ready_name(decoder, context, string, sink)?
                }
                Phase::StagedNameString(mut decoder, context, mut string) => {
                    let amount = input.len().min(string.string.remaining());
                    let bytes = &input[..amount];
                    input = &input[amount..];
                    let (outcome, sink) = mode::carry(
                        sink,
                        payload::Payload::new(&mut decoder.dyn_table).feed(&mut string, bytes),
                    )?;
                    match outcome {
                        payload::Outcome::Stored => {
                            Self::staged_name(decoder, context, string, sink)?
                        }
                        payload::Outcome::Overflow => {
                            let retain = match context.route() {
                                mode::NameRoute::Retain => {
                                    match context.retention().admit(string.string.decoded_len()) {
                                        limit::Admission::Admitted => strings::Retention::Retained,
                                        limit::Admission::Exceeded => strings::Retention::Counted,
                                    }
                                }
                                mode::NameRoute::Count => strings::Retention::Counted,
                            };
                            decoder.core.name_scratch.clear();
                            let result = match retain {
                                strings::Retention::Retained => decoder
                                    .dyn_table
                                    .prefix()
                                    .map_err(decode::Error::from)
                                    .and_then(|prefix| {
                                        decoder
                                            .core
                                            .name_scratch
                                            .copy_prefix(prefix)
                                            .map_err(decode::Error::Allocation)
                                    }),
                                strings::Retention::Counted => Ok(()),
                            };
                            let ((), sink) = mode::carry(sink, result)?;
                            let mut decoder = decode::Decoder(decoder.discard());
                            string.retention = retain;
                            let ((), sink) = mode::carry(
                                sink,
                                payload::Scratch::new(&mut decoder.0).feed_name(
                                    &mut string,
                                    &context,
                                    bytes,
                                ),
                            )?;
                            Self::ready_name(decoder, context, string, sink)?
                        }
                    }
                }
                Phase::ReadyValueString(mut decoder, context, mut string) => {
                    let amount = input.len().min(string.string.remaining());
                    let bytes = &input[..amount];
                    input = &input[amount..];
                    let result = payload::Scratch::new(&mut decoder.0).feed_value(
                        &mut string,
                        &context,
                        bytes,
                        &mut sink,
                    );
                    let ((), sink) = mode::carry(sink, result)?;
                    Self::ready_value(decoder, context, string, budget, sink)?
                }
                Phase::StagedValueString(mut decoder, context, mut string) => {
                    let amount = input.len().min(string.string.remaining());
                    let bytes = &input[..amount];
                    input = &input[amount..];
                    let (outcome, sink) = mode::carry(
                        sink,
                        payload::Payload::new(&mut decoder.dyn_table).feed(&mut string, bytes),
                    )?;
                    match outcome {
                        payload::Outcome::Stored => {
                            Self::staged_value(decoder, context, string, budget, sink)?
                        }
                        payload::Outcome::Overflow => {
                            let retain = match context.route() {
                                mode::Route::Retain => {
                                    match context.limit().admit(string.string.decoded_len()) {
                                        limit::Admission::Admitted => strings::Retention::Retained,
                                        limit::Admission::Exceeded => strings::Retention::Counted,
                                    }
                                }
                                mode::Route::Count
                                | mode::Route::Validate
                                | mode::Route::Direct(_) => strings::Retention::Counted,
                            };
                            decoder.core.name_scratch.clear();
                            decoder.core.value_scratch.clear();
                            let result: Result<(), decode::Error> = (|| {
                                if matches!(retain, strings::Retention::Retained) {
                                    decoder
                                        .core
                                        .name_scratch
                                        .copy_from(
                                            decoder
                                                .dyn_table
                                                .named()
                                                .map_err(decode::Error::from)?,
                                        )
                                        .map_err(decode::Error::Allocation)?;
                                    decoder
                                        .core
                                        .value_scratch
                                        .copy_prefix(
                                            decoder
                                                .dyn_table
                                                .value_prefix()
                                                .map_err(decode::Error::from)?,
                                        )
                                        .map_err(decode::Error::Allocation)?;
                                }
                                Ok(())
                            })(
                            );
                            let ((), mut sink) = mode::carry(sink, result)?;
                            let mut decoder = decode::Decoder(decoder.discard());
                            string.retention = retain;
                            let result = payload::Scratch::new(&mut decoder.0).feed_value(
                                &mut string,
                                &context,
                                bytes,
                                &mut sink,
                            );
                            let ((), sink) = mode::carry(sink, result)?;
                            Self::ready_value(decoder, context, string, budget, sink)?
                        }
                    }
                }
            };
        }
        Ok((phase, sink))
    }

    pub(crate) fn feed_all<'a, S, I>(
        mut self,
        budget: &mut limit::Budget,
        parts: I,
        mut sink: S,
    ) -> Result<(Self, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
        I: IntoIterator<Item = &'a [u8]>,
    {
        for input in parts {
            (self, sink) = self.decode(budget, input, sink)?;
        }
        Ok((self, sink))
    }

    fn ready_name<S>(
        decoder: decode::Decoder,
        context: limit::Literal<M>,
        string: limit::StringState,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        match string.string.completion() {
            Completion::Pending => {
                return Ok((Phase::ReadyNameString(decoder, context, string), sink));
            }
            Completion::Complete => {}
        }
        let (len, sink) = mode::carry(sink, string.string.finish().map_err(decode::Error::from))?;
        let (decoder, context, sink) =
            literal::Literal::new(decoder).name(context, len, string.retention, sink)?;
        Ok((Phase::ReadyValuePrefix(decoder, context), sink))
    }

    fn ready_value<S>(
        decoder: decode::Decoder,
        context: limit::Named<M>,
        string: limit::StringState,
        budget: &mut limit::Budget,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        match string.string.completion() {
            Completion::Pending => {
                return Ok((Phase::ReadyValueString(decoder, context, string), sink));
            }
            Completion::Complete => {}
        }
        let (len, sink) = mode::carry(sink, string.string.finish().map_err(decode::Error::from))?;
        let mut sink = sink;
        if matches!(context.route(), mode::Route::Validate) {
            let result = M::validate(&mut sink, string.validation.finish());
            sink = mode::carry(sink, result)?.1;
        }
        let (decoder, sink) =
            literal::Literal::new(decoder).value(budget, context, len, string.retention, sink)?;
        Ok((Phase::Representation(decoder), sink))
    }

    fn staged_name<S>(
        decoder: Machine<stages::Name>,
        context: limit::Literal<M>,
        string: limit::StringState,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        match string.string.completion() {
            Completion::Pending => {
                return Ok((Phase::StagedNameString(decoder, context, string), sink));
            }
            Completion::Complete => {}
        }
        let (_, sink) = mode::carry(sink, string.string.finish().map_err(decode::Error::from))?;
        let (decoder, context, sink) = literal::Literal::new(decoder).name(context, sink)?;
        Ok((Phase::StagedValuePrefix(decoder, context), sink))
    }

    fn staged_value<S>(
        decoder: Machine<stages::Value>,
        context: limit::Named<M>,
        string: limit::StringState,
        budget: &mut limit::Budget,
        sink: S,
    ) -> Result<(Phase<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        match string.string.completion() {
            Completion::Pending => {
                return Ok((Phase::StagedValueString(decoder, context, string), sink));
            }
            Completion::Complete => {}
        }
        let (_, sink) = mode::carry(sink, string.string.finish().map_err(decode::Error::from))?;
        let (decoder, sink) = literal::Literal::new(decoder).value(budget, context, sink)?;
        Ok((Phase::Representation(decoder), sink))
    }

    pub(crate) fn finish(self) -> Result<decode::Decoder, decode::Error> {
        match self {
            Self::Representation(decoder) => decoder.finish(),
            _ => Err(decode::Error::NeedMore),
        }
    }
}
