use limit::{Integer, LengthState, Literal, Named, Number, StringState};
use sark_http::{field::metadata::strings::Completion, payload::compress::dynamic::insert::stages};

use crate::hpack::decode::{
    self,
    decoder::{Core, complete, literal, payload, repr},
    limit, mode, strings,
};

pub(crate) struct Phase<M: mode::Kind> {
    state: State<M>,
    core: Core,
}

impl<M: mode::Kind> Phase<M> {
    pub(crate) fn new(decoder: decode::Decoder) -> Self {
        let decode::Decoder { dyn_table, core } = decoder;
        Self {
            state: State::Representation(dyn_table),
            core,
        }
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
            (self.state, sink) = self.state.decode(&mut self.core, budget, input, sink)?;
        }
        Ok((self, sink))
    }

    pub(crate) fn finish(self) -> Result<decode::Decoder, decode::Error> {
        match self.state {
            State::Representation(dyn_table) => decode::Decoder {
                dyn_table,
                core: self.core,
            }
            .finish(),
            _ => Err(decode::Error::NeedMore),
        }
    }
}

pub(crate) enum State<M: mode::Kind> {
    Representation(stages::Ready),
    Integer(stages::Ready, Number, Integer),
    ReadyNamePrefix(stages::Ready, Literal<M>),
    StagedNamePrefix(stages::Name, Literal<M>),
    ReadyValuePrefix(stages::Ready, Named<M>),
    StagedValuePrefix(stages::Value, Named<M>),
    ReadyNameLength(stages::Ready, Literal<M>, LengthState),
    StagedNameLength(stages::Name, Literal<M>, LengthState),
    ReadyValueLength(stages::Ready, Named<M>, LengthState),
    StagedValueLength(stages::Value, Named<M>, LengthState),
    ReadyNameString(stages::Ready, Literal<M>, StringState),
    StagedNameString(stages::Name, Literal<M>, StringState),
    ReadyValueString(stages::Ready, Named<M>, StringState),
    StagedValueString(stages::Value, Named<M>, StringState),
}

impl<M: mode::Kind> State<M> {
    pub(crate) fn decode<S, E>(
        self,
        core: &mut Core,
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
                State::Representation(decoder) => {
                    match complete::run::<M, _>(core, decoder, &mut input, budget, sink) {
                        complete::Outcome::Complete(decoder, sink) => {
                            (State::Representation(decoder), sink)
                        }
                        complete::Outcome::Failed(sink, cause) => return Err((sink, cause)),
                        complete::Outcome::NeedMore(first, decoder, sink) => {
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
                            repr::start::<M, S>(core, decoder, budget, first, mask, target, sink)?
                        }
                    }
                }
                State::Integer(decoder, mut number, target) => {
                    let (progress, sink) = mode::carry(sink, number.feed(&mut input))?;
                    match progress {
                        limit::Progress::Complete => repr::apply::<M, S>(
                            core,
                            decoder,
                            budget,
                            number.value(),
                            target,
                            sink,
                        )?,
                        limit::Progress::Pending => (State::Integer(decoder, number, target), sink),
                    }
                }
                State::ReadyNamePrefix(decoder, context) => {
                    input = rest;
                    repr::name_prefix(core, decoder, context, first, sink)?
                }
                State::StagedNamePrefix(decoder, context) => {
                    input = rest;
                    repr::staged_name_prefix(core, decoder, context, first, sink)?
                }
                State::ReadyValuePrefix(decoder, context) => {
                    input = rest;
                    repr::value_prefix(core, decoder, budget, context, first, sink)?
                }
                State::StagedValuePrefix(decoder, context) => {
                    input = rest;
                    repr::staged_value_prefix(core, decoder, budget, context, first, sink)?
                }
                State::ReadyNameLength(decoder, context, mut length) => {
                    let (progress, sink) = mode::carry(sink, length.number.feed(&mut input))?;
                    match progress {
                        limit::Progress::Complete => repr::name_length(
                            core,
                            decoder,
                            context,
                            length.number.value(),
                            length.encoding,
                            sink,
                        )?,
                        limit::Progress::Pending => {
                            (State::ReadyNameLength(decoder, context, length), sink)
                        }
                    }
                }
                State::StagedNameLength(decoder, context, mut length) => {
                    let (progress, sink) = mode::carry(sink, length.number.feed(&mut input))?;
                    match progress {
                        limit::Progress::Complete => repr::staged_name_length(
                            core,
                            decoder,
                            context,
                            length.number.value(),
                            length.encoding,
                            sink,
                        )?,
                        limit::Progress::Pending => {
                            (State::StagedNameLength(decoder, context, length), sink)
                        }
                    }
                }
                State::ReadyValueLength(decoder, context, mut length) => {
                    let (progress, sink) = mode::carry(sink, length.number.feed(&mut input))?;
                    match progress {
                        limit::Progress::Complete => repr::value_length(
                            core,
                            decoder,
                            budget,
                            context,
                            length.number.value(),
                            length.encoding,
                            sink,
                        )?,
                        limit::Progress::Pending => {
                            (State::ReadyValueLength(decoder, context, length), sink)
                        }
                    }
                }
                State::StagedValueLength(decoder, context, mut length) => {
                    let (progress, sink) = mode::carry(sink, length.number.feed(&mut input))?;
                    match progress {
                        limit::Progress::Complete => repr::staged_value_length(
                            core,
                            decoder,
                            budget,
                            context,
                            length.number.value(),
                            length.encoding,
                            sink,
                        )?,
                        limit::Progress::Pending => {
                            (State::StagedValueLength(decoder, context, length), sink)
                        }
                    }
                }
                State::ReadyNameString(decoder, context, mut string) => {
                    let amount = input.len().min(string.string.remaining());
                    let bytes = &input[..amount];
                    input = &input[amount..];
                    let ((), sink) =
                        mode::carry(sink, core.feed_name(&mut string, &context, bytes))?;
                    Self::ready_name(core, decoder, context, string, sink)?
                }
                State::StagedNameString(mut decoder, context, mut string) => {
                    let amount = input.len().min(string.string.remaining());
                    let bytes = &input[..amount];
                    input = &input[amount..];
                    let (outcome, sink) =
                        mode::carry(sink, payload::feed(&mut decoder, &mut string, bytes))?;
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
                            core.name_scratch.clear();
                            let result = match retain {
                                strings::Retention::Retained => decoder
                                    .prefix()
                                    .map_err(decode::Error::from)
                                    .and_then(|prefix| {
                                        core.name_scratch
                                            .copy_prefix(prefix)
                                            .map_err(decode::Error::Allocation)
                                    }),
                                strings::Retention::Counted => Ok(()),
                            };
                            let ((), sink) = mode::carry(sink, result)?;
                            let decoder = decoder.discard();
                            string.retention = retain;
                            let ((), sink) =
                                mode::carry(sink, core.feed_name(&mut string, &context, bytes))?;
                            Self::ready_name(core, decoder, context, string, sink)?
                        }
                    }
                }
                State::ReadyValueString(decoder, context, mut string) => {
                    let amount = input.len().min(string.string.remaining());
                    let bytes = &input[..amount];
                    input = &input[amount..];
                    let result = core.feed_value(&mut string, &context, bytes, &mut sink);
                    let ((), sink) = mode::carry(sink, result)?;
                    Self::ready_value(core, decoder, context, string, budget, sink)?
                }
                State::StagedValueString(mut decoder, context, mut string) => {
                    let amount = input.len().min(string.string.remaining());
                    let bytes = &input[..amount];
                    input = &input[amount..];
                    let (outcome, sink) =
                        mode::carry(sink, payload::feed(&mut decoder, &mut string, bytes))?;
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
                            core.name_scratch.clear();
                            core.value_scratch.clear();
                            let result: Result<(), decode::Error> = (|| {
                                if matches!(retain, strings::Retention::Retained) {
                                    core.name_scratch
                                        .copy_from(decoder.named().map_err(decode::Error::from)?)
                                        .map_err(decode::Error::Allocation)?;
                                    core.value_scratch
                                        .copy_prefix(
                                            decoder.value_prefix().map_err(decode::Error::from)?,
                                        )
                                        .map_err(decode::Error::Allocation)?;
                                }
                                Ok(())
                            })(
                            );
                            let ((), mut sink) = mode::carry(sink, result)?;
                            let decoder = decoder.discard();
                            string.retention = retain;
                            let result = core.feed_value(&mut string, &context, bytes, &mut sink);
                            let ((), sink) = mode::carry(sink, result)?;
                            Self::ready_value(core, decoder, context, string, budget, sink)?
                        }
                    }
                }
            };
        }
        Ok((phase, sink))
    }

    fn ready_name<S>(
        core: &mut Core,
        decoder: stages::Ready,
        context: limit::Literal<M>,
        string: limit::StringState,
        sink: S,
    ) -> Result<(State<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        match string.string.completion() {
            Completion::Pending => {
                return Ok((State::ReadyNameString(decoder, context, string), sink));
            }
            Completion::Complete => {}
        }
        let (len, sink) = mode::carry(sink, string.string.finish().map_err(decode::Error::from))?;
        let (decoder, context, sink) =
            literal::name(core, decoder, context, len, string.retention, sink)?;
        Ok((State::ReadyValuePrefix(decoder, context), sink))
    }

    fn ready_value<S>(
        core: &mut Core,
        decoder: stages::Ready,
        context: limit::Named<M>,
        string: limit::StringState,
        budget: &mut limit::Budget,
        sink: S,
    ) -> Result<(State<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        match string.string.completion() {
            Completion::Pending => {
                return Ok((State::ReadyValueString(decoder, context, string), sink));
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
            literal::value(core, decoder, budget, context, len, string.retention, sink)?;
        Ok((State::Representation(decoder), sink))
    }

    fn staged_name<S>(
        decoder: stages::Name,
        context: limit::Literal<M>,
        string: limit::StringState,
        sink: S,
    ) -> Result<(State<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        match string.string.completion() {
            Completion::Pending => {
                return Ok((State::StagedNameString(decoder, context, string), sink));
            }
            Completion::Complete => {}
        }
        let (_, sink) = mode::carry(sink, string.string.finish().map_err(decode::Error::from))?;
        let (decoder, context, sink) = literal::staged_name(decoder, context, sink)?;
        Ok((State::StagedValuePrefix(decoder, context), sink))
    }

    fn staged_value<S>(
        decoder: stages::Value,
        context: limit::Named<M>,
        string: limit::StringState,
        budget: &mut limit::Budget,
        sink: S,
    ) -> Result<(State<M>, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        match string.string.completion() {
            Completion::Pending => {
                return Ok((State::StagedValueString(decoder, context, string), sink));
            }
            Completion::Complete => {}
        }
        let (_, sink) = mode::carry(sink, string.string.finish().map_err(decode::Error::from))?;
        let (decoder, sink) = literal::staged_value(decoder, budget, context, sink)?;
        Ok((State::Representation(decoder), sink))
    }
}
