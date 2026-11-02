use mode::Direct as _;
use sark_http::field::metadata::block::{self, buffer};

use crate::hpack::decode::{
    self,
    decoder::{
        Insert, Machine, kind,
        literal::{self, Path},
        lookup, payload,
    },
    limit, mode, strings,
};

pub(in crate::hpack::decode) enum Outcome<S, E> {
    Complete(decode::Decoder, S),
    NeedMore(u8, Incomplete, S),
    Failed(S, E),
}

pub(in crate::hpack::decode) struct Complete {
    decoder: decode::Decoder,
}

pub(in crate::hpack::decode) struct Incomplete(decode::Decoder);

impl Incomplete {
    pub(in crate::hpack::decode) fn resume(self) -> decode::Decoder {
        self.0
    }

    pub(in crate::hpack::decode) fn fail(self) -> decode::Error {
        decode::Error::NeedMore
    }
}

impl Complete {
    pub(in crate::hpack::decode) fn new(decoder: decode::Decoder) -> Self {
        Self { decoder }
    }

    pub(in crate::hpack::decode) fn block<M, S>(
        self,
        mut input: &[u8],
        budget: &mut limit::Budget,
        sink: S,
    ) -> Result<(decode::Decoder, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        match self.run::<M, S>(&mut input, budget, sink) {
            Outcome::Complete(decoder, sink) => mode::carry(sink, decoder.finish()),
            Outcome::NeedMore(_, owner, sink) => Err((sink, owner.fail().into())),
            Outcome::Failed(sink, cause) => Err((sink, cause)),
        }
    }

    pub(in crate::hpack::decode) fn run<M, S>(
        mut self,
        input: &mut &[u8],
        budget: &mut limit::Budget,
        mut sink: S,
    ) -> Outcome<S, M::Error>
    where
        M: mode::Flow<S>,
    {
        while let Some((first, rest)) = input.split_first() {
            let mut tail = *input;
            let parsed = match kind::Kind::parse(&mut tail) {
                Ok(parsed) => parsed,
                Err(decode::Error::NeedMore) => {
                    *input = rest;
                    return Outcome::NeedMore(*first, Incomplete(self.decoder), sink);
                }
                Err(cause) => return Outcome::Failed(sink, cause.into()),
            };
            let result = match parsed {
                kind::Kind::Indexed(index) => self.indexed::<M, S>(index, budget, sink),
                kind::Kind::Literal { index, name, value } => {
                    match self.read_literal::<M, S>(index, name, value, budget, sink) {
                        Ok((decoder, sink)) => {
                            self = Self::new(decoder);
                            Ok(sink)
                        }
                        Err((sink, cause)) => return Outcome::Failed(sink, cause),
                    }
                }
                kind::Kind::Resize(size) => {
                    mode::carry(sink, self.decoder.resize(budget, size)).map(|((), sink)| sink)
                }
            };
            sink = match result {
                Ok(sink) => sink,
                Err((sink, cause)) => return Outcome::Failed(sink, cause),
            };
            *input = tail;
        }
        Outcome::Complete(self.decoder, sink)
    }

    fn indexed<M, S>(
        &self,
        index: usize,
        budget: &mut limit::Budget,
        sink: S,
    ) -> Result<S, (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let ((), sink) = mode::carry(sink, self.decoder.0.core.field(budget))?;
        let (field, sink) = mode::carry(sink, lookup::get(&self.decoder.0.dyn_table, index))?;
        M::field(sink, budget, field.name(), field.valued())
    }

    fn read_literal<M, S>(
        self,
        index: mode::Index,
        name: kind::Name<'_>,
        value: strings::Encoded<'_>,
        budget: &mut limit::Budget,
        sink: S,
    ) -> Result<(decode::Decoder, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let ((), sink) = mode::carry(sink, self.decoder.0.core.field(budget))?;
        match index {
            mode::Index::Plain | mode::Index::Never => {
                self.read_plain::<M, S>(index, name, value, budget, sink)
            }
            mode::Index::Indexed => self.read_indexed::<M, S>(name, value, budget, sink),
        }
    }

    fn read_plain<M, S>(
        self,
        index: mode::Index,
        name: kind::Name<'_>,
        value: strings::Encoded<'_>,
        budget: &mut limit::Budget,
        sink: S,
    ) -> Result<(decode::Decoder, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let mut literal = literal::Literal::new(self.decoder);
        let context: limit::Literal<M> = literal.context(budget, index);
        let Machine {
            dyn_table: table,
            core,
        } = &mut literal.decoder.0;
        let result = match name {
            kind::Name::Literal(encoded) => {
                encoded.decode_named(&mut core.name_scratch, context.retention().option())
            }
            kind::Name::Indexed(index) => lookup::get(table, index.get()).map(|field| {
                let name = match context.retention().admit(field.name().span().len()) {
                    limit::Admission::Admitted => Some(field.name()),
                    limit::Admission::Exceeded => None,
                };
                (field.name().span().len(), name)
            }),
        };
        let ((len, name), sink) = mode::carry(sink, result)?;
        let (context, mut sink) = context.begin(sink, name, len)?;
        let result = match context.route() {
            mode::Route::Retain => value
                .decode_span(&mut core.value_scratch, context.limit().option())
                .map_err(M::Error::from),
            mode::Route::Count => value
                .decoded_len()
                .map(|decoded| (decoded.len, None))
                .map_err(M::Error::from),
            mode::Route::Validate => {
                value
                    .validate()
                    .map_err(M::Error::from)
                    .and_then(|(decoded, valid)| {
                        M::validate(&mut sink, valid)?;
                        Ok((decoded.len, None))
                    })
            }
            mode::Route::Direct(proof) => proof
                .literal(&mut sink, value)
                .map(|decoded| (decoded.len, None)),
        };
        let ((len, value), sink) = mode::carry(sink, result)?;
        let sink = M::finish(
            context.mode,
            sink,
            budget,
            mode::Field {
                name: name.unwrap_or(block::Named::from_slice(&[])),
                value: mode::Value::new(name.and(value), len),
            },
        )?;
        Ok((literal.decoder, sink))
    }

    fn read_indexed<M, S>(
        self,
        name: kind::Name<'_>,
        value: strings::Encoded<'_>,
        budget: &mut limit::Budget,
        sink: S,
    ) -> Result<(decode::Decoder, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let literal = literal::Literal::new(self.decoder);
        let context: limit::Literal<M> = literal.context(budget, mode::Index::Indexed);
        let (path, context, sink) = match name {
            kind::Name::Literal(encoded) => {
                let (insert, sink) = match literal.decoder.0.begin() {
                    Insert::Name(mut decoder) => {
                        let (outcome, sink) = mode::carry(
                            sink,
                            payload::Payload::new(&mut decoder.dyn_table).store(encoded),
                        )?;
                        (
                            match outcome {
                                payload::Outcome::Stored => Insert::Name(decoder),
                                payload::Outcome::Overflow => Insert::Ready(decoder.discard()),
                            },
                            sink,
                        )
                    }
                    Insert::Ready(decoder) => (Insert::Ready(decoder), sink),
                };
                match insert {
                    Insert::Name(decoder) => {
                        let (decoder, next, sink) =
                            literal::Literal::new(decoder).name(context, sink)?;
                        (Path::Staged(decoder), next, sink)
                    }
                    Insert::Ready(decoder) => {
                        let mut decoder = decode::Decoder(decoder);
                        let (decoded, sink) = mode::carry(
                            sink,
                            encoded.decode_into(
                                &mut decoder.0.core.name_scratch,
                                context.retention().option(),
                            ),
                        )?;
                        let (decoder, next, sink) = literal::Literal::new(decoder).name(
                            context,
                            decoded.len,
                            decoded.retention,
                            sink,
                        )?;
                        (Path::Ready(decoder), next, sink)
                    }
                }
            }
            kind::Name::Indexed(index) => {
                let ((path, decoded), sink) =
                    mode::carry(sink, literal.indexed(index, context.retention()))?;
                let (name, sink) = mode::carry(sink, path.name(decoded.retention))?;
                let (context, sink) = context.begin(sink, name, decoded.len)?;
                (path, context, sink)
            }
        };
        let (mut decoder, sink) = match path {
            Path::Ready(decoder) => (decoder, sink),
            Path::Staged(mut decoder) => {
                let (outcome, sink) = mode::carry(
                    sink,
                    payload::Payload::new(&mut decoder.dyn_table).store(value),
                )?;
                match outcome {
                    payload::Outcome::Stored => {
                        return literal::Literal::new(decoder).value(budget, context, sink);
                    }
                    payload::Outcome::Overflow => {
                        decoder.name_scratch.clear();
                        let sink = if matches!(context.route(), mode::Route::Retain) {
                            let (name, sink) = mode::carry(
                                sink,
                                decoder.dyn_table.named().map_err(decode::Error::from),
                            )?;
                            mode::carry(
                                sink,
                                decoder
                                    .core
                                    .name_scratch
                                    .copy_from(name)
                                    .map_err(decode::Error::Allocation),
                            )?
                            .1
                        } else {
                            sink
                        };
                        (decode::Decoder(decoder.discard()), sink)
                    }
                }
            }
        };
        let (decoded, sink) =
            Self::value(&context, sink, value, &mut decoder.0.core.value_scratch)?;
        literal::Literal::new(decoder).value(budget, context, decoded.len, decoded.retention, sink)
    }

    fn value<M, S>(
        context: &limit::Named<M>,
        mut sink: S,
        input: strings::Encoded<'_>,
        buf: &mut buffer::Values,
    ) -> Result<(strings::Decoded, S), (S, M::Error)>
    where
        M: mode::Flow<S>,
    {
        let result = match context.route() {
            mode::Route::Retain => input
                .decode_into(buf, context.limit().option())
                .map_err(M::Error::from),
            mode::Route::Count => input.decode_into(buf, Some(0)).map_err(M::Error::from),
            mode::Route::Validate => {
                input
                    .validate()
                    .map_err(M::Error::from)
                    .and_then(|(decoded, valid)| {
                        M::validate(&mut sink, valid)?;
                        Ok(decoded)
                    })
            }
            mode::Route::Direct(proof) => proof.literal(&mut sink, input),
        };
        mode::carry(sink, result)
    }
}

const _: () = {
    assert!(core::mem::size_of::<usize>() >= core::mem::size_of::<u64>());
    assert!(
        core::mem::size_of::<Outcome<(), decode::Error>>()
            <= core::mem::size_of::<decode::Decoder>() + core::mem::size_of::<usize>()
    );
    assert!(
        core::mem::size_of::<Outcome<(), decode::Failure<sark_http::field::metadata::lower::Error>>>(
        ) <= core::mem::size_of::<decode::Decoder>() + core::mem::size_of::<usize>()
    );
};
