use mode::Direct as _;
use sark_http::{
    field::metadata::block::{self, buffer},
    payload::compress::dynamic::insert::stages,
};

use crate::hpack::decode::{
    self,
    decoder::{
        Core, kind,
        literal::{self, Path},
        lookup, payload,
    },
    limit, mode, strings,
};

pub(in crate::hpack::decode) enum Outcome<S, E> {
    Complete(stages::Ready, S),
    NeedMore(u8, stages::Ready, S),
    Failed(S, E),
}

pub(in crate::hpack::decode) fn run<M, S>(
    core: &mut Core,
    mut table: stages::Ready,
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
                return Outcome::NeedMore(*first, table, sink);
            }
            Err(cause) => return Outcome::Failed(sink, cause.into()),
        };
        let result = match parsed {
            kind::Kind::Indexed(index) => indexed::<M, S>(core, &table, index, budget, sink),
            kind::Kind::Literal { index, name, value } => {
                match read_literal::<M, S>(core, table, index, name, value, budget, sink) {
                    Ok((decoder, sink)) => {
                        table = decoder;
                        Ok(sink)
                    }
                    Err((sink, cause)) => return Outcome::Failed(sink, cause),
                }
            }
            kind::Kind::Resize(size) => {
                mode::carry(sink, core.resize(&mut table, budget, size)).map(|((), sink)| sink)
            }
        };
        sink = match result {
            Ok(sink) => sink,
            Err((sink, cause)) => return Outcome::Failed(sink, cause),
        };
        *input = tail;
    }
    Outcome::Complete(table, sink)
}

fn indexed<M, S>(
    core: &Core,
    table: &stages::Ready,
    index: usize,
    budget: &mut limit::Budget,
    sink: S,
) -> Result<S, (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let ((), sink) = mode::carry(sink, core.field(budget))?;
    let (field, sink) = mode::carry(sink, lookup::get(table, index))?;
    M::field(sink, budget, field.name(), field.valued())
}

fn read_literal<M, S>(
    core: &mut Core,
    table: stages::Ready,
    index: mode::Index,
    name: kind::Name<'_>,
    value: strings::Encoded<'_>,
    budget: &mut limit::Budget,
    sink: S,
) -> Result<(stages::Ready, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let ((), sink) = mode::carry(sink, core.field(budget))?;
    match index {
        mode::Index::Plain | mode::Index::Never => {
            read_plain::<M, S>(core, table, index, name, value, budget, sink)
        }
        mode::Index::Indexed => read_indexed::<M, S>(core, table, name, value, budget, sink),
    }
}

fn read_plain<M, S>(
    core: &mut Core,
    table: stages::Ready,
    index: mode::Index,
    name: kind::Name<'_>,
    value: strings::Encoded<'_>,
    budget: &mut limit::Budget,
    sink: S,
) -> Result<(stages::Ready, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let context: limit::Literal<M> = literal::context(&table, budget, index);
    let result = match name {
        kind::Name::Literal(encoded) => {
            encoded.decode_named(&mut core.name_scratch, context.retention().option())
        }
        kind::Name::Indexed(index) => lookup::get(&table, index.get()).map(|field| {
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
    Ok((table, sink))
}

fn read_indexed<M, S>(
    core: &mut Core,
    table: stages::Ready,
    name: kind::Name<'_>,
    value: strings::Encoded<'_>,
    budget: &mut limit::Budget,
    sink: S,
) -> Result<(stages::Ready, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let context: limit::Literal<M> = literal::context(&table, budget, mode::Index::Indexed);
    let (path, context, sink) = match name {
        kind::Name::Literal(encoded) => {
            let (insert, sink) = match table.begin() {
                stages::Begin::Name(mut decoder) => {
                    let (outcome, sink) = mode::carry(sink, payload::store(&mut decoder, encoded))?;
                    (
                        match outcome {
                            payload::Outcome::Stored => stages::Begin::Name(decoder),
                            payload::Outcome::Overflow => stages::Begin::Omitted(decoder.discard()),
                        },
                        sink,
                    )
                }
                stages::Begin::Omitted(decoder) => (stages::Begin::Omitted(decoder), sink),
            };
            match insert {
                stages::Begin::Name(decoder) => {
                    let (decoder, next, sink) = literal::staged_name(decoder, context, sink)?;
                    (Path::Staged(decoder), next, sink)
                }
                stages::Begin::Omitted(decoder) => {
                    let (decoded, sink) = mode::carry(
                        sink,
                        encoded.decode_into(&mut core.name_scratch, context.retention().option()),
                    )?;
                    let (decoder, next, sink) = literal::name(
                        core,
                        decoder,
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
            let ((path, decoded), sink) = mode::carry(
                sink,
                literal::indexed(core, table, index, context.retention()),
            )?;
            let (name, sink) = mode::carry(sink, path.name(core, decoded.retention))?;
            let (context, sink) = context.begin(sink, name, decoded.len)?;
            (path, context, sink)
        }
    };
    let (decoder, sink) = match path {
        Path::Ready(decoder) => (decoder, sink),
        Path::Staged(mut decoder) => {
            let (outcome, sink) = mode::carry(sink, payload::store(&mut decoder, value))?;
            match outcome {
                payload::Outcome::Stored => {
                    return literal::staged_value(decoder, budget, context, sink);
                }
                payload::Outcome::Overflow => {
                    core.name_scratch.clear();
                    let sink = if matches!(context.route(), mode::Route::Retain) {
                        let (name, sink) =
                            mode::carry(sink, decoder.named().map_err(decode::Error::from))?;
                        mode::carry(
                            sink,
                            core.name_scratch
                                .copy_from(name)
                                .map_err(decode::Error::Allocation),
                        )?
                        .1
                    } else {
                        sink
                    };
                    (decoder.discard(), sink)
                }
            }
        }
    };
    let (decoded, sink) = decode_value(&context, sink, value, &mut core.value_scratch)?;
    literal::value(
        core,
        decoder,
        budget,
        context,
        decoded.len,
        decoded.retention,
        sink,
    )
}

fn decode_value<M, S>(
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
