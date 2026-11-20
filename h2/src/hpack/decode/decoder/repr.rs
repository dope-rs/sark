use sark_http::{
    field::metadata::strings::Encoding,
    payload::compress::dynamic::insert::stages,
};

use crate::hpack::decode::{
    decoder::{Core, literal, lookup},
    limit, mode,
    phase::State,
    strings,
};

pub(in crate::hpack::decode) fn start<M, S>(
    core: &mut Core,
    table: stages::Ready,
    budget: &mut limit::Budget,
    first: u8,
    mask: u8,
    target: limit::Integer,
    sink: S,
) -> Result<(State<M>, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let sink = match target {
        limit::Integer::Indexed | limit::Integer::LiteralName(_) => {
            mode::carry(sink, core.field(budget))?.1
        }
        limit::Integer::DynamicSize => sink,
    };
    let prefix = first & mask;
    if prefix < mask {
        apply(core, table, budget, u64::from(prefix), target, sink)
    } else {
        Ok((
            State::Integer(table, limit::Number::new(u64::from(mask)), target),
            sink,
        ))
    }
}

pub(in crate::hpack::decode) fn apply<M, S>(
    core: &mut Core,
    mut table: stages::Ready,
    budget: &mut limit::Budget,
    value: u64,
    target: limit::Integer,
    sink: S,
) -> Result<(State<M>, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    match target {
        limit::Integer::Indexed => {
            let (field, sink) = mode::carry(sink, lookup::get(&table, value as usize))?;
            let sink = M::field(sink, budget, field.name(), field.valued())?;
            Ok((State::Representation(table), sink))
        }
        limit::Integer::LiteralName(index) => {
            literal::start(core, table, budget, value as usize, index, sink)
        }
        limit::Integer::DynamicSize => {
            let size = value as usize;
            let ((), sink) = mode::carry(sink, core.resize(&mut table, budget, size))?;
            Ok((State::Representation(table), sink))
        }
    }
}

pub(in crate::hpack::decode) fn name_prefix<M, S>(
    core: &mut Core,
    table: stages::Ready,
    context: limit::Literal<M>,
    first: u8,
    sink: S,
) -> Result<(State<M>, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let (value, encoding) = strings::prefix(first);
    if value < 0x7f {
        name_length(core, table, context, u64::from(value), encoding, sink)
    } else {
        Ok((
            State::ReadyNameLength(
                table,
                context,
                limit::LengthState::new(encoding),
            ),
            sink,
        ))
    }
}

pub(in crate::hpack::decode) fn name_length<M, S>(
    core: &mut Core,
    table: stages::Ready,
    context: limit::Literal<M>,
    value: u64,
    encoding: Encoding,
    sink: S,
) -> Result<(State<M>, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let (len, sink) = mode::carry(sink, strings::length(value))?;
    let retain = match (context.route(), encoding) {
        (mode::NameRoute::Retain, Encoding::Huffman) => strings::Retention::Retained,
        (mode::NameRoute::Retain, Encoding::Plain) => match context.limit().admit(len) {
            limit::Admission::Admitted => strings::Retention::Retained,
            limit::Admission::Exceeded => strings::Retention::Counted,
        },
        (mode::NameRoute::Count, _) => strings::Retention::Counted,
    };
    core.name_scratch.clear();
    let string = limit::StringState::new(len, encoding, retain);
    if len != 0 {
        return Ok((State::ReadyNameString(table, context, string), sink));
    }
    let (decoder, context, sink) = literal::name(core, table, context, 0, retain, sink)?;
    Ok((State::ReadyValuePrefix(decoder, context), sink))
}

pub(in crate::hpack::decode) fn value_prefix<M, S>(
    core: &mut Core,
    table: stages::Ready,
    budget: &mut limit::Budget,
    context: limit::Named<M>,
    first: u8,
    sink: S,
) -> Result<(State<M>, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let (value, encoding) = strings::prefix(first);
    if value < 0x7f {
        value_length(
            core,
            table,
            budget,
            context,
            u64::from(value),
            encoding,
            sink,
        )
    } else {
        Ok((
            State::ReadyValueLength(
                table,
                context,
                limit::LengthState::new(encoding),
            ),
            sink,
        ))
    }
}

pub(in crate::hpack::decode) fn value_length<M, S>(
    core: &mut Core,
    table: stages::Ready,
    budget: &mut limit::Budget,
    context: limit::Named<M>,
    value: u64,
    encoding: Encoding,
    sink: S,
) -> Result<(State<M>, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let (len, sink) = mode::carry(sink, strings::length(value))?;
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
    core.value_scratch.clear();
    let string = limit::StringState::new(len, encoding, retain);
    if len != 0 {
        return Ok((State::ReadyValueString(table, context, string), sink));
    }
    let (decoder, sink) = literal::value(core, table, budget, context, 0, retain, sink)?;
    Ok((State::Representation(decoder), sink))
}

pub(in crate::hpack::decode) fn staged_name_prefix<M, S>(
    core: &mut Core,
    table: stages::Name,
    context: limit::Literal<M>,
    first: u8,
    sink: S,
) -> Result<(State<M>, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let (value, encoding) = strings::prefix(first);
    if value < 0x7f {
        staged_name_length(core, table, context, u64::from(value), encoding, sink)
    } else {
        Ok((
            State::StagedNameLength(
                table,
                context,
                limit::LengthState::new(encoding),
            ),
            sink,
        ))
    }
}

pub(in crate::hpack::decode) fn staged_name_length<M, S>(
    core: &mut Core,
    table: stages::Name,
    context: limit::Literal<M>,
    value: u64,
    encoding: Encoding,
    sink: S,
) -> Result<(State<M>, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let (len, sink) = mode::carry(sink, strings::length(value))?;
    core.name_scratch.clear();
    let string = limit::StringState::new(len, encoding, strings::Retention::Retained);
    if len != 0 {
        return Ok((State::StagedNameString(table, context, string), sink));
    }
    let (decoder, context, sink) = literal::staged_name(table, context, sink)?;
    Ok((State::StagedValuePrefix(decoder, context), sink))
}

pub(in crate::hpack::decode) fn staged_value_prefix<M, S>(
    core: &mut Core,
    table: stages::Value,
    budget: &mut limit::Budget,
    context: limit::Named<M>,
    first: u8,
    sink: S,
) -> Result<(State<M>, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let (value, encoding) = strings::prefix(first);
    if value < 0x7f {
        staged_value_length(
            core,
            table,
            budget,
            context,
            u64::from(value),
            encoding,
            sink,
        )
    } else {
        Ok((
            State::StagedValueLength(
                table,
                context,
                limit::LengthState::new(encoding),
            ),
            sink,
        ))
    }
}

pub(in crate::hpack::decode) fn staged_value_length<M, S>(
    core: &mut Core,
    table: stages::Value,
    budget: &mut limit::Budget,
    context: limit::Named<M>,
    value: u64,
    encoding: Encoding,
    sink: S,
) -> Result<(State<M>, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let (len, sink) = mode::carry(sink, strings::length(value))?;
    core.value_scratch.clear();
    let string = limit::StringState::new(len, encoding, strings::Retention::Retained);
    if len != 0 {
        return Ok((State::StagedValueString(table, context, string), sink));
    }
    let (decoder, sink) = literal::staged_value(table, budget, context, sink)?;
    Ok((State::Representation(decoder), sink))
}

const _: () = assert!(core::mem::size_of::<usize>() >= core::mem::size_of::<u64>());
