use std::num::NonZero;

use sark_http::{
    field::metadata::block,
    payload::compress::{
        dynamic,
        dynamic::insert::{self, stages},
    },
};
use stages::Write as _;

use crate::hpack::{
    decode::{
        self,
        decoder::{Core, lookup},
        limit, mode,
        phase::State,
        strings,
    },
    statics,
};

type Named<D, M, S> = Result<(D, limit::Named<M>, S), (S, <M as mode::Flow<S>>::Error)>;

pub(in crate::hpack::decode) enum Path {
    Ready(stages::Ready),
    Staged(stages::Value),
}

impl Path {
    pub(in crate::hpack::decode) fn name<'a>(
        &'a self,
        core: &'a Core,
        retain: strings::Retention,
    ) -> Result<Option<block::Named<'a>>, decode::Error> {
        let name = match (self, retain) {
            (Self::Ready(_), strings::Retention::Counted) => return Ok(None),
            (Self::Ready(_), strings::Retention::Retained) => core.name_scratch.named(),
            (Self::Staged(table), _) => table.named().map_err(decode::Error::from)?,
        };
        Ok(match retain {
            strings::Retention::Retained => Some(name),
            strings::Retention::Counted => None,
        })
    }
}

fn copy_name(table: stages::Name, index: NonZero<usize>) -> Result<Path, decode::Error> {
    let index = index.get();
    let fixed = statics::TABLE.len();
    let copied = if index <= fixed {
        let Some(field) = statics::TABLE.get(index) else {
            return Err(decode::Error::BadIndex);
        };
        table.set_name(field.name())
    } else {
        table.copy(index - fixed - 1)
    };
    match copied {
        Ok(table) => Ok(Path::Staged(table)),
        Err((table, insert::Error::Capacity { .. } | insert::Error::Length { .. })) => {
            Ok(Path::Ready(table.discard()))
        }
        Err((_, error)) => Err(match error {
            insert::Error::Index { .. } => decode::Error::BadIndex,
            error => decode::Error::from(error),
        }),
    }
}

pub(in crate::hpack) fn start<M, S>(
    core: &mut Core,
    table: stages::Ready,
    budget: &limit::Budget,
    index: usize,
    kind: mode::Index,
    sink: S,
) -> Result<(State<M>, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let context = context(&table, budget, kind);
    let Some(index) = NonZero::new(index) else {
        return Ok((
            match kind {
                mode::Index::Plain | mode::Index::Never => State::ReadyNamePrefix(table, context),
                mode::Index::Indexed => match table.begin() {
                    stages::Begin::Name(decoder) => State::StagedNamePrefix(decoder, context),
                    stages::Begin::Omitted(decoder) => State::ReadyNamePrefix(decoder, context),
                },
            },
            sink,
        ));
    };
    match kind {
        mode::Index::Plain | mode::Index::Never => {
            let ((len, retain), sink) = mode::carry(
                sink,
                retain_name(core, &table, index.get(), context.retention()),
            )?;
            let (decoder, context, sink) = name(core, table, context, len, retain, sink)?;
            Ok((State::ReadyValuePrefix(decoder, context), sink))
        }
        mode::Index::Indexed => {
            let ((path, decoded), sink) =
                mode::carry(sink, indexed(core, table, index, context.retention()))?;
            let (name, sink) = mode::carry(sink, path.name(core, decoded.retention))?;
            let (context, sink) = context.begin(sink, name, decoded.len)?;
            Ok((
                match path {
                    Path::Ready(decoder) => State::ReadyValuePrefix(decoder, context),
                    Path::Staged(decoder) => State::StagedValuePrefix(decoder, context),
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
    core: &mut Core,
    table: stages::Ready,
    index: NonZero<usize>,
    limit: limit::Bound,
) -> Result<(Path, strings::Decoded), decode::Error> {
    let path = match table.begin() {
        stages::Begin::Name(decoder) => copy_name(decoder, index)?,
        stages::Begin::Omitted(decoder) => Path::Ready(decoder),
    };
    let table = match path {
        Path::Staged(decoder) => {
            let len = decoder.written();
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
    let (len, retain) = retain_name(core, &table, index.get(), limit)?;
    Ok((
        Path::Ready(table),
        strings::Decoded {
            len,
            retention: retain,
        },
    ))
}

pub(in crate::hpack) fn context<M: mode::Kind>(
    table: &stages::Ready,
    budget: &limit::Budget,
    index: mode::Index,
) -> limit::Literal<M> {
    let limit = budget.retention();
    let limit = match index {
        mode::Index::Plain | mode::Index::Never => limit,
        mode::Index::Indexed => match table.max_size().checked_sub(dynamic::ENTRY_OVERHEAD) {
            Some(capacity) => limit.max(capacity),
            None => limit,
        },
    };
    limit::Literal::new(index, limit)
}

pub(in crate::hpack) fn retain_name(
    core: &mut Core,
    table: &stages::Ready,
    index: usize,
    limit: limit::Bound,
) -> Result<(usize, strings::Retention), decode::Error> {
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
    core: &Core,
    table: stages::Ready,
    context: limit::Literal<M>,
    len: usize,
    retain: strings::Retention,
    sink: S,
) -> Named<stages::Ready, M, S>
where
    M: mode::Flow<S>,
{
    let span = match retain {
        strings::Retention::Retained => Some(core.name_scratch.named()),
        strings::Retention::Counted => None,
    };
    let (context, sink) = context.begin(sink, span, len)?;
    Ok((table, context, sink))
}

/// Indexed literals reach scratch only after native insertion cannot fit.
pub(in crate::hpack) fn value<M, S>(
    core: &Core,
    mut table: stages::Ready,
    budget: &mut limit::Budget,
    context: limit::Named<M>,
    len: usize,
    retain: strings::Retention,
    sink: S,
) -> Result<(stages::Ready, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let retain = match (context.retention(), retain) {
        (mode::NameRoute::Retain, strings::Retention::Retained) => strings::Retention::Retained,
        (mode::NameRoute::Retain, strings::Retention::Counted) | (mode::NameRoute::Count, _) => {
            strings::Retention::Counted
        }
    };
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
    Ok((table, sink))
}

pub(in crate::hpack) fn staged_name<M, S>(
    table: stages::Name,
    context: limit::Literal<M>,
    sink: S,
) -> Named<stages::Value, M, S>
where
    M: mode::Flow<S>,
{
    let (decoder, sink) = mode::carry(
        sink,
        table
            .finish()
            .map_err(|(_, cause)| decode::Error::from(cause)),
    )?;
    let (name, sink) = mode::carry(sink, decoder.named().map_err(decode::Error::from))?;
    let (context, sink) = context.begin(sink, Some(name), name.span().len())?;
    Ok((decoder, context, sink))
}

/// Completes admitted native spans before committing their table entry.
pub(in crate::hpack) fn staged_value<M, S>(
    table: stages::Value,
    budget: &mut limit::Budget,
    context: limit::Named<M>,
    sink: S,
) -> Result<(stages::Ready, S), (S, M::Error)>
where
    M: mode::Flow<S>,
{
    let (name, sink) = mode::carry(sink, table.named().map_err(decode::Error::from))?;
    let (value, sink) = mode::carry(sink, table.valued().map_err(decode::Error::from))?;
    let sink = M::finish(
        context.mode,
        sink,
        budget,
        mode::Field {
            name,
            value: mode::Value::Retained(value),
        },
    )?;
    Ok((table.commit(), sink))
}

const _: () = assert!(core::mem::size_of::<usize>() >= core::mem::size_of::<u64>());
