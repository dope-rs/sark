use o3::buffer::write;
use sark_http::field::{
    Indexing,
    metadata::{
        block,
        block::buffer,
        lower,
        map::record,
        packed::Decoded,
        section::{
            rules,
            state::{complete, complete::Selection, decode},
            storage,
        },
    },
};

use crate::qpack::{decoder::Error, identity::Identity, literal};

fn subtract(available: usize, required: usize) -> Result<usize, Error> {
    available.checked_sub(required).ok_or(Error::FieldCapacity {
        available,
        required,
    })
}

pub(super) struct RawOutput<'a> {
    fields: &'a mut storage::Writer<Decoded>,
    remaining: usize,
}

impl<'a> RawOutput<'a> {
    pub(super) fn new(fields: &'a mut storage::Writer<Decoded>, left: usize) -> Self {
        Self {
            fields,
            remaining: left,
        }
    }

    pub(super) fn push<S: crate::Source + ?Sized>(
        &mut self,
        name: literal::Literal<'_, S>,
        value: literal::Literal<'_, S>,
        indexing: Indexing,
    ) -> Result<(), Error> {
        let budget = subtract(self.remaining, 32)?;
        self.fields.try_write_as::<Error>(indexing, |writer| {
            let name_len = name.write_to(writer, budget)?;
            let limit = subtract(budget, name_len)?;
            let data_len = value.write_to(writer, limit)?;
            self.remaining = subtract(limit, data_len)?;
            Ok(name_len)
        })
    }

    pub(super) fn push_proven(
        &mut self,
        name: record::Span<'_>,
        value: record::Span<'_>,
    ) -> Result<(), Error> {
        let limit = subtract(subtract(self.remaining, 32)?, name.len())?;
        self.remaining = subtract(limit, value.len())?;
        self.fields.try_write::<Error>(|writer| {
            let (name_a, name_b) = name.parts();
            write::Sink::write_slice(writer, name_a)?;
            write::Sink::write_slice(writer, name_b)?;
            let (value_a, value_b) = value.parts();
            write::Sink::write_slice(writer, value_a)?;
            write::Sink::write_slice(writer, value_b)?;
            Ok(name.len())
        })
    }
}

pub(super) struct FieldOutput<'a, P: rules::Policy> {
    consumer: complete::Decoder<'a, P>,
    remaining: usize,
    identity: Option<&'a mut Identity>,
}

pub(super) enum Name<'a, S: crate::Source + ?Sized> {
    Known(block::Named<'a>),
    Literal(literal::Literal<'a, S>),
}

impl<'a, P: rules::Policy> FieldOutput<'a, P> {
    pub(super) fn new(
        consumer: complete::Decoder<'a, P>,
        left: usize,
        identity: Option<&'a mut Identity>,
    ) -> Self {
        Self {
            consumer,
            remaining: left,
            identity,
        }
    }

    pub(super) fn finish(self) -> Result<decode::Final<P>, Error> {
        Ok(self.consumer.finish()?)
    }

    /// Lowers an already materialized static or dynamic table entry directly.
    /// Indexed fields never acquire `Literal` wrappers and never pass through
    /// the classification branch used by wire literal names.
    pub(super) fn push_proven<A: rules::Push>(
        &mut self,
        name: block::Named<'_>,
        value: block::Valued<'_>,
    ) -> Result<(), Error> {
        let limit = subtract(subtract(self.remaining, 32)?, name.span().len())?;
        let left = subtract(limit, value.span().len())?;
        if let Some(identity) = &mut self.identity {
            identity.try_field(name.span(), value.span())?;
        }
        match self.consumer.select(name) {
            Selection::Skipped(field) => field.validate(value.validation()),
            Selection::Buffered(field) => field
                .push::<A>(value, Indexing::Allowed)
                .map_err(Into::<lower::Error>::into)?,
        }
        self.remaining = left;
        Ok(())
    }

    pub(super) fn push<A: rules::Push, S: crate::Source + ?Sized>(
        &mut self,
        scratch: &mut Scratch<'_>,
        name: Name<'_, S>,
        value: literal::Literal<'_, S>,
        indexing: Indexing,
    ) -> Result<(), Error> {
        let budget = subtract(self.remaining, 32)?;
        let (name, limit) = match name {
            Name::Known(name) => (name, subtract(budget, name.span().len())?),
            Name::Literal(name) => name.decode_name(&mut *scratch.name, budget)?,
        };
        let decision = self.consumer.select(name);
        match decision {
            Selection::Skipped(field) => {
                let left = if let Some(identity) = &mut self.identity {
                    let (value, left) = value.decode_into(&mut *scratch.value, limit)?;
                    identity.try_field(name.span(), value.span())?;
                    field.validate(value.validation());
                    left
                } else {
                    let (len, valid) = value.validate(limit)?;
                    field.validate(valid);
                    subtract(limit, len)?
                };
                self.remaining = left;
            }
            Selection::Buffered(field) => {
                let (value, left) = value.decode_into(&mut *scratch.value, limit)?;
                if let Some(identity) = &mut self.identity {
                    identity.try_field(name.span(), value.span())?;
                }
                field
                    .push::<A>(value, indexing)
                    .map_err(Into::<lower::Error>::into)?;
                self.remaining = left;
            }
        }
        Ok(())
    }
}

pub(super) struct Scratch<'a> {
    name: &'a mut buffer::Names,
    value: &'a mut buffer::Values,
}

impl<'a> Scratch<'a> {
    pub(super) fn new(name: &'a mut buffer::Names, value: &'a mut buffer::Values) -> Self {
        Self { name, value }
    }
}

const _: () = {
    assert!(core::mem::size_of::<RawOutput<'static>>() == 2 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Scratch<'static>>() == 2 * core::mem::size_of::<usize>());
};
