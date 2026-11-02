use core::marker;

use sark_http::{
    field::{
        Indexing,
        metadata::{block, section::rules},
    },
    payload::compress::dynamic::{Field, mode},
};

use crate::qpack::{
    self,
    decoder::{Error, fields, inputs},
    dynamics, literal, statics,
};

fn indexing(first: u8, mask: u8) -> Indexing {
    if first & mask == 0 {
        Indexing::Allowed
    } else {
        Indexing::Never
    }
}

pub(super) trait Access<'a>: Copy {
    fn pre(self, base: u64, byte: u8, index: u64) -> Result<Field<'a>, Error>;

    fn post(self, base: u64, byte: u8, index: u64) -> Result<Field<'a>, Error>;
}

#[derive(Clone, Copy)]
pub(super) struct Static;

impl<'a> Access<'a> for Static {
    fn pre(self, _: u64, byte: u8, index: u64) -> Result<Field<'a>, Error> {
        Err(Error::DynamicDisabled { byte, index })
    }

    fn post(self, _: u64, byte: u8, index: u64) -> Result<Field<'a>, Error> {
        Err(Error::DynamicDisabled { byte, index })
    }
}

#[derive(Clone, Copy)]
pub(super) struct Dynamic<'a> {
    table: &'a dynamics::View<'a, mode::Staged>,
    required: qpack::Reference,
}

impl<'a> Dynamic<'a> {
    pub(super) const fn new(
        table: &'a dynamics::View<'a, mode::Staged>,
        required: qpack::Reference,
    ) -> Self {
        Self { table, required }
    }
}

impl<'a> Access<'a> for Dynamic<'a> {
    fn pre(self, base: u64, _: u8, index: u64) -> Result<Field<'a>, Error> {
        self.table
            .pre_base(self.required, base, index)
            .map_err(Error::DynamicReference)
    }

    fn post(self, base: u64, _: u8, index: u64) -> Result<Field<'a>, Error> {
        self.table
            .post_base(self.required, base, index)
            .map_err(Error::DynamicReference)
    }
}

pub(super) struct Lines<I, O> {
    pub(super) input: I,
    pub(super) base: u64,
    pub(super) output: O,
}

pub(super) struct Fields<'work, 'output, 'literal, P: rules::Policy, A> {
    pub(super) output: &'work mut fields::FieldOutput<'output, P>,
    pub(super) scratch: &'work mut fields::Scratch<'literal>,
    pub(super) push: marker::PhantomData<A>,
}

pub(super) trait Sink {
    fn indexed(&mut self, name: block::Named<'_>, value: block::Valued<'_>) -> Result<(), Error>;

    fn literal<S: crate::Source + ?Sized>(
        &mut self,
        name: fields::Name<'_, S>,
        value: literal::Literal<'_, S>,
        indexing: Indexing,
    ) -> Result<(), Error>;
}

impl<P: rules::Policy, A: rules::Push> Sink for Fields<'_, '_, '_, P, A> {
    fn indexed(&mut self, name: block::Named<'_>, value: block::Valued<'_>) -> Result<(), Error> {
        self.output.push_proven::<A>(name, value)
    }

    fn literal<S: crate::Source + ?Sized>(
        &mut self,
        name: fields::Name<'_, S>,
        value: literal::Literal<'_, S>,
        indexing: Indexing,
    ) -> Result<(), Error> {
        self.output
            .push::<A, S>(self.scratch, name, value, indexing)
    }
}

impl Sink for &mut fields::RawOutput<'_> {
    fn indexed(&mut self, name: block::Named<'_>, value: block::Valued<'_>) -> Result<(), Error> {
        self.push_proven(name.span(), value.span())
    }

    fn literal<S: crate::Source + ?Sized>(
        &mut self,
        name: fields::Name<'_, S>,
        value: literal::Literal<'_, S>,
        indexing: Indexing,
    ) -> Result<(), Error> {
        let name = match name {
            fields::Name::Known(name) => literal::Literal::raw_span(name.span()),
            fields::Name::Literal(name) => name,
        };
        self.push(name, value, indexing)
    }
}

impl<I, O: Sink> Lines<I, O> {
    pub(super) fn decode<'source, 'table, D: Access<'table>>(self, access: D) -> Result<(), Error>
    where
        I: inputs::Input<'source>,
    {
        let Self {
            mut input,
            base,
            mut output,
        } = self;
        while input.remaining() != 0 {
            let len = input.remaining();
            let first = input.peek().ok_or(Error::Incomplete { available: len })?;
            if first & 0x80 != 0 {
                let index = input.prefixed::<6>()?.get();
                if first & 0x40 != 0 {
                    let entry = statics::STATIC.resolve(index)?;
                    output.indexed(entry.name(), entry.valued())?;
                } else {
                    let field = access.pre(base, first, index)?;
                    output.indexed(field.name(), field.valued())?;
                }
                continue;
            }
            if first & 0xc0 == 0x40 {
                let index = input.prefixed::<4>()?.get();
                let value = input.literal::<7>()?;
                let name = if first & 0x10 != 0 {
                    let entry = statics::STATIC.resolve(index)?;
                    entry.name()
                } else {
                    let field = access.pre(base, first, index)?;
                    field.name()
                };
                output.literal(fields::Name::Known(name), value, indexing(first, 0x20))?;
                continue;
            }
            if first & 0xf0 == 0x10 {
                let index = input.prefixed::<4>()?.get();
                let field = access.post(base, first, index)?;
                output.indexed(field.name(), field.valued())?;
                continue;
            }
            if first & 0xf0 == 0x00 {
                let index = input.prefixed::<3>()?.get();
                let value = input.literal::<7>()?;
                let field = access.post(base, first, index)?;
                output.literal(
                    fields::Name::Known(field.name()),
                    value,
                    indexing(first, 0x08),
                )?;
                continue;
            }
            let name = input.literal::<3>()?;
            let value = input.literal::<7>()?;
            output.literal(fields::Name::Literal(name), value, indexing(first, 0x10))?;
        }
        Ok(())
    }
}

const _: () = {
    assert!(core::mem::size_of::<Static>() == 0);
    assert!(core::mem::size_of::<Dynamic<'_>>() == 2 * core::mem::size_of::<usize>());
};
