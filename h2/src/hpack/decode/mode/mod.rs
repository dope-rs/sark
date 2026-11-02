use core::{convert::Infallible, mem};

use sark_http::field::{
    Indexing,
    metadata::{block, map::record, strings::Cursor},
};

use crate::hpack::decode::{self, limit, strings};

mod store;

pub(crate) use store::Store;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Index {
    Plain,
    Never,
    Indexed,
}

impl Index {
    pub(super) fn indexing(self) -> Indexing {
        match self {
            Self::Never => Indexing::Never,
            Self::Plain | Self::Indexed => Indexing::Allowed,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Facts {
    pub(super) len: usize,
    pub(super) index: Index,
}

#[derive(Clone, Copy)]
pub(crate) struct Emit;

#[derive(Clone, Copy)]
pub(crate) struct Discard;

#[derive(Clone, Copy)]
pub(crate) struct Emission(Facts);

#[derive(Clone, Copy)]
pub(crate) struct Omission(Facts);

const _: () = assert!(mem::size_of::<Emit>() == 0);
const _: () = assert!(mem::size_of::<Discard>() == 0);
const _: () = assert!(mem::size_of::<Index>() == mem::size_of::<bool>());
const _: () = assert!(mem::size_of::<Facts>() == 2 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Emission>() == mem::size_of::<Facts>());
const _: () = assert!(mem::size_of::<Omission>() == mem::size_of::<Facts>());

#[derive(Clone, Copy)]
pub(crate) enum Route<D> {
    Retain,
    Count,
    Validate,
    Direct(D),
}

#[derive(Clone, Copy)]
pub(crate) enum NameRoute {
    Retain,
    Count,
}

pub(crate) struct Name<'a> {
    pub(super) span: Option<block::Named<'a>>,
    pub(super) facts: Facts,
}

pub(crate) struct Field<'a> {
    pub(super) name: block::Named<'a>,
    pub(super) value: Value<'a>,
}

#[derive(Clone, Copy)]
pub(crate) enum Value<'a> {
    Retained(block::Valued<'a>),
    Counted(usize),
}

impl<'a> Value<'a> {
    pub(super) const fn new(value: Option<block::Valued<'a>>, len: usize) -> Self {
        match value {
            Some(value) => Self::Retained(value),
            None => Self::Counted(len),
        }
    }

    pub(super) const fn len(self) -> usize {
        match self {
            Self::Retained(value) => value.span().len(),
            Self::Counted(len) => len,
        }
    }
}

const _: () = assert!(mem::size_of::<Field<'_>>() == 10 * mem::size_of::<usize>());

pub(crate) trait Kind: Copy {
    type Direct: Copy;
    type Active: Active<Direct = Self::Direct>;

    fn new() -> Self;

    fn name(self) -> NameRoute;
}

pub(crate) trait Active: Copy {
    type Direct: Copy;

    fn route(self) -> Route<Self::Direct>;

    fn facts(self) -> Facts;
}

pub(crate) trait Direct<S: ?Sized>: Copy {
    type Error: From<decode::Error>;

    fn literal(
        self,
        sink: &mut S,
        input: strings::Encoded<'_>,
    ) -> Result<strings::Decoded, Self::Error>;

    fn string(self, sink: &mut S, string: &mut Cursor, input: &[u8]) -> Result<(), Self::Error>;
}

pub(crate) trait Flow<S: ?Sized>: Kind<Direct: Direct<S, Error = Self::Error>> {
    type Error: From<decode::Error>;

    fn validate(sink: &mut S, value: block::Validation) -> Result<(), Self::Error>;

    fn begin(self, sink: S, name: Name<'_>) -> Result<(Self::Active, S), (S, Self::Error)>
    where
        S: Sized;

    fn field(
        sink: S,
        budget: &mut limit::Budget,
        name: block::Named<'_>,
        value: block::Valued<'_>,
    ) -> Result<S, (S, Self::Error)>
    where
        S: Sized,
    {
        let (active, sink) = Self::new().begin(
            sink,
            Name {
                span: Some(name),
                facts: Facts {
                    len: name.span().len(),
                    index: Index::Indexed,
                },
            },
        )?;
        Self::finish(
            active,
            sink,
            budget,
            Field {
                name,
                value: Value::Retained(value),
            },
        )
    }

    fn finish(
        active: Self::Active,
        sink: S,
        budget: &mut limit::Budget,
        field: Field<'_>,
    ) -> Result<S, (S, Self::Error)>
    where
        S: Sized;
}

pub(super) fn carry<T, S, E>(sink: S, result: Result<T, impl Into<E>>) -> Result<(T, S), (S, E)> {
    match result {
        Ok(value) => Ok((value, sink)),
        Err(cause) => Err((sink, cause.into())),
    }
}

impl<S: ?Sized> Direct<S> for Infallible {
    type Error = decode::Error;

    fn literal(
        self,
        _: &mut S,
        _: strings::Encoded<'_>,
    ) -> Result<strings::Decoded, decode::Error> {
        match self {}
    }

    fn string(self, _: &mut S, _: &mut Cursor, _: &[u8]) -> Result<(), decode::Error> {
        match self {}
    }
}

impl Kind for Emit {
    type Direct = Infallible;
    type Active = Emission;

    fn new() -> Self {
        Self
    }

    fn name(self) -> NameRoute {
        NameRoute::Retain
    }
}

impl Active for Emission {
    type Direct = Infallible;

    fn route(self) -> Route<Self::Direct> {
        Route::Retain
    }

    fn facts(self) -> Facts {
        self.0
    }
}

impl<F> Flow<F> for Emit
where
    F: FnMut(record::Span<'_>, record::Span<'_>),
{
    type Error = decode::Error;

    fn validate(_: &mut F, _: block::Validation) -> Result<(), Self::Error> {
        Ok(())
    }

    fn begin(self, sink: F, name: Name<'_>) -> Result<(Self::Active, F), (F, Self::Error)> {
        Ok((Emission(name.facts), sink))
    }

    fn finish(
        active: Self::Active,
        mut emit: F,
        budget: &mut limit::Budget,
        field: Field<'_>,
    ) -> Result<F, (F, Self::Error)> {
        let facts = active.facts();
        let Value::Retained(value) = field.value else {
            budget.exceed(facts.len, field.value.len());
            return Ok(emit);
        };
        match budget.admit(facts.len, field.value.len()) {
            limit::Admission::Admitted => emit(field.name.span(), value.span()),
            limit::Admission::Exceeded => {}
        }
        Ok(emit)
    }
}

impl Kind for Discard {
    type Direct = Infallible;
    type Active = Omission;

    fn new() -> Self {
        Self
    }

    fn name(self) -> NameRoute {
        NameRoute::Count
    }
}

impl Active for Omission {
    type Direct = Infallible;

    fn route(self) -> Route<Self::Direct> {
        Route::Count
    }

    fn facts(self) -> Facts {
        self.0
    }
}

impl Flow<()> for Discard {
    type Error = decode::Error;

    fn validate(_: &mut (), _: block::Validation) -> Result<(), Self::Error> {
        Ok(())
    }

    fn begin(self, (): (), name: Name<'_>) -> Result<(Self::Active, ()), ((), Self::Error)> {
        Ok((Omission(name.facts), ()))
    }

    fn finish(
        active: Self::Active,
        (): (),
        budget: &mut limit::Budget,
        field: Field<'_>,
    ) -> Result<(), ((), Self::Error)> {
        let facts = active.facts();
        budget.admit(facts.len, field.value.len());
        Ok(())
    }
}
