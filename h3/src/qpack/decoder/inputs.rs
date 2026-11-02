use std::{marker, num::NonZero};

use sark_http::field::metadata::integer;

use crate::{
    qpack::{self, decoder::Error, literal},
    sources,
};

pub(super) trait Input<'a> {
    type Source: crate::Source + ?Sized + 'a;

    fn remaining(&self) -> usize;
    fn peek(&mut self) -> Option<u8>;
    fn prefixed<const BITS: u8>(&mut self) -> Result<integer::PrefixedInt<BITS>, Error>
    where
        integer::PrefixedInt<BITS>: integer::Width;
    fn literal<const BITS: u8>(&mut self) -> Result<literal::Literal<'a, Self::Source>, Error>
    where
        integer::PrefixedInt<BITS>: literal::Prefix;
}

pub(super) enum Prepared<I> {
    Ready(DecodePlan<I>),
    Blocked(qpack::Reference),
}

impl<I> Prepared<I> {
    pub(super) fn parse<'a>(
        mut input: I,
        inserts: u64,
        max: u64,
        retained: &mut Option<qpack::Reference>,
    ) -> Result<Self, Error>
    where
        I: Input<'a>,
    {
        let encoded = input.prefixed::<8>()?;
        let required = match *retained {
            Some(required) => qpack::Requirement::Dynamic(required),
            None => Self::required(encoded.get(), inserts, max)?,
        };
        let len = input.remaining();
        let sign = if input.peek().ok_or(Error::Incomplete { available: len })? & 0x80 != 0 {
            qpack::DeltaSign::Negative
        } else {
            qpack::DeltaSign::Positive
        };
        let delta = input.prefixed::<7>()?;
        if let qpack::Requirement::Dynamic(required) = required {
            *retained = Some(required);
            if !required.is_received(inserts) {
                return Ok(Self::Blocked(required));
            }
        }
        let base = Self::base(required.count(), delta.get(), sign)?;
        Ok(Self::Ready(DecodePlan {
            input,
            requirement: required,
            base,
        }))
    }

    fn required(encoded: u64, inserts: u64, max: u64) -> Result<qpack::Requirement, Error> {
        if encoded == 0 {
            return Ok(qpack::Requirement::Static);
        }
        let error = || Error::Required {
            encoded,
            inserted: inserts,
            maximum: max,
        };
        let range = max.checked_mul(2).ok_or_else(error)?;
        if encoded > range {
            return Err(error());
        }
        let upper = inserts.checked_add(max).ok_or_else(error)?;
        let wrapped = (upper / range) * range;
        let mut required = wrapped
            .checked_add(encoded)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(error)?;
        if required > upper {
            if required <= range {
                return Err(error());
            }
            required -= range;
        }
        let required = NonZero::new(required).ok_or_else(error)?;
        Ok(qpack::Requirement::Dynamic(qpack::Reference::from_count(
            required,
        )))
    }

    fn base(required: u64, delta: u64, sign: qpack::DeltaSign) -> Result<u64, Error> {
        let error = Error::Base {
            required,
            delta,
            sign,
        };
        match sign {
            qpack::DeltaSign::Negative => required
                .checked_sub(delta)
                .and_then(|value| value.checked_sub(1))
                .ok_or(error),
            qpack::DeltaSign::Positive => required.checked_add(delta).ok_or(error),
        }
    }
}

pub(crate) struct DecodePlan<I> {
    input: I,
    requirement: qpack::Requirement,
    base: u64,
}

impl<I> DecodePlan<I> {
    pub(crate) const fn requirement(&self) -> qpack::Requirement {
        self.requirement
    }

    pub(super) fn into_parts(self) -> (I, qpack::Requirement, u64) {
        (self.input, self.requirement, self.base)
    }
}

pub(crate) struct Segmented<'a, S: crate::Source + ?Sized> {
    cursor: sources::Cursor<'a, S::Chunks<'a>>,
    source: marker::PhantomData<&'a S>,
}

impl<'a, S: crate::Source + ?Sized> Segmented<'a, S> {
    pub(super) fn new(source: &'a S) -> Self {
        Self {
            cursor: source.cursor(),
            source: marker::PhantomData,
        }
    }
}

impl<'a, S: crate::Source + ?Sized> Input<'a> for Segmented<'a, S> {
    type Source = S;

    fn remaining(&self) -> usize {
        self.cursor.remaining()
    }

    fn peek(&mut self) -> Option<u8> {
        self.cursor.peek()
    }

    fn prefixed<const BITS: u8>(&mut self) -> Result<integer::PrefixedInt<BITS>, Error>
    where
        integer::PrefixedInt<BITS>: integer::Width,
    {
        let len = self.cursor.remaining();
        self.cursor
            .prefixed::<BITS>()
            .map_err(|error| Error::integer::<BITS>(len, error))
    }

    fn literal<const BITS: u8>(&mut self) -> Result<literal::Literal<'a, Self::Source>, Error>
    where
        integer::PrefixedInt<BITS>: literal::Prefix,
    {
        literal::Literal::parse_cursor::<BITS>(&mut self.cursor)
    }
}

impl<'a> Input<'a> for &'a [u8] {
    type Source = [u8];

    fn remaining(&self) -> usize {
        self.len()
    }

    fn peek(&mut self) -> Option<u8> {
        self.first().copied()
    }

    fn prefixed<const BITS: u8>(&mut self) -> Result<integer::PrefixedInt<BITS>, Error>
    where
        integer::PrefixedInt<BITS>: integer::Width,
    {
        let len = self.len();
        integer::PrefixedInt::<BITS>::take(self).map_err(|error| Error::integer::<BITS>(len, error))
    }

    fn literal<const BITS: u8>(&mut self) -> Result<literal::Literal<'a, Self::Source>, Error>
    where
        integer::PrefixedInt<BITS>: literal::Prefix,
    {
        literal::Literal::take::<BITS>(self)
    }
}
