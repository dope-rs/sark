use core::{mem, num::NonZero};

use sark_http::field::metadata::integer::PrefixedInt;

use crate::hpack::decode::{self, mode, strings};

pub(super) enum Kind<'a> {
    Indexed(usize),
    Literal {
        index: mode::Index,
        name: Name<'a>,
        value: strings::Encoded<'a>,
    },
    Resize(usize),
}

pub(super) enum Name<'a> {
    Literal(strings::Encoded<'a>),
    Indexed(NonZero<usize>),
}

impl<'a> Kind<'a> {
    pub(super) fn parse(input: &mut &'a [u8]) -> Result<Self, decode::Error> {
        let Some(&first) = input.first() else {
            return Err(decode::Error::NeedMore);
        };
        if first & 0x80 != 0 {
            let index = PrefixedInt::<7>::take(input)?;
            return Ok(Self::Indexed(index.get() as usize));
        }
        if first & 0xC0 == 0x40 {
            let index = PrefixedInt::<6>::take(input)?;
            return Self::literal(input, index.get() as usize, mode::Index::Indexed);
        }
        if first & 0xE0 == 0x20 {
            let size = PrefixedInt::<5>::take(input)?;
            return Ok(Self::Resize(size.get() as usize));
        }
        let index = PrefixedInt::<4>::take(input)?;
        let kind = if first & 0x10 != 0 {
            mode::Index::Never
        } else {
            mode::Index::Plain
        };
        Self::literal(input, index.get() as usize, kind)
    }

    fn literal(
        input: &mut &'a [u8],
        name: usize,
        index: mode::Index,
    ) -> Result<Self, decode::Error> {
        let name = match NonZero::<usize>::new(name) {
            Some(index) => Name::Indexed(index),
            None => Name::Literal(strings::Encoded::take(input)?),
        };
        let value = strings::Encoded::take(input)?;
        Ok(Self::Literal { index, name, value })
    }
}

const _: () = {
    assert!(mem::size_of::<usize>() >= mem::size_of::<u64>());
    assert!(mem::size_of::<Name<'_>>() <= 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Kind<'_>>() <= 9 * mem::size_of::<usize>());
};
