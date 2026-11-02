//! JSON decoding contracts.

use std::num::NonZero;

use o3::buffer::storage;

use crate::writing;

/// Remaining containers below the current typed object; the root object is excluded.
#[derive(Clone, Copy, Debug)]
pub struct Depth(u8);

impl Depth {
    pub const ROOT: Self = Self(crate::nesting::MAX_DEPTH as u8);

    pub fn child(self) -> crate::Result<Self> {
        Ok(Self(self.nonzero()?.get() - 1))
    }

    pub(crate) fn nonzero(self) -> crate::Result<NonZero<u8>> {
        NonZero::new(self.0).ok_or(crate::Error::Depth)
    }
}

#[derive(Debug)]
pub enum Error<'a> {
    Parse(crate::Error),
    Missing(&'a str),
}

impl From<crate::Error> for Error<'_> {
    fn from(error: crate::Error) -> Self {
        Self::Parse(error)
    }
}

pub trait Decode: Sized {
    fn decode<'a>(input: storage::Shared) -> Result<Self, Error<'a>>;

    fn decode_ref<'a>(input: &'a [u8]) -> Result<Self, Error<'a>> {
        let input = storage::Shared::try_copy(input).map_err(crate::Error::from)?;
        Self::decode(input)
    }
}

/// Decodes a request-scoped view without copying the complete JSON body.
pub trait Request {
    type View<'req>: writing::TryEncode;

    fn decode<'req>(input: &'req [u8]) -> Result<Self::View<'req>, Error<'req>> {
        let (view, mut index) = Self::prefix(input, 0, Depth::ROOT)?;
        crate::input::Input::finish(input, &mut index)?;
        Ok(view)
    }

    /// Decodes one object prefix using the budget below that object.
    /// Callers entering a nested object must first consume [`Depth::child`].
    #[doc(hidden)]
    fn prefix<'req>(
        input: &'req [u8],
        index: usize,
        depth: Depth,
    ) -> Result<(Self::View<'req>, usize), Error<'req>>;
}
