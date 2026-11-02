//! Response progress for a separately retained, append-only source.

use core::convert::Infallible;

use crate::h1::head::{
    Limit, ParseError, field,
    response::{Parts, parts},
    state,
};

/// Response progress whose exact source prefix the caller preserves.
/// A failure consumes progress, preventing the failed fields from being replayed.
#[repr(transparent)]
pub struct State(state::State<parts::Kind>);

impl State {
    pub const fn new(limit: Limit) -> Self {
        Self(state::State::new(limit))
    }

    /// Parses appended bytes and emits newly completed fields.
    ///
    /// # Safety
    /// `source` must preserve the exact prefix and nondecreasing length from every prior advance or scan of this state.
    pub unsafe fn advance_from<'owner, E>(
        self,
        source: &'owner [u8],
        sink: impl FnMut(field::Entry<'owner>) -> Result<(), E>,
    ) -> Result<(Self, Option<Parts>), ParseError<E>> {
        let (state, parts) = self.0.parse(source, sink)?;
        Ok((Self(state), parts))
    }

    /// Advances validation without emitting parsed field entries.
    ///
    /// # Safety
    /// `source` must meet [`Self::advance_from`]'s safety contract for every prior advance or scan of this state.
    pub unsafe fn scan_from(
        self,
        source: &[u8],
    ) -> Result<(Self, Option<Parts>), ParseError<Infallible>> {
        let (state, parts) = self.0.scan(source)?;
        Ok((Self(state), parts))
    }
}
