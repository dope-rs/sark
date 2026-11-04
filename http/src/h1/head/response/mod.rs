//! Response-head semantics and owner-free wire parts.

mod admitted;
mod coding;
mod date;
mod fields;
mod parts;
pub mod raw;
mod server;

pub use admitted::Admitted;
pub use coding::Coding;
pub use date::Date;
pub use parts::Parts;
pub use server::Server;

use core::{convert::Infallible, marker::PhantomData};

use o3::buffer::{
    self,
    resident::staging::{self, reserve},
    write,
};

use crate::h1::head::{Limit, ParseError, field, response};
use crate::payload::response::Method;

/// Append-only input retaining the identity of one response parser.
pub struct State<'id, S> {
    source: S,
    brand: PhantomData<*mut &'id ()>,
}

/// Progress tied to one input owner; failures consume it without replaying fields.
pub struct Progress<'id> {
    inner: response::raw::State,
    brand: PhantomData<*mut &'id ()>,
}

impl<S> State<'_, S> {
    pub fn with<R>(
        source: S,
        request: Method,
        limit: Limit,
        run: impl for<'id> FnOnce(State<'id, S>, Progress<'id>) -> R,
    ) -> R {
        let brand = PhantomData;
        run(
            State { source, brand },
            Progress {
                inner: response::raw::State::new(request, limit),
                brand,
            },
        )
    }

    /// Releases the source, leaving no owner that can resume this parser.
    pub fn into_inner(self) -> S {
        self.source
    }
}

impl<'id, S: buffer::raw::Source> State<'id, S> {
    /// Borrows the retained source without permitting exclusive mutation.
    pub fn source(&self) -> &S {
        &self.source
    }
}

impl<'id> Progress<'id> {
    /// Parses appended bytes, retaining field and error borrows on their input owner.
    pub fn advance<'owner, S: buffer::raw::Source, E>(
        self,
        source: &'owner State<'id, S>,
        sink: impl FnMut(field::Entry<'owner>) -> Result<(), E>,
    ) -> Result<(Self, Option<Parts>), ParseError<E>> {
        // SAFETY: this invariant identity admits only its retained source; all source
        // transitions preserve the exact prefix, and shared access prevents mutation.
        let (inner, parts) = unsafe { self.inner.advance_from(source.as_ref(), sink) }?;
        Ok((Self { inner, ..self }, parts))
    }

    /// Advances validation without emitting parsed field entries.
    pub fn scan<S: buffer::raw::Source>(
        self,
        source: &State<'id, S>,
    ) -> Result<(Self, Option<Parts>), ParseError<Infallible>> {
        self.advance(source, |_| Ok(()))
    }
}

impl<S: buffer::raw::Source> AsRef<[u8]> for State<'_, S> {
    fn as_ref(&self) -> &[u8] {
        self.source.as_ref()
    }
}

impl<S: buffer::raw::Source + write::Sink> State<'_, S> {
    pub fn try_extend(&mut self, bytes: &[u8]) -> Result<(), S::Error> {
        self.source.write_slice(bytes)
    }
}

impl<'d> State<'_, staging::Buf<'d>> {
    pub fn try_reserve(&mut self, target: usize) -> Result<(), reserve::Error> {
        self.source.try_reserve(target)
    }

    pub fn try_grow(&mut self) -> Result<(), reserve::Error> {
        self.source.try_grow()
    }
}

const _: () = {
    assert!(core::mem::size_of::<State<'static, ()>>() == 0);
    assert!(core::mem::size_of::<Progress<'static>>() <= 104);
};
