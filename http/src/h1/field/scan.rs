use core::{num::NonZero, ops};

use o3::buffer::{self, write};
use write::{CopyError, SourceExt as _};

use crate::h1::field::{self, Cursor, Entry, Limit, syntax};

/// Append-only ownership and syntax progress through a selected field suffix.
/// Bytes before the starting offset are retained unchanged, but are not validated.
pub struct State<S> {
    source: S,
    cursor: Cursor,
}

pub enum Progress<S> {
    More(State<S>),
    Complete(Complete<S>),
}

/// The original owner and the suffix terminator established by its field cursor.
/// This does not certify any bytes preceding the cursor's starting offset.
pub struct Complete<S> {
    source: S,
    end: field::End,
}

#[derive(Debug)]
pub enum Error<E> {
    Syntax(syntax::Error),
    Sink(E),
}

impl<S: buffer::raw::Source> State<S> {
    pub const fn source(&self) -> &S {
        &self.source
    }

    /// Starts syntax admission at `start`, charging new fields after `seen`.
    /// The skipped prefix and its count remain the caller's proof obligations.
    pub fn try_new(
        source: S,
        start: usize,
        seen: usize,
        fields: usize,
        line: usize,
    ) -> Result<Self, (S, syntax::Error)> {
        if start > source.as_ref().len() {
            return Err((source, syntax::Error::Invalid));
        }
        match Cursor::seed(start, seen, Limit::new(fields, line)) {
            Ok(cursor) => Ok(Self { source, cursor }),
            Err(error) => Err((source, error)),
        }
    }

    pub fn advance<E>(
        mut self,
        mut sink: impl FnMut(Entry<'_>) -> Result<(), E>,
    ) -> Result<Progress<S>, (S, Error<E>)> {
        loop {
            let progress = self
                .cursor
                .advance(self.source.as_ref(), |field| match sink(field) {
                    Ok(()) => ops::ControlFlow::Continue(()),
                    Err(error) => ops::ControlFlow::Break(error),
                });
            match progress {
                Ok(field::Progress::Emitted(())) => {}
                Ok(field::Progress::More) => return Ok(Progress::More(self)),
                Ok(field::Progress::End(end)) => {
                    return Ok(Progress::Complete(Complete {
                        source: self.source,
                        end,
                    }));
                }
                Ok(field::Progress::Break(error)) => return Err((self.source, Error::Sink(error))),
                Err(error) => return Err((self.source, Error::Syntax(error))),
            }
        }
    }

    /// Copies the exact source while retaining its already established progress.
    pub fn try_copy<D>(&self, target: D) -> Result<State<D>, (D, CopyError<D::Error>)>
    where
        D: buffer::raw::Source + write::Sink,
    {
        let target = target.try_copy(self.source.as_ref())?;
        Ok(State {
            source: target,
            cursor: self.cursor,
        })
    }
}

impl<S: write::Sink> State<S> {
    pub fn try_extend(&mut self, bytes: &[u8]) -> Result<(), S::Error> {
        self.source.write_slice(bytes)
    }
}

impl<S: buffer::raw::Source> AsRef<[u8]> for State<S> {
    fn as_ref(&self) -> &[u8] {
        self.source.as_ref()
    }
}

impl<S> Complete<S> {
    /// Returns the owner, empty-line start, and end of the scanned suffix.
    /// The prefix before the original starting offset is not certified here.
    pub fn into_parts(self) -> (S, usize, NonZero<u32>) {
        (self.source, self.end.fields() as usize, self.end.wire_len())
    }
}
