use core::{mem, num::NonZero};

use chunk::trailer;
pub use chunk::trailer::Pending;
use o3::{
    buffer::{
        resident,
        storage::Owned,
        write::{Sink as _, SpareExt as _, consume},
    },
    mem::Rejected,
};

use crate::{
    field,
    h1::{chunk, field::Entry},
};

/// Append-only ownership of the trailer prefix and its admitted syntax state.
pub struct State<'d> {
    source: Owned<resident::Budget<'d>>,
    parser: trailer::Cursor,
}

/// Newly admitted input retained with its original owner through capacity growth.
#[must_use = "append the admitted bytes to transfer their framing proof"]
pub struct Plan<'input, 'd, P = Phase<NonZero<usize>>> {
    state: State<'d>,
    bytes: &'input [u8],
    next: P,
}

pub struct Phase<T>(Next<T>);

enum Next<T> {
    More {
        cursor: trailer::Cursor,
        consumed: Option<T>,
    },
    Complete(T),
}

pub struct More<T> {
    cursor: trailer::Cursor,
    consumed: Option<T>,
}

pub struct Ready<T>(T);

#[must_use]
pub struct Append<'input, 'd, T>(Plan<'input, 'd, Ready<T>>);

/// The exact completed trailer owner, excluding the terminating empty line.
pub struct Complete<'d> {
    source: Owned<resident::Budget<'d>>,
}

#[derive(Debug)]
pub enum StoreError {
    Prefix(NonZero<usize>),
    Capacity(consume::Error),
}

impl Pending<'_> {
    pub const CAPACITY: usize = chunk::Framing::DEFAULT.trailer_bytes;

    /// Copies the exact borrowed prefix before transferring its parsing proof.
    pub fn try_store<'d>(
        self,
        mut target: Owned<resident::Budget<'d>>,
    ) -> Result<State<'d>, Rejected<(Self, Owned<resident::Budget<'d>>), StoreError>> {
        if let Err(error) = StoreError::copy(&mut target, self.bytes) {
            return Err(Rejected::new((self, target), error));
        }
        Ok(State {
            source: target,
            parser: self.cursor,
        })
    }
}

impl<'d> State<'d> {
    pub fn len(&self) -> usize {
        self.source.len()
    }

    pub fn is_empty(&self) -> bool {
        self.source.is_empty()
    }

    pub fn prepare<'input>(
        self,
        input: &'input [u8],
    ) -> Result<Plan<'input, 'd>, Rejected<Self, chunk::Error>> {
        let (next, bytes) = match self.parser.scan(input, &chunk::Framing::DEFAULT) {
            Ok(next) => next,
            Err(error) => return Err(Rejected::new(self, error)),
        };
        let next = match next {
            trailer::Progress::More { next, consumed } => Next::More {
                cursor: next,
                consumed,
            },
            trailer::Progress::Complete { consumed, .. } => Next::Complete(consumed),
        };
        Ok(Plan {
            state: self,
            bytes,
            next: Phase(next),
        })
    }
}

impl<'input, 'd> Plan<'input, 'd> {
    /// Binds each consumed prefix to its caller's linear input proof once.
    pub fn try_map<T, E>(
        self,
        bind: impl FnOnce(NonZero<usize>) -> Result<T, E>,
    ) -> Result<Plan<'input, 'd, Phase<T>>, Rejected<State<'d>, E>> {
        let next = match self.next.0 {
            Next::More { cursor, consumed } => {
                let consumed = match consumed.map(bind).transpose() {
                    Ok(consumed) => consumed,
                    Err(error) => return Err(Rejected::new(self.state, error)),
                };
                Next::More { cursor, consumed }
            }
            Next::Complete(consumed) => match bind(consumed) {
                Ok(consumed) => Next::Complete(consumed),
                Err(error) => return Err(Rejected::new(self.state, error)),
            },
        };
        Ok(Plan {
            state: self.state,
            bytes: self.bytes,
            next: Phase(next),
        })
    }
}

impl<'input, 'd, T> Plan<'input, 'd, Phase<T>> {
    pub fn split(self) -> Result<Plan<'input, 'd, Ready<T>>, Plan<'input, 'd, More<T>>> {
        match self.next.0 {
            Next::Complete(consumed) => Ok(Plan {
                state: self.state,
                bytes: self.bytes,
                next: Ready(consumed),
            }),
            Next::More { cursor, consumed } => Err(Plan {
                state: self.state,
                bytes: self.bytes,
                next: More { cursor, consumed },
            }),
        }
    }
}

impl<'d, P> Plan<'_, 'd, P> {
    pub fn len(&self) -> usize {
        self.state.len()
    }

    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    pub fn cancel(self) -> State<'d> {
        self.state
    }

    /// Copies only the retained prefix; the new input's admission remains intact.
    pub fn try_grow(
        &mut self,
        mut target: Owned<resident::Budget<'d>>,
    ) -> Result<(), Rejected<Owned<resident::Budget<'d>>, StoreError>> {
        if let Err(error) = StoreError::copy(&mut target, self.state.source.as_slice()) {
            return Err(Rejected::new(target, error));
        }
        self.state.source = target;
        Ok(())
    }
}

impl<'d, T> Plan<'_, 'd, More<T>> {
    pub fn try_append(&mut self) -> Result<Option<T>, consume::Error> {
        let mut spare = self.state.source.spare();
        spare.write_slice(self.bytes)?;
        spare.commit();
        self.state.parser = self.next.cursor;
        self.bytes = &[];
        Ok(self.next.consumed.take())
    }
}

impl<'input, 'd, T> Plan<'input, 'd, Ready<T>> {
    /// Borrows completed field lines before appending or releasing either source.
    pub fn fields(&self) -> [&[u8]; 2] {
        let prefix = self.state.source.as_slice();
        let left = prefix.len() - 2usize.saturating_sub(self.bytes.len());
        let right = self.bytes.len().saturating_sub(2);
        // SAFETY: Ready is constructed only from the completed trailer phase,
        // which admitted the final CRLF across this unchanged retained prefix
        // and exact borrowed input. Removing those two bytes cannot exceed
        // either source, including a CRLF split between the two owners.
        unsafe {
            [
                prefix.get_unchecked(..left),
                self.bytes.get_unchecked(..right),
            ]
        }
    }

    pub fn try_append(self) -> Result<Append<'input, 'd, T>, Rejected<Self, consume::Error>> {
        let source = &self.state.source;
        match consume::Error::check(self.bytes.len(), source.capacity() - source.len()) {
            Ok(()) => Ok(Append(self)),
            Err(error) => Err(Rejected::new(self, error)),
        }
    }
}

impl<'input, 'd, T> Append<'input, 'd, T> {
    pub fn cancel(self) -> Plan<'input, 'd, Ready<T>> {
        self.0
    }

    pub fn commit(self) -> (Complete<'d>, T) {
        let plan = self.0;
        let mut source = plan.state.source;
        let start = source.len();
        let bytes = plan.bytes;
        // SAFETY: admission retained this exclusive owner with room for all bytes.
        // Ready proved the final CRLF spans the two disjoint slices.
        unsafe {
            if bytes.len() > 2 {
                bytes
                    .as_ptr()
                    .copy_to_nonoverlapping(source.as_mut_ptr().add(start), bytes.len() - 2);
            }
            source.set_len(start + bytes.len() - 2);
        }
        (Complete { source }, plan.next.0)
    }
}

impl<'d> Complete<'d> {
    pub fn fields(&self) -> Fields<'_> {
        Fields::new(self.source.as_slice())
    }

    pub fn into_owner(self) -> Owned<resident::Budget<'d>> {
        self.source
    }
}

impl StoreError {
    fn copy(target: &mut Owned<resident::Budget<'_>>, bytes: &[u8]) -> Result<(), Self> {
        if let Some(len) = NonZero::new(target.len()) {
            return Err(Self::Prefix(len));
        }
        target.try_extend(bytes).map_err(Self::Capacity)
    }
}

/// Admitted field lines, excluding the section's terminating empty line.
/// Each retained line has a permitted name, colon, value, and CRLF.
pub struct Fields<'a> {
    lines: &'a [u8],
}

impl<'a> Fields<'a> {
    pub(super) fn new(lines: &'a [u8]) -> Self {
        Self { lines }
    }

    /// Returns the remaining field section without the terminating empty line.
    pub const fn section(&self) -> &'a [u8] {
        self.lines
    }
}

impl<'a> Iterator for Fields<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let (name, value, rest) = Entry::split(self.lines)?;
        self.lines = rest;
        Some((name, field::Error::trim_ows(value)))
    }
}

const _: () = assert!(mem::size_of::<Fields<'_>>() == 2 * mem::size_of::<usize>());
