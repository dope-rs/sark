use std::{mem, ops};

use dope::core::{Errno, random};
use o3::{
    alloc,
    alloc::VecExt as _,
    buffer::{
        storage::inline,
        write::{Reserved, Sink},
    },
    depot::map::compact,
};
use sark_http::{
    self, field,
    field::metadata::strings,
    payload::compress::dynamic::{hash, mode, plan},
};

use crate::{
    frame, qpack,
    qpack::{dynamics, ledger, stream},
};

mod buffer;
mod capacity;
mod delta;
mod error;
mod output;
mod section;
mod state;

pub(crate) use capacity::Capacity;
pub(crate) use delta::{Delta, Journal};
pub use error::Error;
pub(crate) use output::Output;
pub(crate) use section::{Peer, Section};
pub(crate) use state::State;
pub(super) use state::Tracking;

pub struct Encoder {
    state: State,
    pending: Wire,
}

/// An encoded field section and the exact QPACK state delta it depends on.
/// Dropping it leaves the committed encoder unchanged.
pub(crate) struct Prepared<'a, C = ()> {
    section: qpack::Section,
    commit: Commit<'a>,
    identity: C,
}

struct Encoding<'a> {
    section: qpack::Section,
    cache: buffer::Cache<'a>,
}

pub(crate) struct Commit<'a> {
    pub(in crate::qpack::encoder) tracking: Option<ledger::Tracked<'a>>,
    pub(in crate::qpack::encoder) section: buffer::Cache<'a>,
    pub(in crate::qpack::encoder) delta: Option<Journal<'a>>,
}

impl Encoder {
    const DEFAULT_OUTSTANDING_SECTIONS: usize = 1024;

    /// Creates a static-only encoder without allocating or acquiring entropy.
    pub fn new() -> Self {
        Self {
            state: State::new(Tracking::Disabled),
            pending: Wire::new(),
        }
    }

    pub fn try_table(table: usize) -> Result<Self, BuildError> {
        Self::try_bounded(table, Self::DEFAULT_OUTSTANDING_SECTIONS)
    }

    /// Creates an active dynamic encoder with fixed table and section bounds.
    /// The initial capacity instruction is returned by [`Self::take_wire`].
    pub fn try_bounded(table: usize, sections: usize) -> Result<Self, BuildError> {
        let plan = plan::Plan::<mode::Direct>::new(table).map_err(BuildError::Table)?;
        let sections = ledger::Plan::new(sections)?;
        let seed = random::Seed::acquire().map_err(BuildError::Entropy)?;
        let capacity = Capacity::new(table);
        Ok(Self {
            state: State::new(Tracking::Active {
                table: dynamics::Table::try_new(plan, table, hash::Seed::new(&seed))
                    .map_err(BuildError::Allocation)?,
                ledger: ledger::Ledger::try_new(sections).map_err(BuildError::Allocation)?,
            }),
            pending: Wire::from(&capacity),
        })
    }

    pub fn set_encoding(&mut self, encoding: strings::Encoding) {
        self.state.encoding = encoding;
    }

    pub fn set_blocked(&mut self, blocked: u64) {
        self.state.set_blocked(blocked);
    }

    pub fn take_wire(&mut self) -> Wire {
        mem::replace(&mut self.pending, Wire::new())
    }

    /// Applies decoder feedback. Cancellation releases current references;
    /// the caller owns application stream closure.
    pub fn ingest(&mut self, buf: &[u8]) -> Result<usize, stream::Error> {
        let published = self.state.insert_count();
        let mut tail = buf;
        while let Some(instruction) = stream::Instruction::take(&mut tail)? {
            self.state.apply(instruction, published)?;
        }
        Ok(buf.len() - tail.len())
    }

    /// Writes a field section under its HTTP/3 stream identity.
    pub fn encode<S: field::Source>(
        &mut self,
        stream: u64,
        fields: S,
        out: &mut Vec<u8>,
    ) -> Result<(), Error> {
        let prepared = Prepared::prepare(&mut self.state, stream, fields, ())?;
        let len = prepared.section.encoded_len();
        let out = match Reserved::try_grow(out, len) {
            Ok(out) => out,
            Err(error) => {
                prepared.discard();
                return Err(error.into());
            }
        };
        prepared.commit(&mut self.pending)?.write(out)
    }

    /// Encodes a field section under its HTTP/3 stream identity so QPACK
    /// acknowledgments and cancellations release exactly its references.
    pub fn encode_for<S: field::Source>(
        &mut self,
        stream: u64,
        fields: S,
    ) -> Result<qpack::Section, Error> {
        let prepared = Prepared::prepare(&mut self.state, stream, fields, ())?;
        Ok(prepared.commit(&mut self.pending)?.into_section())
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum BuildError {
    Entropy(Errno),
    Table(plan::Error),
    SectionCapacity {
        requested: usize,
    },
    SectionPlane {
        offset: usize,
        capacity: usize,
        width: usize,
    },
    SectionIndex(compact::Error),
    SectionLength {
        len: usize,
    },
    SectionOverflow(alloc::Overflow),
    Allocation(alloc::Error),
}

impl From<ledger::Error> for BuildError {
    fn from(error: ledger::Error) -> Self {
        match error {
            ledger::Error::Capacity(error) => Self::SectionCapacity {
                requested: error.requested(),
            },
            ledger::Error::Plane {
                offset,
                capacity,
                width,
            } => Self::SectionPlane {
                offset,
                capacity,
                width,
            },
            ledger::Error::Index(error) => Self::SectionIndex(error),
            ledger::Error::Length { len } => Self::SectionLength { len },
            ledger::Error::Overflow(error) => Self::SectionOverflow(error),
        }
    }
}

impl<'a, C> Prepared<'a, C> {
    pub(crate) fn prepare<S: field::Source>(
        encoder: &'a mut State,
        stream: u64,
        source: S,
        identity: C,
    ) -> Result<Self, Error>
    where
        C: section::Capture,
    {
        let mut section = Section::new(encoder, stream, identity)?;
        source.emit(&mut section);
        section.finish()
    }

    pub(crate) fn into_parts(self) -> (qpack::Section, Commit<'a>, C) {
        (self.section, self.commit, self.identity)
    }
}

impl<'a> Prepared<'a> {
    fn commit(self, pending: &mut Wire) -> Result<Encoding<'a>, Error> {
        let Self {
            section, commit, ..
        } = self;
        match commit.try_keep(pending) {
            Ok(()) => {
                let cache = commit.apply(true);
                Ok(Encoding { section, cache })
            }
            Err(error) => {
                commit.discard_vec(section.into_lines());
                Err(error)
            }
        }
    }

    fn discard(self) {
        self.commit.discard_vec(self.section.into_lines());
    }
}

impl Encoding<'_> {
    fn write(mut self, out: Reserved<'_>) -> Result<(), Error> {
        let mut out = out.into_staged();
        let result = self
            .section
            .try_prefix(&mut out)
            .and_then(|()| out.write_slice(self.section.lines()));
        let result = match result {
            Ok(()) => out
                .try_complete()
                .map(|out| {
                    out.commit();
                })
                .map_err(|error| {
                    let (expected, actual) = error.into_parts();
                    Error::WireLength {
                        expected: expected.get(),
                        actual,
                    }
                }),
            Err(error) => Err(Error::WireWrite {
                expected: out.capacity(),
                written: out.len(),
                additional: error.into_parts().0.get(),
            }),
        };
        self.cache.recycle(self.section.into_lines());
        result
    }

    fn into_section(self) -> qpack::Section {
        self.section
    }
}

impl<'a> Commit<'a> {
    pub(crate) fn wire_len(&self) -> usize {
        self.delta.as_ref().map_or(0, Journal::wire_len)
    }

    pub(crate) fn base(&self) -> u64 {
        self.delta.as_ref().map_or(0, Journal::base)
    }

    pub(crate) fn encode_wire(&self, out: &mut frame::Writer<'_>) {
        if let Some(delta) = &self.delta {
            delta.encode(out);
        }
    }

    pub(crate) fn recycle(&mut self, lines: Vec<u8>) {
        self.section.recycle(lines);
    }

    pub(crate) fn clear(self) {
        self.apply(true);
    }

    /// The native caller owns the encoded instructions while initial section
    /// references await publication.
    pub(crate) fn defer(self) {
        self.apply(false);
    }

    fn try_keep(&self, pending: &mut Wire) -> Result<(), Error> {
        let Some(delta) = &self.delta else {
            return Ok(());
        };
        pending
            .try_reserve(delta.wire_len())?
            .try_write(|writer| delta.encode(writer))
            .map_err(Error::from)
    }

    fn apply(mut self, published: bool) -> buffer::Cache<'a> {
        if let Some(tracking) = self.tracking.take() {
            tracking.apply(published);
        }
        if let Some(delta) = self.delta {
            delta.apply();
        }
        self.section
    }

    pub(crate) fn discard(self) {
        if let Some(delta) = self.delta {
            delta.rollback();
        }
    }

    pub(crate) fn discard_vec(mut self, lines: Vec<u8>) {
        self.section.recycle(lines);
        self.discard();
    }
}

/// Move-only QPACK encoder-stream bytes with allocation-free inline storage.
#[must_use = "QPACK encoder-stream bytes must precede dependent field sections"]
pub struct Wire {
    storage: Storage,
}

enum Storage {
    Inline(inline::Bytes),
    Owned(Vec<u8>),
}

impl Wire {
    pub(crate) const fn new() -> Self {
        Self {
            storage: Storage::Inline(inline::Bytes::new()),
        }
    }

    fn from(capacity: &Capacity) -> Self {
        Self {
            storage: Storage::Inline(capacity.pack()),
        }
    }

    /// One room comparison admits the inline owner; otherwise the wire reowns to grow.
    fn try_reserve(&mut self, extra: usize) -> Result<frame::Output<'_>, Error> {
        if let Storage::Inline(bytes) = &self.storage
            && extra > bytes.available()
        {
            let required = extra
                .checked_add(bytes.len())
                .ok_or(alloc::Error::Overflow)?;
            let mut owned = Vec::try_new(required)?;
            owned.extend_from_slice(bytes.as_ref());
            self.storage = Storage::Owned(owned);
        }
        match &mut self.storage {
            Storage::Inline(bytes) => Ok(frame::Output::Inline(bytes.fit(extra))),
            Storage::Owned(bytes) => Ok(frame::Output::Owned(Reserved::try_grow(bytes, extra)?)),
        }
    }
}

impl ops::Deref for Wire {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match &self.storage {
            Storage::Inline(bytes) => bytes.as_ref(),
            Storage::Owned(bytes) => bytes,
        }
    }
}

const _: () = assert!(core::mem::size_of::<Wire>() <= 4 * core::mem::size_of::<usize>());
