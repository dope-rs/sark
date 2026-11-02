use std::slice;

use delta::buffer::{self, Storage};
use dope::core::random;
use dynamic::{hash, mode, plan, table};
use o3::{
    alloc,
    buffer::write::{self, Sink as _},
};
use sark_http::{
    self,
    field::{
        self, Writer,
        metadata::{
            block::Connect,
            integer,
            map::record::{Entry, Match},
        },
    },
    payload::{compress::dynamic, response},
};
use table::delta;

use crate::hpack::{
    capacity,
    encode::{Commit, Create, Error, Mode, fields, literal},
    statics,
};

pub struct Encoder {
    dyn_table: table::Table<mode::Direct>,
    buffer: Storage,
    max_size_setting: usize,
    pending_minimum: Option<usize>,
    pub(in crate::hpack::encode) mode: Mode,
}

struct Output<'a> {
    append: write::Append<'a>,
    result: Result<(), alloc::Error>,
}

impl Encoder {
    pub fn try_new(max_size: usize) -> Result<Self, capacity::Error> {
        let plan = plan::Plan::<mode::Direct>::new(max_size)?;
        let seed = random::Seed::acquire().map_err(capacity::Error::Entropy)?;
        let table = plan
            .try_table(max_size, hash::Seed::new(&seed))
            .map_err(capacity::Error::from)?;
        let buffer = try_buffer(max_size).map_err(capacity::Error::Buffer)?;
        Ok(Self::from_table(table, max_size, buffer))
    }

    pub(crate) fn try_with(
        plan: plan::Plan<mode::Direct>,
        limit: usize,
        list_cap: usize,
        seed: hash::Seed,
    ) -> Result<Self, Create> {
        let maximum = limit.min(plan.capacity());
        let table = plan.try_table(maximum, seed).map_err(Create::Table)?;
        let buffer = try_buffer(list_cap).map_err(Create::Buffer)?;
        let mut encoder = Self::from_table(table, maximum, buffer);
        encoder.pending_minimum = (maximum != limit).then_some(maximum);
        Ok(encoder)
    }

    fn from_table(table: table::Table<mode::Direct>, limit: usize, buffer: Storage) -> Self {
        Self {
            dyn_table: table,
            buffer,
            max_size_setting: limit,
            pending_minimum: None,
            mode: Mode::Shortest,
        }
    }

    pub fn set_max_size(&mut self, n: usize) {
        self.apply_limit(n.min(self.dyn_table.capacity()));
    }

    pub fn capacity(&self) -> usize {
        self.dyn_table.capacity()
    }

    pub fn max_size(&self) -> usize {
        self.max_size_setting
    }

    fn apply_limit(&mut self, limit: usize) {
        if limit == self.max_size_setting {
            return;
        }
        self.pending_minimum = Some(
            self.pending_minimum
                .map_or(limit, |minimum| minimum.min(limit)),
        );
        self.max_size_setting = limit;
        self.dyn_table.edit().set_limit(limit);
    }

    pub fn set_mode(&mut self, mode: Mode) {
        self.mode = mode;
    }

    /// Retains the output prefix and rolls back this block's table changes on failure.
    pub fn try_encode<'a, I>(&mut self, headers: I, out: &mut Vec<u8>) -> Result<(), Error>
    where
        I: IntoIterator<Item = Entry<'a>>,
    {
        let mode = self.mode;
        let mut out = Output {
            append: write::Append::new(out),
            result: Ok(()),
        };
        let mut draft = self.begin_block(&mut out);
        let result = out.result.map_err(Error::Allocation).and_then(|()| {
            headers.into_iter().try_for_each(|header| {
                Self::encode_field(&mut draft, header, &mut out, mode).map_err(Error::Dynamic)?;
                out.result.map_err(Error::Allocation)
            })
        });
        if let Err(cause) = result {
            draft.rollback();
            out.append.rollback();
            return Err(cause);
        }
        draft.finish().apply();
        self.finish_block();
        Ok(())
    }

    pub(in crate::hpack::encode) fn begin_block(
        &mut self,
        out: &mut impl Extend<u8>,
    ) -> delta::Draft<delta::Borrow<'_>> {
        self.prefix(out);
        delta::Draft::new(delta::Borrow::new(&mut self.dyn_table, &mut self.buffer))
    }

    pub(in crate::hpack::encode) fn prefix(&self, out: &mut impl Extend<u8>) {
        if let Some(minimum) = self.pending_minimum {
            let n = self.max_size_setting;
            if minimum != n {
                integer::PrefixedInt::<5>::new(minimum as u64).encode(0x20, out);
            }
            integer::PrefixedInt::<5>::new(n as u64).encode(0x20, out);
        }
    }

    pub(in crate::hpack::encode) fn finish_block(&mut self) {
        self.pending_minimum = None;
    }

    pub(crate) fn prepare<S, W>(
        self,
        source: S,
        out: &mut W,
        limit: usize,
        connect: Connect,
        method: Option<&mut response::Method>,
    ) -> (Result<(), fields::Error>, Commit)
    where
        S: field::Source,
        W: Extend<u8> + Writer,
    {
        let mut sink = fields::Sink::new(self, out, limit, connect, method);
        source.emit(&mut sink);
        sink.finish()
    }

    pub(in crate::hpack::encode) fn encode_field<W, T: delta::raw::Source>(
        draft: &mut delta::Draft<T>,
        header: Entry<'_>,
        out: &mut W,
        mode: Mode,
    ) -> Result<(), delta::Fault>
    where
        W: Extend<u8> + Writer,
    {
        if header.indexing == field::Indexing::Never {
            Self::encode_name(draft, header.name, header.indexing, out, mode);
            literal::encode(header.value, mode, out);
            return Ok(());
        }
        let fixed = statics::TABLE.lookup(header.name, header.value);
        if let Some(index) = fixed.as_ref().and_then(|found| found.exact) {
            integer::PrefixedInt::<7>::new(index as u64).encode(0x80, out);
            return Ok(());
        }
        let mut query = (draft.max_size() != 0).then(|| draft.query(header));
        let dynamic = match query.as_ref() {
            Some(query) if fixed.is_some() => Match {
                exact: query.exact_index(),
                name: None,
            },
            Some(query) => query.both_index(),
            None => Match::none(),
        };
        if let Some(index) = dynamic.exact {
            let absolute = statics::TABLE.len() + 1 + index;
            integer::PrefixedInt::<7>::new(absolute as u64).encode(0x80, out);
            return Ok(());
        }
        let name_id = match fixed {
            Some(found) => Some(found.name),
            None => dynamic.name.map(|index| statics::TABLE.len() + 1 + index),
        };
        let indexed = match query.as_mut().map(|query| query.try_insert()) {
            Some(Ok(())) => true,
            None
            | Some(Err(
                delta::Failure::Length { .. }
                | delta::Failure::Capacity { .. }
                | delta::Failure::Name { .. }
                | delta::Failure::Value { .. }
                | delta::Failure::Entries { .. }
                | delta::Failure::Bytes { .. },
            )) => false,
            Some(Err(delta::Failure::Fault(cause))) => return Err(cause),
        };
        match name_id {
            Some(index) => {
                if indexed {
                    integer::PrefixedInt::<6>::new(index as u64).encode(0x40, out);
                } else {
                    integer::PrefixedInt::<4>::new(index as u64).encode(0, out);
                }
            }
            None => {
                out.write_byte(if indexed { 0x40 } else { 0 });
                literal::encode(header.name, mode, out);
            }
        }
        literal::encode(header.value, mode, out);
        Ok(())
    }

    pub(in crate::hpack::encode) fn encode_name<W, T: delta::raw::Source>(
        draft: &delta::Draft<T>,
        name: &[u8],
        indexing: field::Indexing,
        out: &mut W,
        mode: Mode,
    ) where
        W: Extend<u8> + Writer,
    {
        let name_id = match statics::TABLE.lookup(name, &[]) {
            Some(found) => Some(found.name),
            None if draft.max_size() == 0 => None,
            None => draft
                .needle_index(name)
                .map(|index| statics::TABLE.len() + 1 + index),
        };
        let prefix = match indexing {
            field::Indexing::Allowed => 0,
            field::Indexing::Never => 0x10,
        };
        match name_id {
            Some(index) => integer::PrefixedInt::<4>::new(index as u64).encode(prefix, out),
            None => {
                out.write_byte(prefix);
                literal::encode(name, mode, out);
            }
        }
    }
}

impl Writer for Output<'_> {
    fn write_byte(&mut self, byte: u8) {
        self.write_slice(slice::from_ref(&byte));
    }

    fn write_slice(&mut self, bytes: &[u8]) {
        if self.result.is_ok() {
            self.result = self.append.write_slice(bytes);
        }
    }
}

impl Extend<u8> for Output<'_> {
    fn extend<T: IntoIterator<Item = u8>>(&mut self, iter: T) {
        if self.result.is_ok() {
            self.result = iter
                .into_iter()
                .try_for_each(|byte| self.append.write_byte(byte));
        }
    }
}

fn try_buffer(limit: usize) -> Result<Storage, buffer::Error> {
    Storage::try_new(limit, limit / dynamic::ENTRY_OVERHEAD)
}

unsafe impl delta::raw::Source for Encoder {
    fn parts(&self) -> (&table::Table<mode::Direct>, &Storage) {
        (&self.dyn_table, &self.buffer)
    }

    fn parts_mut(&mut self) -> (&mut table::Table<mode::Direct>, &mut Storage) {
        (&mut self.dyn_table, &mut self.buffer)
    }
}
