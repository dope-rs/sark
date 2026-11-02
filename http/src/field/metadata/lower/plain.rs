use core::mem;

use metadata::{
    map::record,
    packed::prefix::Prefix,
    section::{rules, state, storage},
};
use o3::buffer::{pool, write::consume};
use state::fixed;

use crate::field::{head, metadata};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Name(usize),
    Value(usize),
    Layout([usize; 3]),
    Write(consume::Error),
}

#[doc(hidden)]
#[must_use = "push the stored field"]
pub struct Plain<'lease, 'field> {
    append: pool::Append<'lease, ([u8; Prefix::LEN], [&'field [u8]; 4]), pool::Span>,
}

impl<'lease, 'field> Plain<'lease, 'field> {
    pub fn try_new(
        lease: &'lease mut pool::Span,
        name: record::Span<'field>,
        value: record::Span<'field>,
        indexing: crate::field::Indexing,
    ) -> Result<Self, Error> {
        let name_len = name.len();
        let data_len = value.len();
        let prefix = Prefix::try_new(name_len, data_len, indexing)?.to_bytes();
        let start = lease.len();
        let value_at = start
            .checked_add(Prefix::LEN)
            .and_then(|start| start.checked_add(name_len))
            .ok_or(Error::Layout([start, name_len, data_len]))?;
        value_at
            .checked_add(data_len)
            .ok_or(Error::Layout([start, name_len, data_len]))?;
        let (name_a, name_b) = name.parts();
        let (value_a, value_b) = value.parts();
        let append = pool::Append::try_prefixed(lease, prefix, [name_a, name_b, value_a, value_b])
            .map_err(Error::Write)?;
        Ok(Self { append })
    }

    fn write(self) -> (&'lease mut pool::Span, record::View<'field>, usize, usize) {
        let indexing =
            crate::field::Indexing::from_never(self.append.input().0[Prefix::LEN - 1] != 0);
        let start = self.append.as_slice().len();
        let (lease, [name_a, name_b, value_a, value_b]) = self.append.commit();
        let name = record::Span::new(name_a, name_b);
        let value = record::Span::new(value_a, value_b);
        let value_at = start + Prefix::LEN + name.len();
        (
            lease,
            record::View::new(name, value).with_index(indexing),
            start,
            value_at,
        )
    }

    pub(in crate::field::metadata) fn push_raw<A: rules::Push>(
        self,
        scratch: &storage::Scratch,
        state: &mut fixed::Raw,
        known: head::Name,
    ) {
        let (lease, field, start, value_at) = self.write();
        let end = lease.len();
        let keep = state.retain::<A>(known, field, lease.as_slice(), scratch.authority());
        match keep {
            rules::Retention::Discard => lease.truncate(start),
            rules::Retention::Retain => state.store(known, value_at..end),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Error>() == 4 * mem::size_of::<usize>());
    assert!(
        mem::size_of::<Plain<'static, 'static>>()
            == mem::size_of::<record::View<'static>>() + 3 * mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Option<Plain<'static, 'static>>>()
            == mem::size_of::<Plain<'static, 'static>>()
    );
};
