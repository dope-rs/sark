use sark_http::{
    field::metadata::{integer, map::record},
    payload::compress::dynamic::table,
};
use table::delta;

pub(crate) struct Journal<'a> {
    count: &'a mut u64,
    next: u64,
    wire_len: usize,
    inner: delta::Commit<delta::Borrow<'a>>,
}

impl<'a> Journal<'a> {
    pub(super) const fn new(
        count: &'a mut u64,
        next: u64,
        wire_len: usize,
        inner: delta::Commit<delta::Borrow<'a>>,
    ) -> Self {
        Self {
            count,
            next,
            wire_len,
            inner,
        }
    }

    pub(crate) const fn wire_len(&self) -> usize {
        self.wire_len
    }

    pub(crate) fn encode(&self, out: &mut impl for<'b> Extend<&'b [u8]>) {
        for field in self.inner.fields() {
            literal::<5>(field.name().span(), 0x40, out);
            literal::<7>(field.value(), 0, out);
        }
    }

    pub(crate) fn apply(self) {
        self.inner.apply();
        *self.count = self.next;
    }

    pub(crate) fn rollback(self) {
        self.inner.rollback();
    }
}

fn literal<const BITS: u8>(
    value: record::Span<'_>,
    prefix: u8,
    out: &mut impl for<'b> Extend<&'b [u8]>,
) where
    integer::PrefixedInt<BITS>: integer::Width,
{
    let bytes = integer::PrefixedInt::<BITS>::new(value.len() as u64).pack::<11>(prefix);
    let (first, second) = value.parts();
    out.extend([bytes.as_ref(), first, second]);
}

const _: () = {
    assert!(core::mem::size_of::<Journal<'_>>() <= 8 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Option<Journal<'_>>>() == core::mem::size_of::<Journal<'_>>());
};
