use o3::buffer::storage::inline;
use sark_http::field::metadata::integer;

use crate::qpack::encoder;

const _: () = assert!(usize::BITS <= u64::BITS);

pub(crate) struct Capacity<S = ()> {
    value: usize,
    state: S,
}

impl Capacity<()> {
    pub(super) fn new(value: usize) -> Self {
        Self { value, state: () }
    }

    pub(super) fn bind(self, state: &mut encoder::State) -> Capacity<&mut encoder::State> {
        Capacity {
            value: self.value,
            state,
        }
    }
}

impl Capacity<&mut encoder::State> {
    pub(crate) fn commit(self) {
        if let encoder::Tracking::Active { table, .. } = &mut self.state.outstanding {
            table.set_limit(self.value);
        }
    }
}

impl<S> Capacity<S> {
    pub(crate) fn pack(&self) -> inline::Bytes {
        integer::PrefixedInt::<5>::new(self.value as u64).pack(0x20)
    }
}

const _: () = {
    assert!(core::mem::size_of::<Capacity>() == core::mem::size_of::<usize>());
    assert!(
        core::mem::size_of::<Capacity<&mut encoder::State>>()
            == core::mem::size_of::<(usize, &mut encoder::State)>()
    );
};
