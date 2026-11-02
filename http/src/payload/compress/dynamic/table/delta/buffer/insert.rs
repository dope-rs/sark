use o3::{alloc::tail, depot, depot::map::compact};

use crate::payload::compress::dynamic::{
    index::Key,
    table::delta::buffer::{append::Ready, record},
};

pub(super) struct Insert<'a, 'field, const N: usize> {
    pub(super) ready: Ready<'a, 'field>,
}

unsafe impl<const N: usize> compact::raw::Read for Insert<'_, '_, N> {
    fn layout(&self) -> compact::Layout {
        self.ready.storage.header().index.layout(Key::staged::<N>())
    }

    fn bytes(&self) -> &[u8] {
        self.ready.storage.view().index()
    }
}

unsafe impl<const N: usize> compact::raw::Source for Insert<'_, '_, N> {
    fn hash(value: compact::Value<'_, Self>) -> u64 {
        let storage = &*value.owner().ready.storage;
        // SAFETY: input or an occupied directory member retains this initialized
        // record, including the new record before head.len publishes it.
        let pointer = unsafe {
            tail::View::new(&storage.block)
                .as_slice()
                .as_ptr()
                .cast::<record::Record>()
                .add(value.index())
        };
        let record = unsafe { &*pointer };
        Key::staged::<N>().of(record.digest())
    }
}

unsafe impl<const N: usize> compact::raw::Write for Insert<'_, '_, N> {
    unsafe fn bytes_mut(&mut self) -> &mut [u8] {
        self.ready.storage.index_mut()
    }
}

unsafe impl<'a, 'field> compact::raw::Insert for Insert<'a, 'field, 0> {
    type Output = Insert<'a, 'field, 1>;

    fn input(&self) -> compact::Value<'_, Self> {
        unsafe {
            compact::Value::new(depot::raw::Proof::new((
                self,
                self.ready.plan.raw_ordinal(),
            )))
        }
    }

    unsafe fn inserted(self) -> Self::Output {
        Insert { ready: self.ready }
    }
}

unsafe impl compact::raw::Insert for Insert<'_, '_, 1> {
    type Output = ();

    fn input(&self) -> compact::Value<'_, Self> {
        unsafe {
            compact::Value::new(depot::raw::Proof::new((
                self,
                self.ready.plan.raw_ordinal(),
            )))
        }
    }

    unsafe fn inserted(self) {
        let head = self.ready.storage.header_mut();
        head.len = self.ready.plan.next();
        head.used = self.ready.plan.record().end() as u32;
    }
}
