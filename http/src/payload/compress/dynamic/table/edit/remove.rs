use o3::{depot, depot::map::compact};
use table::state;

use crate::payload::compress::dynamic::{mode, table};

pub(in crate::payload::compress::dynamic::table) struct Remove<'a, const N: usize> {
    next: state::raw::Pop<'a, mode::Direct>,
}

impl<'a> Remove<'a, 0> {
    pub(in crate::payload::compress::dynamic::table) fn new(
        next: state::raw::Pop<'a, mode::Direct>,
    ) -> Self {
        Self { next }
    }

    pub(in crate::payload::compress::dynamic::table) fn remove(self) {
        let exact = compact::Found::new(self).remove();
        compact::Found::new(exact).remove()
    }
}

unsafe impl<const N: usize> compact::raw::Read for Remove<'_, N> {
    fn layout(&self) -> compact::Layout {
        self.next
            .parts()
            .state
            .layout
            .index
            .layout(Key::staged::<N>())
    }

    fn bytes(&self) -> &[u8] {
        self.next.parts().index
    }
}

unsafe impl<const N: usize> compact::raw::Source for Remove<'_, N> {
    fn hash(value: compact::Value<'_, Self>) -> u64 {
        let record = unsafe {
            value
                .owner()
                .next
                .parts()
                .ledger
                .get_unchecked(value.index())
        };
        Key::staged::<N>().of(record.digest())
    }
}

unsafe impl<const N: usize> compact::raw::Write for Remove<'_, N> {
    unsafe fn bytes_mut(&mut self) -> &mut [u8] {
        unsafe { self.next.bytes_mut() }
    }
}

unsafe impl<const N: usize> compact::raw::Member for Remove<'_, N>
where
    Self: compact::raw::Remove,
{
    fn input(&self) -> compact::Value<'_, Self> {
        unsafe { compact::Value::new(depot::raw::Proof::new((self, self.next.raw()))) }
    }
}

unsafe impl<'a> compact::raw::Remove for Remove<'a, 0> {
    type Output = Remove<'a, 1>;

    fn removed(removed: compact::Removed<Self>) -> Self::Output {
        let owner = unsafe { compact::raw::Remove::into_owner(removed) };
        Remove { next: owner.next }
    }
}

unsafe impl compact::raw::Remove for Remove<'_, 1> {
    type Output = ();

    fn removed(removed: compact::Removed<Self>) -> Self::Output {
        let owner = unsafe { compact::raw::Remove::into_owner(removed) };
        owner.next.commit()
    }
}

use crate::payload::compress::dynamic::index::Key;
