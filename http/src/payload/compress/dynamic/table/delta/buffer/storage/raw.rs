use o3::depot::map::compact;

use crate::payload::compress::dynamic::{
    index::{self, Key},
    table::delta::buffer::Storage,
};

// SAFETY: Storage retains the initialized directories and their admitted plan.
unsafe impl<const N: usize> compact::raw::Read for index::raw::Clear<&mut Storage, N> {
    fn layout(&self) -> compact::Layout {
        self.0.header().index.layout(Key::staged::<N>())
    }

    fn bytes(&self) -> &[u8] {
        self.0.view().index()
    }
}

// SAFETY: Storage exclusively retains the same directory returned by Read.
unsafe impl<const N: usize> compact::raw::Write for index::raw::Clear<&mut Storage, N> {
    unsafe fn bytes_mut(&mut self) -> &mut [u8] {
        self.0.index_mut()
    }
}
