use o3::depot::map::compact;

use crate::payload::compress::dynamic::{
    index::{self, Key},
    mode,
    table::PartsMut,
};

// SAFETY: PartsMut retains the initialized directories and their original plan.
unsafe impl<const N: usize> compact::raw::Read
    for index::raw::Clear<PartsMut<'_, mode::Direct>, N>
{
    fn layout(&self) -> compact::Layout {
        self.0.state.layout.index.layout(Key::staged::<N>())
    }

    fn bytes(&self) -> &[u8] {
        self.0.index
    }
}

// SAFETY: PartsMut exclusively retains the same directory returned by Read.
unsafe impl<const N: usize> compact::raw::Write
    for index::raw::Clear<PartsMut<'_, mode::Direct>, N>
{
    unsafe fn bytes_mut(&mut self) -> &mut [u8] {
        self.0.index
    }
}
