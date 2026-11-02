use o3::{
    buffer,
    buffer::{
        pool,
        pool::{owners, state},
        write::{self, SpareExt as _, consume},
    },
};

use crate::h1::head::request::pool::Layout;

pub struct Buf<'d> {
    pub(in crate::h1::head::request) bytes:
        pool::Span<owners::Plain<pool::Layout>, pool::Borrowed<'d>, state::Uninit>,
}

impl write::Sink for Buf<'_> {
    type Error = consume::Error;

    fn write_byte(&mut self, byte: u8) -> Result<(), Self::Error> {
        let mut out = self.bytes.spare();
        out.write_byte(byte)?;
        out.commit();
        Ok(())
    }

    fn write_slice(&mut self, source: &[u8]) -> Result<(), Self::Error> {
        let mut out = self.bytes.spare();
        out.write_slice(source)?;
        out.commit();
        Ok(())
    }

    fn write_slices<const N: usize>(&mut self, sources: [&[u8]; N]) -> Result<(), Self::Error> {
        let mut out = self.bytes.spare();
        out.write_slices(sources)?;
        out.commit();
        Ok(())
    }
}

// SAFETY: the unique lease preserves its range and bytes through shared access;
// every Sink operation only appends to its initialized suffix.
unsafe impl buffer::raw::Source for Buf<'_> {}

const _: () = assert!(size_of::<Buf<'static>>() == 2 * size_of::<usize>());

impl AsRef<[u8]> for Buf<'_> {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: only Open::buffer constructs Buf, after committing the prefix.
        unsafe { self.bytes.as_slice().get_unchecked(Layout::BYTES..) }
    }
}
