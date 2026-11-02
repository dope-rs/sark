use o3::alloc;
use sark_http::field::metadata::map::record;

/// Ordered decompressed field names and values, independent of QPACK encoding.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Identity(Vec<u8>);

impl Identity {
    pub(crate) const fn new() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn try_field(
        &mut self,
        name: record::Span<'_>,
        value: record::Span<'_>,
    ) -> Result<(), alloc::Error> {
        let extra = 16usize
            .checked_add(name.len())
            .and_then(|size| size.checked_add(value.len()))
            .ok_or(alloc::Error::Overflow)?;
        alloc::VecExt::try_reserve(&mut self.0, extra)?;
        self.0.extend_from_slice(&(name.len() as u64).to_le_bytes());
        name.extend(&mut self.0);
        self.0
            .extend_from_slice(&(value.len() as u64).to_le_bytes());
        value.extend(&mut self.0);
        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.0.capacity()
    }
}

const _: () = assert!(usize::BITS <= u64::BITS);
