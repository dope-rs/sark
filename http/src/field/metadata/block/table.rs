use crate::field::metadata::{block, map::record::Span};

/// Immutable entries with admitted values and cached name classes.
pub struct Table<'a, const N: usize> {
    entries: [(&'a [u8], &'a [u8]); N],
    names: [block::Name; N],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error {
    pub index: usize,
}

/// One bounds-checked entry retaining its table and source bytes.
#[derive(Clone, Copy)]
pub struct Entry<'table, 'data> {
    bytes: &'table (&'data [u8], &'data [u8]),
    proof: block::Name,
}

impl<'a, const N: usize> Table<'a, N> {
    pub const fn try_new(entries: [(&'a [u8], &'a [u8]); N]) -> Result<Self, Error> {
        let mut names = [block::Name::REGULAR; N];
        let mut index = 0;
        while index < N {
            let (name, value) = entries[index];
            let mut scan = block::Value::new();
            let mut offset = 0;
            while offset < value.len() {
                scan.push(value[offset]);
                offset += 1;
            }
            if !scan.is_valid() {
                return Err(Error { index });
            }
            names[index] = block::Name::classify(name);
            index += 1;
        }
        Ok(Self { entries, names })
    }

    pub const fn len(&self) -> usize {
        N
    }

    pub const fn is_empty(&self) -> bool {
        N == 0
    }

    pub const fn entries(&self) -> &[(&'a [u8], &'a [u8]); N] {
        &self.entries
    }

    pub fn get(&self, index: usize) -> Option<Entry<'_, 'a>> {
        let bytes = self.entries.get(index)?;
        Some(Entry {
            bytes,
            proof: self.names[index],
        })
    }
}

impl<'a> Entry<'_, 'a> {
    pub const fn name(self) -> block::Named<'a> {
        block::Named::from_scanned(Span::from_slice(self.bytes.0), self.proof)
    }

    pub const fn value(self) -> &'a [u8] {
        self.bytes.1
    }

    pub const fn valued(self) -> block::Valued<'a> {
        block::Valued::from_scanned(Span::from_slice(self.bytes.1), block::Validation::Valid)
    }

    pub const fn validation(self) -> block::Validation {
        if self.proof.is_forbidden() {
            block::Validation::Malformed
        } else {
            block::Validation::Valid
        }
    }
}

const _: () = assert!(core::mem::size_of::<Entry<'_, '_>>() == 2 * core::mem::size_of::<usize>());
