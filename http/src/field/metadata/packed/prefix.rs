use crate::field::{
    Indexing,
    metadata::{lower::plain, map::record},
};

/// Native packed-field prefix shared by retained field owners.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Prefix {
    name_len: u32,
    value_len: u32,
    indexing: Indexing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Prefix { required: usize, available: usize },
    Length { name: usize, value: usize },
    Payload { required: usize, available: usize },
    Indexing { value: u8 },
}

impl Prefix {
    pub const LEN: usize = 9;

    pub(in crate::field::metadata) const fn new(
        name_len: u32,
        vlen: u32,
        indexing: Indexing,
    ) -> Self {
        Self {
            name_len,
            value_len: vlen,
            indexing,
        }
    }

    pub(in crate::field::metadata) fn try_new(
        name: usize,
        value: usize,
        indexing: Indexing,
    ) -> Result<Self, plain::Error> {
        use plain::Error;
        let name_len = u32::try_from(name).map_err(|_| Error::Name(name))?;
        let vlen = u32::try_from(value).map_err(|_| Error::Value(value))?;
        Ok(Self::new(name_len, vlen, indexing))
    }

    pub(in crate::field::metadata) fn to_bytes(self) -> [u8; Self::LEN] {
        let [n0, n1, n2, n3] = self.name_len.to_ne_bytes();
        let [v0, v1, v2, v3] = self.value_len.to_ne_bytes();
        [n0, n1, n2, n3, v0, v1, v2, v3, self.indexing as u8]
    }

    pub fn try_read(bytes: &[u8]) -> Result<Self, Error> {
        let Some(prefix) = bytes.first_chunk() else {
            return Err(Error::Prefix {
                required: Self::LEN,
                available: bytes.len(),
            });
        };
        let indexing =
            Indexing::from_byte(prefix[8]).ok_or(Error::Indexing { value: prefix[8] })?;
        Ok(Self::decode(prefix, indexing))
    }

    pub(super) fn decode(prefix: &[u8; Self::LEN], indexing: Indexing) -> Self {
        // SAFETY: the admitted nine-byte prefix contains both native-endian u32
        // lengths. Reading them unaligned preserves every possible bit pattern.
        let [name_len, vlen] = unsafe { prefix.as_ptr().cast::<[u32; 2]>().read_unaligned() };
        Self {
            name_len,
            value_len: vlen,
            indexing,
        }
    }

    pub const fn name_len(self) -> u32 {
        self.name_len
    }

    pub const fn value_len(self) -> u32 {
        self.value_len
    }

    pub const fn indexing(self) -> Indexing {
        self.indexing
    }

    pub fn try_take<'a>(
        self,
        bytes: &'a [u8],
        name_len: usize,
    ) -> Result<(record::Entry<'a>, &'a [u8]), Error> {
        let fields = bytes.get(Self::LEN..).ok_or(Error::Prefix {
            required: Self::LEN,
            available: bytes.len(),
        })?;
        let value = self.value_len as usize;
        let used = name_len.checked_add(value).ok_or(Error::Length {
            name: name_len,
            value,
        })?;
        if used > fields.len() {
            return Err(Error::Payload {
                required: used,
                available: fields.len(),
            });
        }
        let (packed, rest) = fields.split_at(used);
        let (name, value) = packed.split_at(name_len);
        Ok((
            record::Entry::new(name, value).with_index(self.indexing),
            rest,
        ))
    }
}
