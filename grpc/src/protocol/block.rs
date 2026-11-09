use o3::buffer::write::Sink as _;
use sark_http::{
    field,
    field::metadata::{
        Metadata, map,
        map::record::Entry,
        packed::{Iter, prefix::Prefix},
    },
};

use crate::{
    Reason, metadata,
    protocol::{request, response, sinks, trailer},
};

#[derive(PartialEq, Eq)]
pub struct Block {
    fields: map::Fields,
}

impl Block {
    pub const fn new() -> Self {
        Self {
            fields: map::Fields::new(),
        }
    }

    pub(super) fn add_capacity(
        capacity: usize,
        name: &[u8],
        vlen: usize,
    ) -> Result<usize, crate::Status> {
        if name.len() > u32::MAX as usize {
            return Err(crate::Status::length(Reason::HeaderBounds, name.len()));
        }
        if vlen > u32::MAX as usize {
            return Err(crate::Status::length(Reason::HeaderBounds, vlen));
        }
        capacity
            .checked_add(Prefix::LEN + name.len() + vlen)
            .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::HeaderCapacity))
    }

    pub fn push(&mut self, name: &[u8], value: &[u8]) -> Result<(), crate::Status> {
        self.push_entry(Entry::new(name, value))
    }

    pub fn push_entry(&mut self, field: Entry<'_>) -> Result<(), crate::Status> {
        let value = field::Value::try_bytes(field.value).map_err(metadata::Error::value)?;
        let pseudo = match field.name {
            b":method" => Some(field::Pseudo::Method),
            b":scheme" => Some(field::Pseudo::Scheme),
            b":authority" => Some(field::Pseudo::Authority),
            b":path" => Some(field::Pseudo::Path),
            b":protocol" => Some(field::Pseudo::Protocol),
            b":status" => Some(field::Pseudo::Status),
            _ => None,
        };
        let admitted = match pseudo {
            Some(pseudo) => field::Entry::pseudo(pseudo, value),
            None => {
                let name = field::Lower::try_bytes(field.name).map_err(|error| match error {
                    field::Error::Empty => metadata::Error::EmptyName,
                    field::Error::InvalidByte { index, byte } => {
                        metadata::Error::InvalidName { index, byte }
                    }
                })?;
                field::Entry::new(name, value)
            }
        };
        self.push_field(admitted.with_index(field.indexing))
    }

    pub(super) fn push_field(&mut self, field: field::Entry<'_>) -> Result<(), crate::Status> {
        Ok(self.fields.push_entry(field.as_record())?)
    }

    pub(super) fn push_encoded<E: field::Encode>(
        &mut self,
        name: field::Lower<&[u8]>,
        value: E,
        indexing: field::Indexing,
    ) -> Result<(), crate::Status> {
        let len = value.encoded_len();
        Ok(self.fields.push_with(name.get(), len, indexing, |out| {
            value.try_write(|bytes| out.write_slice(bytes))
        })?)
    }

    pub fn iter(&self) -> Iter<'_> {
        self.fields.iter()
    }

    pub fn request(
        path: &[u8],
        host: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<Self, crate::Status> {
        Self::materialize(request::Fields::new(path, host, metadata)?)
    }

    pub fn response(metadata: &metadata::Metadata) -> Result<Self, crate::Status> {
        Self::materialize(response::Fields::new(metadata))
    }

    pub fn trailers(
        status: &crate::Status,
        metadata: &metadata::Metadata,
    ) -> Result<Self, crate::Status> {
        Self::materialize(trailer::Fields::new(status, metadata)?)
    }

    fn materialize<S: field::Source>(source: S) -> Result<Self, crate::Status> {
        let mut count = sinks::Count { capacity: Ok(0) };
        source.emit(&mut count);
        let mut block = Self {
            fields: map::Fields::try_with(count.capacity?)?,
        };
        let mut sink = sinks::Block {
            block: &mut block,
            error: None,
        };
        source.emit(&mut sink);
        match sink.error {
            Some(error) => Err(error),
            None => Ok(block),
        }
    }
}

impl Metadata for Block {
    type Iter<'a> = Iter<'a>;

    fn fields(&self) -> Self::Iter<'_> {
        self.fields.iter()
    }
}

impl field::Source for Block {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        use sark_http::field::raw::Admit as _;

        for entry in self.iter() {
            // SAFETY: Block admits lowercase ordinary names or supported pseudo-names and HTTP values before packing this immutable owner.
            sink.entry(unsafe { field::Entry::from_raw(entry) });
        }
    }
}

impl Default for Block {
    fn default() -> Self {
        Self::new()
    }
}

const _: () = assert!(core::mem::size_of::<Block>() == core::mem::size_of::<Vec<u8>>());
