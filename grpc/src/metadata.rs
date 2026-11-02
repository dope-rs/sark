use field::metadata::{
    map,
    map::record::{Entry, Span},
};
use sark_http::field;

use crate::protocol::section::{self, Valid};

fn values<'a, I>(entries: I, name: &'a [u8]) -> impl Iterator<Item = &'a [u8]> + 'a
where
    I: Iterator<Item = Entry<'a>> + 'a,
{
    entries.filter_map(move |entry| (entry.name == name).then_some(entry.value))
}

/// Application-built gRPC metadata in one contiguous packed allocation.
/// Protocol and pseudo fields are rejected at insertion, so repeated sends
/// do not rescan the block before encoding it.
#[derive(Default, PartialEq, Eq)]
pub struct Metadata {
    fields: map::Fields<Vec<u8>>,
}

/// Driver-local decoded metadata owner with a niche-backed empty state.
///
#[derive(Default, PartialEq, Eq)]
pub struct Decoded {
    fields: Option<Valid>,
}

/// Borrowed projection over a decoded field owner.
///
/// The view contains no owner, allocation, or reference count. Its lifetime
/// is exactly the lifetime of the request or response owner that produced it.
///
#[derive(Clone, Copy, Debug)]
pub struct View<'a> {
    fields: Option<&'a Valid>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    EmptyName,
    InvalidName { index: usize, byte: u8 },
    ReservedName,
    EmptyValue,
    InvalidValue { index: usize, byte: u8 },
    Pack(map::Error),
}

impl Error {
    pub(crate) fn value(error: field::Error) -> Self {
        match error {
            field::Error::Empty => Self::EmptyValue,
            field::Error::InvalidByte { index, byte } => Self::InvalidValue { index, byte },
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Encoding {
    Ascii,
    Binary,
}

const _: () = assert!(core::mem::size_of::<Encoding>() == core::mem::size_of::<bool>());
const _: () = assert!(core::mem::align_of::<Encoding>() == core::mem::align_of::<bool>());

impl Metadata {
    pub const fn new() -> Self {
        Self {
            fields: map::Fields::new(),
        }
    }

    pub fn try_with(capacity: usize) -> Result<Self, Error> {
        Ok(Self {
            fields: map::Fields::try_with(capacity).map_err(Error::Pack)?,
        })
    }

    pub fn push(&mut self, name: &[u8], value: &[u8]) -> Result<(), Error> {
        self.push_entry(Entry::new(name, value))
    }

    pub fn push_entry(&mut self, field: Entry<'_>) -> Result<(), Error> {
        match Self::check_name(field.name)? {
            Encoding::Ascii => Self::check_ascii(field.value)?,
            Encoding::Binary => {}
        }
        self.fields.push_entry(field).map_err(Error::Pack)
    }

    pub fn entries(&self) -> impl Iterator<Item = Entry<'_>> {
        self.fields.iter()
    }

    pub fn values<'a>(&'a self, name: &'a [u8]) -> impl Iterator<Item = &'a [u8]> + 'a {
        values(self.entries(), name)
    }

    pub fn len(&self) -> usize {
        self.entries().count()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.as_bytes().is_empty()
    }

    pub(crate) fn check_name(name: &[u8]) -> Result<Encoding, Error> {
        Self::check_span(Span::from_slice(name))
    }

    pub(crate) fn check_span(name: Span<'_>) -> Result<Encoding, Error> {
        if name.is_empty() {
            return Err(Error::EmptyName);
        }
        for (index, byte) in name.iter().copied().enumerate() {
            if !(byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'_' | b'-' | b'.'))
            {
                return Err(Error::InvalidName { index, byte });
            }
        }
        if Self::is_reserved(name) {
            return Err(Error::ReservedName);
        }
        if name.ends_with(b"-bin") {
            if name.len() == 4 {
                return Err(Error::InvalidName {
                    index: 0,
                    byte: b'-',
                });
            }
            Ok(Encoding::Binary)
        } else {
            Ok(Encoding::Ascii)
        }
    }

    pub(crate) fn is_visible(name: &[u8]) -> bool {
        !name.starts_with(b":") && !Self::is_reserved(Span::from_slice(name))
    }

    pub(crate) fn is_reserved(name: Span<'_>) -> bool {
        name.starts_with(b"grpc-")
            || name.is_equal(b"connection")
            || name.is_equal(b"content-length")
            || name.is_equal(b"content-type")
            || name.is_equal(b"keep-alive")
            || name.is_equal(b"proxy-connection")
            || name.is_equal(b"te")
            || name.is_equal(b"transfer-encoding")
            || name.is_equal(b"upgrade")
            || name.is_equal(b"user-agent")
    }

    pub(crate) fn check_ascii(value: &[u8]) -> Result<(), Error> {
        if value.is_empty() {
            return Err(Error::EmptyValue);
        }
        for (index, &byte) in value.iter().enumerate() {
            if !matches!(byte, 0x20..=0x7e) {
                return Err(Error::InvalidValue { index, byte });
            }
        }
        Ok(())
    }
}

impl Decoded {
    pub const fn new() -> Self {
        Self { fields: None }
    }

    pub(crate) fn from(fields: Valid) -> Self {
        Self {
            fields: Some(fields),
        }
    }

    pub fn view(&self) -> View<'_> {
        View {
            fields: self.fields.as_ref(),
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = Entry<'_>> {
        self.view().entries()
    }

    pub fn values<'a>(&'a self, name: &'a [u8]) -> impl Iterator<Item = &'a [u8]> + 'a {
        values(self.entries(), name)
    }

    pub fn len(&self) -> usize {
        self.entries().count()
    }

    pub fn is_empty(&self) -> bool {
        self.entries().next().is_none()
    }
}

impl<'a> View<'a> {
    pub(crate) const fn empty() -> Self {
        Self { fields: None }
    }

    pub(crate) const fn from(fields: &'a Valid) -> Self {
        Self {
            fields: Some(fields),
        }
    }

    pub fn entries(self) -> impl Iterator<Item = Entry<'a>> + 'a {
        self.fields
            .into_iter()
            .flat_map(section::Iter::from)
            .filter(|field| Metadata::is_visible(field.name))
    }

    pub fn values(self, name: &'a [u8]) -> impl Iterator<Item = &'a [u8]> + 'a {
        values(self.entries(), name)
    }

    pub fn len(self) -> usize {
        self.entries().count()
    }

    pub fn is_empty(self) -> bool {
        self.entries().next().is_none()
    }
}

const _: () = {
    use std::mem;

    assert!(mem::size_of::<Metadata>() == mem::size_of::<Vec<u8>>());
    assert!(
        mem::size_of::<Decoded>() == mem::size_of::<Valid>(),
        "empty decoded metadata must use the field owner's existing niche"
    );
    assert!(
        mem::size_of::<View<'static>>() == mem::size_of::<usize>(),
        "a decoded metadata projection must be exactly one nullable pointer"
    );
};
