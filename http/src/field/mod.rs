#[doc(hidden)]
pub mod base64;
pub mod date;
pub mod head;
pub mod metadata;
pub mod raw;
pub mod scan;
pub mod storage;
pub mod text;

use std::{convert::Infallible, iter::Copied, slice};

pub use storage::{Lower, Name, Value};

/// Requires successful field admission at compile time.
/// Invalid input fails during constant evaluation.
#[macro_export]
macro_rules! field_literal {
    ($value:expr $(,)?) => {
        const {
            match $value {
                ::core::result::Result::Ok(value) => value,
                ::core::result::Result::Err(_) => ::core::panic!("literal"),
            }
        }
    };
}

use metadata::map::record;
use storage::encoded;

pub use crate::field_literal as literal;
use crate::payload::response::status;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Empty,
    InvalidByte { index: usize, byte: u8 },
}

/// Compression permission retained with one HTTP field line.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Indexing {
    #[default]
    Allowed,
    Never,
}

impl Indexing {
    pub(in crate::field) const fn from_byte(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Allowed),
            1 => Some(Self::Never),
            _ => None,
        }
    }

    pub(in crate::field) const fn from_never(never: bool) -> Self {
        if never { Self::Never } else { Self::Allowed }
    }
}

/// Writes one generated field value into its protocol encoder.
pub trait Writer {
    fn write_byte(&mut self, byte: u8);

    fn write_slice(&mut self, bytes: &[u8]);
}

/// A generated field value with an exact encoded length.
pub trait Encode: encoded::Encoded {
    fn encoded_len(&self) -> usize;

    /// Stops at the first rejected fragment and preserves its exact error.
    fn try_write<E>(self, emit: impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E>;

    fn encode(self, out: &mut impl Writer)
    where
        Self: Sized,
    {
        match self.try_write(|bytes| {
            out.write_slice(bytes);
            Ok::<_, Infallible>(())
        }) {
            Ok(()) => {}
            Err(never) => match never {},
        }
    }
}

/// Receives one synchronous semantic field plan without staging storage.
pub trait Sink<'field> {
    fn entry(&mut self, field: Entry<'field>);

    /// Emits a preencoded group while retaining each field's indexing policy.
    fn group(&mut self, group: &Group<'field>) {
        for entry in group.entries() {
            self.entry(*entry);
        }
    }

    fn field(&mut self, name: Lower<&'field [u8]>, value: Value<&'field [u8]>) {
        self.entry(Entry::new(name, value));
    }

    fn encoded_with<E: Encode>(&mut self, name: Lower<&'field [u8]>, value: E, indexing: Indexing);

    /// Emits a newly generated value with indexing permitted.
    fn generated<E: Encode>(&mut self, name: Lower<&'field [u8]>, value: E) {
        self.encoded_with(name, value, Indexing::Allowed);
    }
}

/// Borrows and synchronously emits one semantic field plan.
pub trait Source {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>);
}

/// Borrows the exact bytes of a typed outgoing field value.
pub trait AsValue {
    fn as_value(&self) -> Value<&[u8]>;
}

impl<T: storage::Bytes> AsValue for Value<T> {
    fn as_value(&self) -> Value<&[u8]> {
        Value(self.0.as_bytes())
    }
}

impl<T: storage::Bytes> Encode for Value<T> {
    fn encoded_len(&self) -> usize {
        self.as_bytes().len()
    }

    fn try_write<E>(self, mut emit: impl FnMut(&[u8]) -> Result<(), E>) -> Result<(), E> {
        let Self(owner) = self;
        emit(owner.as_bytes())
    }
}

impl<W: Writer + ?Sized> Writer for &mut W {
    fn write_byte(&mut self, byte: u8) {
        (**self).write_byte(byte);
    }

    fn write_slice(&mut self, bytes: &[u8]) {
        (**self).write_slice(bytes);
    }
}

impl<S: Source + ?Sized> Source for &S {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        (**self).emit(sink);
    }
}

impl Source for [Entry<'_>] {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        for field in self {
            sink.entry(*field);
        }
    }
}

impl<const N: usize> Source for [Entry<'_>; N] {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        self.as_slice().emit(sink);
    }
}

impl Source for Vec<Entry<'_>> {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        self.as_slice().emit(sink);
    }
}

impl<'entry, 'value> Source for Copied<slice::Iter<'entry, Entry<'value>>>
where
    'value: 'entry,
{
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        for field in self.clone() {
            sink.entry(field);
        }
    }
}

impl<'entry, 'value> Source for slice::Iter<'entry, Entry<'value>>
where
    'value: 'entry,
{
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        for field in self.clone() {
            sink.entry(*field);
        }
    }
}

impl Error {
    pub fn trim_ows(mut value: &[u8]) -> &[u8] {
        while matches!(value.first(), Some(b' ' | b'\t')) {
            value = &value[1..];
        }
        while matches!(value.last(), Some(b' ' | b'\t')) {
            value = &value[..value.len() - 1];
        }
        value
    }

    pub const fn name(name: &[u8]) -> Result<(), Self> {
        if name.is_empty() {
            return Err(Self::Empty);
        }
        let mut index = 0;
        while index < name.len() {
            let byte = name[index];
            if !Self::is_name_byte(byte) {
                return Err(Self::InvalidByte { index, byte });
            }
            index += 1;
        }
        Ok(())
    }

    pub const fn is_name_byte(byte: u8) -> bool {
        byte.is_ascii_alphanumeric()
            | (byte == b'!')
            | (byte.wrapping_sub(b'#') <= b'\'' - b'#')
            | (byte.wrapping_sub(b'*') <= b'+' - b'*')
            | (byte.wrapping_sub(b'-') <= b'.' - b'-')
            | (byte.wrapping_sub(b'^') <= b'`' - b'^')
            | (byte == b'|')
            | (byte == b'~')
    }

    pub const fn value(value: &[u8]) -> Result<(), Self> {
        let mut index = 0;
        while index < value.len() {
            let byte = value[index];
            if !Self::is_text(byte) {
                return Err(Self::InvalidByte { index, byte });
            }
            index += 1;
        }
        Ok(())
    }

    pub const fn is_text(byte: u8) -> bool {
        ((byte >= 0x20) & (byte != 0x7f)) | (byte == b'\t')
    }
}

/// HTTP/2 and HTTP/3 response fields prefixed by `:status`.
pub struct Response<F> {
    value: Value<[u8; 3]>,
    fields: F,
}

impl<F> Response<F> {
    pub const fn new(status: status::Code, fields: F) -> Self {
        Self {
            value: Value(status.as_bytes()),
            fields,
        }
    }
}

impl<F: Source> Source for Response<F> {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        sink.entry(Entry::pseudo(Pseudo::Status, self.value.borrow()));
        self.fields.emit(sink);
    }
}

/// One admitted HTTP field whose borrowed bytes cannot change during emission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Entry<'a>(pub(in crate::field) record::Entry<'a>);

/// A pseudo-field supported by HTTP/2 and HTTP/3.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pseudo {
    Method,
    Scheme,
    Authority,
    Path,
    Protocol,
    Status,
}

impl<'a> Entry<'a> {
    pub const fn new(name: Lower<&'a [u8]>, value: Value<&'a [u8]>) -> Self {
        Self(record::Entry::new(name.0, value.0))
    }

    pub const fn pseudo(name: Pseudo, value: Value<&'a [u8]>) -> Self {
        let name = match name {
            Pseudo::Method => b":method".as_slice(),
            Pseudo::Scheme => b":scheme".as_slice(),
            Pseudo::Authority => b":authority".as_slice(),
            Pseudo::Path => b":path".as_slice(),
            Pseudo::Protocol => b":protocol".as_slice(),
            Pseudo::Status => b":status".as_slice(),
        };
        Self(record::Entry::new(name, value.0))
    }

    pub const fn with_index(self, indexing: Indexing) -> Self {
        Self(self.0.with_index(indexing))
    }

    pub const fn name(self) -> &'a [u8] {
        self.0.name
    }

    pub const fn value(self) -> Value<&'a [u8]> {
        Value(self.0.value)
    }

    pub const fn indexing(self) -> Indexing {
        self.0.indexing
    }

    pub const fn as_record(&self) -> record::Entry<'a> {
        self.0
    }
}

/// Immutable fields paired with their exact name/value line encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Group<'a> {
    entries: &'a [Entry<'a>],
    wire: &'a [u8],
}

/// The first byte where a field sequence and its wire encoding differ.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mismatch {
    pub offset: usize,
    pub expected: Option<u8>,
    pub actual: Option<u8>,
}

struct Cursor<'a> {
    wire: &'a [u8],
    offset: usize,
}

impl<'a> Group<'a> {
    pub const fn try_new(entries: &'a [Entry<'a>], wire: &'a [u8]) -> Result<Self, Mismatch> {
        let mut cursor = Cursor { wire, offset: 0 };
        let mut index = 0;
        while index < entries.len() {
            let entry = entries[index];
            let parts = [entry.name(), b": ", entry.value().get(), b"\r\n"];
            let mut part = 0;
            while part < parts.len() {
                if let Err(error) = cursor.check(parts[part]) {
                    return Err(error);
                }
                part += 1;
            }
            index += 1;
        }
        if let Some((&actual, _)) = cursor.wire.split_first() {
            return Err(Mismatch {
                offset: cursor.offset,
                expected: None,
                actual: Some(actual),
            });
        }
        Ok(Self { entries, wire })
    }

    pub const fn entries(&self) -> &'a [Entry<'a>] {
        self.entries
    }

    pub const fn wire(&self) -> &'a [u8] {
        self.wire
    }
}

impl Source for Group<'_> {
    fn emit<'field>(&'field self, sink: &mut impl Sink<'field>) {
        sink.group(self);
    }
}

impl Cursor<'_> {
    const fn check(&mut self, expected: &[u8]) -> Result<(), Mismatch> {
        let mut index = 0;
        while index < expected.len() {
            let byte = expected[index];
            let (actual, rest) = match self.wire.split_first() {
                Some((&actual, rest)) => (Some(actual), rest),
                None => (None, self.wire),
            };
            if !matches!(actual, Some(actual) if actual == byte) {
                return Err(Mismatch {
                    offset: self.offset,
                    expected: Some(byte),
                    actual,
                });
            }
            self.wire = rest;
            self.offset += 1;
            index += 1;
        }
        Ok(())
    }
}
