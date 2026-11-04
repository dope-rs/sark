//! JSON sizing and encoding machinery.
use std::{mem, ops::ControlFlow};

use escape::Split;
use exact::Overflow;
use o3::{
    alloc,
    buffer::write::{self, SpareExt as _, consume, spans},
    mem::Rejected,
};
use ryu;
use sark_http::{
    field::{Value, literal},
    payload::response::{
        Encode, bounded,
        dispatch::IntoOutput,
        exact,
        headers::{Entry, Name, head::Template, set::Set},
        status::Code,
    },
};

use crate::{Backing, Plain, Raw, Text, TextValue};

mod escape;

#[repr(transparent)]
#[must_use = "continue the writer chain"]
pub struct Writer<'a, W: write::Sink> {
    inner: &'a mut W,
}

#[repr(C)]
#[must_use = "preserve the writer and its exact failure"]
pub struct Failure<'a, W: write::Sink> {
    writer: Writer<'a, W>,
    error: W::Error,
}

impl<'a, W: write::Sink> Writer<'a, W> {
    pub fn new(inner: &'a mut W) -> Self {
        Self { inner }
    }

    /// Writes bytes verbatim without JSON validation, quoting, or escaping.
    pub fn put(self, src: &[u8]) -> ControlFlow<Failure<'a, W>, Self> {
        match self.inner.write_slice(src) {
            Ok(()) => ControlFlow::Continue(self),
            Err(error) => ControlFlow::Break(Failure {
                writer: self,
                error,
            }),
        }
    }

    pub fn put_str<T: TextValue + ?Sized>(self, value: &T) -> ControlFlow<Failure<'a, W>, Self> {
        let mut value = value.as_bytes();
        match Split::escape(&mut value) {
            Some(split) => self.put(b"\"")?.put_rest(value, Some(split)),
            None => self.put_plain(&Plain::admitted(value)),
        }
    }

    fn put_rest<'s>(
        mut self,
        mut value: &'s [u8],
        mut next: Option<Split<'s>>,
    ) -> ControlFlow<Failure<'a, W>, Self> {
        while let Some(split) = next {
            if !split.prefix.is_empty() {
                self = self.put(split.prefix)?;
            }
            self = self.put(split.token.bytes())?;
            next = Split::escape(&mut value);
        }
        if !value.is_empty() {
            self = self.put(value)?;
        }
        self.put(b"\"")
    }

    pub fn put_plain<T: Backing>(self, value: &Plain<T>) -> ControlFlow<Failure<'a, W>, Self> {
        match self
            .inner
            .write_slices([b"\"".as_slice(), value.as_slice(), b"\"".as_slice()])
        {
            Ok(()) => ControlFlow::Continue(self),
            Err(error) => ControlFlow::Break(Failure {
                writer: self,
                error,
            }),
        }
    }

    pub fn put_u64(self, value: u64) -> ControlFlow<Failure<'a, W>, Self> {
        let mut digits;
        let bytes = if value < 100 {
            let pair = &Digits::PAIRS[value as usize];
            if value < 10 { &pair[1..] } else { pair }
        } else {
            digits = Digits { bytes: [0; 20] };
            digits.write(value)
        };
        self.put(bytes)
    }

    pub fn put_i64(mut self, value: i64) -> ControlFlow<Failure<'a, W>, Self> {
        if value < 0 {
            self = self.put(b"-")?;
        }
        self.put_u64(value.unsigned_abs())
    }

    /// Writes a JSON number, or `null` for NaN and either infinity.
    pub fn put_f64(self, value: f64) -> ControlFlow<Failure<'a, W>, Self> {
        if value.is_finite() {
            self.put(ryu::Buffer::new().format_finite(value).as_bytes())
        } else {
            self.put(b"null")
        }
    }
}

impl<'a, W: write::Sink> Failure<'a, W> {
    pub fn into_error(self) -> W::Error {
        self.error
    }
}

const _: () = {
    assert!(usize::BITS <= u64::BITS);
    assert!(mem::size_of::<Writer<'static, write::Slice<'static>>>() == mem::size_of::<usize>());
    assert!(
        mem::size_of::<Option<Writer<'static, write::Slice<'static>>>>() == mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Failure<'static, write::Slice<'static>>>() == mem::size_of::<usize>() * 3
    );
    assert!(
        mem::size_of::<
            ControlFlow<
                Failure<'static, write::Slice<'static>>,
                Writer<'static, write::Slice<'static>>,
            >,
        >() == mem::size_of::<usize>() * 3
    );
};

struct Digits {
    bytes: [u8; 20],
}

impl Digits {
    const PAIRS: [[u8; 2]; 100] = {
        let mut pairs = [[0; 2]; 100];
        let mut value = 0;
        while value < pairs.len() {
            pairs[value] = [b'0' + (value / 10) as u8, b'0' + (value % 10) as u8];
            value += 1;
        }
        pairs
    };

    fn write(&mut self, mut value: u64) -> &[u8] {
        let bytes = &mut self.bytes;
        let mut start = bytes.len();
        while value >= 100 {
            start -= 2;
            bytes[start..start + 2].copy_from_slice(&Self::PAIRS[(value % 100) as usize]);
            value /= 100;
        }
        start -= 2;
        bytes[start..start + 2].copy_from_slice(&Self::PAIRS[value as usize]);
        &bytes[start + usize::from(value < 10)..]
    }
}

pub trait Size {
    fn json_size(&self) -> usize;
}

/// Sizing that can reject an unrepresentable encoded length.
pub trait TrySize {
    fn try_size(&self) -> Result<usize, Overflow>;
}

/// A checked upper bound that can be obtained without exact sizing.
pub trait TryMaxSize {
    type Error;

    fn try_max_size(&self) -> Result<usize, Self::Error>;
}

impl<T: Size + ?Sized> TrySize for T {
    fn try_size(&self) -> Result<usize, Overflow> {
        Ok(self.json_size())
    }
}

/// Generated sequence code keeps each value alive while borrowing its encoder.
#[doc(hidden)]
#[derive(Debug)]
pub struct Item<T>(pub T);

impl<T: Copy + IntoIterator> IntoIterator for &Item<T> {
    type Item = T::Item;
    type IntoIter = T::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIterator::into_iter(self.0)
    }
}

impl<'inner, T: ?Sized> IntoIterator for Item<&&'inner T>
where
    &'inner T: IntoIterator,
{
    type Item = <&'inner T as IntoIterator>::Item;
    type IntoIter = <&'inner T as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIterator::into_iter(*self.0)
    }
}

#[doc(hidden)]
pub trait Nested<'a> {
    type Value: TrySize + ?Sized + 'a;

    fn into_ref(self) -> &'a Self::Value;
}

impl<'a, T: TrySize + ?Sized> Nested<'a> for Item<&'a T> {
    type Value = T;

    fn into_ref(self) -> &'a Self::Value {
        self.0
    }
}

impl<'a, T: TrySize> Nested<'a> for &'a Item<T> {
    type Value = T;

    fn into_ref(self) -> &'a Self::Value {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeError {
    Size(Overflow),
    Allocate(alloc::Error),
    Capacity(consume::Error),
    Length(spans::Gap),
}

/// One nonempty sequence of object members, without enclosing braces.
/// Nonempty generated objects using decoding or `encode` implement this contract.
/// `FIRST` omits the leading comma; the size always excludes that comma and both braces.
pub trait Members {
    fn try_fields(&self) -> Result<usize, Overflow>;

    fn try_members<'a, W: write::Sink, const FIRST: bool>(
        &self,
        writer: Writer<'a, W>,
    ) -> ControlFlow<Failure<'a, W>, Writer<'a, W>>;
}

impl<T: Members + ?Sized> Members for &T {
    fn try_fields(&self) -> Result<usize, Overflow> {
        T::try_fields(*self)
    }

    fn try_members<'a, W: write::Sink, const FIRST: bool>(
        &self,
        writer: Writer<'a, W>,
    ) -> ControlFlow<Failure<'a, W>, Writer<'a, W>> {
        T::try_members::<W, FIRST>(*self, writer)
    }
}

impl Size for u64 {
    fn json_size(&self) -> usize {
        self.checked_ilog10().unwrap_or(0) as usize + 1
    }
}

impl Size for usize {
    fn json_size(&self) -> usize {
        (*self as u64).json_size()
    }
}

impl Size for i64 {
    fn json_size(&self) -> usize {
        self.unsigned_abs().json_size() + usize::from(*self < 0)
    }
}

impl Size for f64 {
    fn json_size(&self) -> usize {
        if self.is_finite() {
            ryu::Buffer::new().format_finite(*self).len()
        } else {
            4
        }
    }
}

impl TrySize for [u8] {
    fn try_size(&self) -> Result<usize, Overflow> {
        let mut value = self;
        let mut len = Overflow::try_add(2, value.len())?;
        while let Some(split) = Split::escape(&mut value) {
            len = Overflow::try_add(len, split.token.overhead())?;
        }
        Ok(len)
    }
}

impl<T: Backing> TrySize for Text<T> {
    fn try_size(&self) -> Result<usize, Overflow> {
        self.as_slice().try_size()
    }
}

impl<T: Backing> TrySize for Plain<T> {
    fn try_size(&self) -> Result<usize, Overflow> {
        Overflow::try_add(2, self.len())
    }
}

impl<T: Backing> TrySize for Raw<T> {
    fn try_size(&self) -> Result<usize, Overflow> {
        Ok(self.len())
    }
}

impl TrySize for str {
    fn try_size(&self) -> Result<usize, Overflow> {
        self.as_bytes().try_size()
    }
}

impl TrySize for String {
    fn try_size(&self) -> Result<usize, Overflow> {
        self.as_str().try_size()
    }
}

pub trait TryEncode: TrySize + Sized {
    fn try_write<'a, W: write::Sink>(
        &self,
        writer: Writer<'a, W>,
    ) -> ControlFlow<Failure<'a, W>, Writer<'a, W>>;

    /// Admits and writes one exact suffix, preserving the original prefix on failure.
    fn try_append(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        let expected = self.try_size().map_err(EncodeError::Size)?;
        consume::Error::check(expected, u32::MAX as usize).map_err(EncodeError::Capacity)?;
        let mut staged =
            write::Staged::try_grow(out, expected as u32).map_err(EncodeError::Allocate)?;
        let mut spare = staged.spare();
        if let ControlFlow::Break(failure) = self.try_write(Writer::new(&mut spare)) {
            return Err(EncodeError::Capacity(failure.into_error()));
        }
        spare.try_complete().map_err(EncodeError::Length)?;
        staged.commit();
        Ok(())
    }

    fn try_encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut out = Vec::new();
        self.try_append(&mut out)?;
        Ok(out)
    }
}

impl TryEncode for u64 {
    fn try_write<'a, W: write::Sink>(
        &self,
        writer: Writer<'a, W>,
    ) -> ControlFlow<Failure<'a, W>, Writer<'a, W>> {
        writer.put_u64(*self)
    }
}

impl TryEncode for usize {
    fn try_write<'a, W: write::Sink>(
        &self,
        writer: Writer<'a, W>,
    ) -> ControlFlow<Failure<'a, W>, Writer<'a, W>> {
        (*self as u64).try_write(writer)
    }
}

pub struct Body<T>(T);

/// A checked JSON response retaining only its bound and original encoder owner.
#[repr(transparent)]
pub struct Response<T>(bounded::Plan<Body<T>>);

static HEAD: Template = Template::new(&[Entry::new(
    literal!(Name::try_bytes(b"content-type")),
    literal!(Value::try_bytes(b"application/json")),
)]);

impl<T> Body<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T: TryEncode + TryMaxSize> Body<T> {
    /// Prepares an OK JSON response, checking its upper bound once before emission.
    /// The static JSON header imposes no lifetime requirement on the body.
    pub fn try_response(self) -> Result<Response<T>, Rejected<Self, T::Error>> {
        Ok(Response(bounded::Plan::try_new(self)?))
    }
}

impl<'req, 'd, T: TryEncode + TryMaxSize> IntoOutput<'req, 'd> for Response<T> {
    type Output = bounded::Output<'req, bounded::Plan<Body<T>>, 0>;

    fn into_output(self) -> Self::Output {
        bounded::Output::new(Code::OK, &HEAD, Set::new(), self.0)
    }
}

impl<T: TryEncode> exact::TryBody for Body<T> {
    fn try_len(&self) -> Result<usize, Overflow> {
        self.0.try_size()
    }
}

impl<T: TryEncode> Encode for Body<T> {
    fn encode<W>(self, out: &mut W) -> Result<(), consume::Error>
    where
        W: write::Sink<Error = consume::Error>,
    {
        let ControlFlow::Break(failure) = self.0.try_write(Writer::new(out)) else {
            return Ok(());
        };
        Err(failure.into_error())
    }
}

impl<T: TryEncode + TryMaxSize> bounded::TryBody for Body<T> {
    type Error = T::Error;

    fn try_max_len(&self) -> Result<usize, Self::Error> {
        self.0.try_max_size()
    }
}
