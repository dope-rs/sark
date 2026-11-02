use o3::buffer::write;
use sark_http::field::metadata::{
    block,
    block::buffer,
    huffman::{decoder, source},
    integer,
    map::record,
    strings,
};

use crate::{frame, qpack::decoder::Error, sources};

mod sink;

enum LiteralBytes<'a, S: sources::Source + ?Sized + 'a> {
    Borrowed(&'a [u8]),
    Split(record::Span<'a>),
    Cursor(sources::Span<'a, S::Chunks<'a>>),
}

pub(super) struct Literal<'a, S: sources::Source + ?Sized + 'a = [u8]> {
    bytes: LiteralBytes<'a, S>,
    encoding: strings::Encoding,
}

pub(super) enum Access<'a> {
    Raw(record::Span<'a>),
    Materialize,
}

pub(super) enum Raw<'a> {
    Plain(&'a [u8]),
    Huffman(source::Measured<'a>),
}

impl<'a> Literal<'a, [u8]> {
    pub(super) fn take<const BITS: u8>(input: &mut &'a [u8]) -> Result<Self, Error>
    where
        integer::PrefixedInt<BITS>: Prefix,
    {
        let mut tail = *input;
        let first = *tail.first().ok_or(Error::Incomplete {
            available: tail.len(),
        })?;
        let encoding = match first & <integer::PrefixedInt<BITS> as Prefix>::HUFFMAN_BIT {
            0 => strings::Encoding::Plain,
            _ => strings::Encoding::Huffman,
        };
        let size = tail.len();
        let len = integer::PrefixedInt::<BITS>::take(&mut tail)
            .map_err(|error| Error::integer::<BITS>(size, error))?;
        let raw = len.get();
        let (bytes, tail) = usize::try_from(raw)
            .ok()
            .and_then(|len| tail.split_at_checked(len))
            .ok_or(Error::LiteralLength {
                encoded: raw,
                available: tail.len(),
            })?;
        *input = tail;
        Ok(Self {
            bytes: LiteralBytes::Borrowed(bytes),
            encoding,
        })
    }
}

impl<'a> Raw<'a> {
    pub(super) fn try_new(
        bytes: &'a [u8],
        encoding: strings::Encoding,
    ) -> Result<Self, source::Overflow> {
        Ok(match encoding {
            strings::Encoding::Plain => Self::Plain(bytes),
            strings::Encoding::Huffman => Self::Huffman(source::Source::new(bytes).try_measure()?),
        })
    }

    pub(super) const fn encoded_len(&self) -> usize {
        match self {
            Self::Plain(bytes) => bytes.len(),
            Self::Huffman(measured) => measured.encoded_len(),
        }
    }

    pub(super) fn encode<const BITS: u8>(self, prefix: u8, out: &mut frame::Writer<'_>)
    where
        integer::PrefixedInt<BITS>: Prefix,
    {
        let length = integer::PrefixedInt::<BITS>::new(self.encoded_len() as u64);
        match self {
            Self::Huffman(measured) => {
                length.encode(
                    prefix | <integer::PrefixedInt<BITS> as Prefix>::HUFFMAN_BIT,
                    out,
                );
                measured.encode(out);
            }
            Self::Plain(bytes) => {
                length.encode(prefix, out);
                out.extend([bytes]);
            }
        }
    }
}

impl<'a, S: sources::Source + ?Sized + 'a> Literal<'a, S> {
    pub(super) const fn raw_span(bytes: record::Span<'a>) -> Self {
        Self {
            bytes: LiteralBytes::Split(bytes),
            encoding: strings::Encoding::Plain,
        }
    }

    pub(super) fn parse_cursor<const BITS: u8>(
        input: &mut sources::Cursor<'a, S::Chunks<'a>>,
    ) -> Result<Self, Error>
    where
        integer::PrefixedInt<BITS>: Prefix,
    {
        let before = input.remaining();
        let first = input
            .peek()
            .ok_or(Error::Incomplete { available: before })?;
        let encoding = match first & <integer::PrefixedInt<BITS> as Prefix>::HUFFMAN_BIT {
            0 => strings::Encoding::Plain,
            _ => strings::Encoding::Huffman,
        };
        let len = input
            .prefixed::<BITS>()
            .map_err(|error| Error::integer::<BITS>(before, error))?;
        let raw = len.get();
        if raw > usize::MAX as u64 {
            return Err(Error::LiteralLength {
                encoded: raw,
                available: input.remaining(),
            });
        }
        let len = raw as usize;
        let left = input.remaining();
        let bytes = input.take(len).ok_or(Error::LiteralLength {
            encoded: raw,
            available: left,
        })?;
        Ok(Self {
            bytes: LiteralBytes::Cursor(bytes),
            encoding,
        })
    }

    fn encoded_len(&self) -> usize {
        match &self.bytes {
            LiteralBytes::Borrowed(bytes) => bytes.len(),
            LiteralBytes::Split(bytes) => bytes.len(),
            LiteralBytes::Cursor(span) => span.len(),
        }
    }

    fn try_for_each(&self, mut feed: impl FnMut(&[u8]) -> Result<(), Error>) -> Result<(), Error> {
        match &self.bytes {
            LiteralBytes::Borrowed(bytes) => feed(bytes),
            LiteralBytes::Split(bytes) => {
                let (first, second) = bytes.parts();
                feed(first)?;
                feed(second)
            }
            LiteralBytes::Cursor(span) => {
                for chunk in span.chunks() {
                    feed(chunk)?;
                }
                Ok(())
            }
        }
    }

    pub(super) fn write_to<W>(&self, out: &mut W, max_len: usize) -> Result<usize, Error>
    where
        W: write::Sink,
        Error: From<W::Error>,
    {
        let mut string = strings::Cursor::new(self.encoded_len(), self.encoding);
        let mut limited = sink::Limited::new(out, max_len);
        self.try_for_each(|bytes| match string.feed_into(bytes, &mut limited) {
            Ok((progress, _)) => {
                if progress.consumed() != bytes.len() {
                    return Err(Error::LiteralDecodeInput {
                        expected: bytes.len(),
                        actual: progress.consumed(),
                    });
                }
                Ok(())
            }
            Err(failure) => match failure.cause {
                decoder::DecodeError::InvalidEncoding(_) => Err(Error::LiteralEncoding),
                decoder::DecodeError::Sink(sink::Error::Limit {
                    written,
                    additional: extra,
                    maximum,
                }) => Err(Error::LiteralCapacity {
                    written,
                    additional: extra,
                    maximum,
                }),
                decoder::DecodeError::Sink(sink::Error::Output(error)) => Err(error.into()),
            },
        })?;
        match string.finish() {
            Ok(len) => Ok(len),
            Err(decoder::Error) => Err(Error::LiteralEncoding),
        }
    }

    pub(super) fn access(&self) -> Access<'a> {
        match self.encoding {
            strings::Encoding::Huffman => return Access::Materialize,
            strings::Encoding::Plain => {}
        }
        match &self.bytes {
            LiteralBytes::Borrowed(bytes) => Access::Raw(record::Span::from_slice(bytes)),
            LiteralBytes::Split(bytes) => Access::Raw(*bytes),
            LiteralBytes::Cursor(span) => match span.form() {
                sources::Form::Contiguous(bytes) => Access::Raw(record::Span::from_slice(bytes)),
                sources::Form::Segmented => Access::Materialize,
            },
        }
    }

    pub(super) fn decode_into<'scratch>(
        &self,
        scratch: &'scratch mut buffer::Values,
        max_len: usize,
    ) -> Result<block::Valued<'scratch>, Error> {
        scratch.clear();
        self.write_to(&mut scratch.writer(None), max_len)?;
        Ok(scratch.valued())
    }

    pub(super) fn decode_name<'scratch>(
        &self,
        scratch: &'scratch mut buffer::Names,
        max_len: usize,
    ) -> Result<block::Named<'scratch>, Error> {
        scratch.clear();
        self.write_to(&mut scratch.writer(None), max_len)?;
        Ok(scratch.named())
    }

    pub(super) fn validate(&self, max_len: usize) -> Result<(usize, block::Validation), Error> {
        let mut scan = block::Value::new();
        let len = self.write_to(&mut scan, max_len)?;
        Ok((len, scan.finish()))
    }
}

pub(super) trait Prefix: integer::Width {
    const HUFFMAN_BIT: u8;
}

impl Prefix for integer::PrefixedInt<3> {
    const HUFFMAN_BIT: u8 = 1 << 3;
}

impl Prefix for integer::PrefixedInt<5> {
    const HUFFMAN_BIT: u8 = 1 << 5;
}

impl Prefix for integer::PrefixedInt<7> {
    const HUFFMAN_BIT: u8 = 1 << 7;
}
