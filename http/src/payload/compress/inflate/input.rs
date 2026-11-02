use std::{borrow::BorrowMut, marker::PhantomData, num::NonZero, slice};

use miniz_oxide::inflate::{
    TINFLStatus,
    core::{DecompressorOxide, decompress, inflate_flags},
};

use crate::{
    field::scan,
    payload::{
        compress::inflate::{Checked, Count, Error, HeaderFault, Part, checked::STEP},
        crc32::Hasher,
    },
};

const FLAG_HEADER_CRC: u8 = 0x02;
const FLAG_EXTRA: u8 = 0x04;
const FLAG_NAME: u8 = 0x08;
const FLAG_COMMENT: u8 = 0x10;
const FLAG_RESERVED: u8 = 0xe0;

pub(super) struct Chunk<'a, S> {
    source: S,
    len: NonZero<usize>,
    bytes: PhantomData<&'a [u8]>,
}

impl<'a, S: BorrowMut<Cursor<'a>>> Chunk<'a, S> {
    pub(super) fn next<const CHECK: bool, E>(
        mut source: S,
        segments: &mut impl Iterator<Item = &'a [u8]>,
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Step<'a, S>, Checked<Error, E>> {
        let cursor = source.borrow_mut();
        let len = loop {
            if let Some(len) = NonZero::new(cursor.current.len()) {
                break len;
            }
            if CHECK {
                check().map_err(Checked::Check)?;
            }
            cursor.current = match segments.next() {
                Some(current) => current,
                None => return Ok(Step::End(End(cursor.consumed))),
            };
        };
        let len = if CHECK {
            len.min(NonZero::<usize>::MIN.saturating_add(STEP - 1))
        } else {
            len
        };
        Ok(Step::Chunk(Self {
            source,
            len,
            bytes: PhantomData,
        }))
    }

    fn bytes(&self) -> &'a [u8] {
        // SAFETY: chunk admission retains this input and its nonempty bounded prefix.
        unsafe { self.source.borrow().current.get_unchecked(..self.len.get()) }
    }

    pub(super) fn first(&self) -> u8 {
        // SAFETY: this chunk retains at least one byte of its original input.
        unsafe { *self.source.borrow().current.get_unchecked(0) }
    }

    pub(super) fn into_source(self) -> S {
        self.source
    }

    fn consume(mut self, len: usize) -> S {
        let cursor = self.source.borrow_mut();
        // SAFETY: only this chunk's checked read count, nonempty byte,
        // bounded prefix, and bounded search can consume its retained input.
        cursor.current = unsafe { cursor.current.get_unchecked(len..) };
        cursor.consumed = cursor.consumed.add(len);
        self.source
    }

    fn byte(self) -> u8 {
        let byte = self.first();
        self.consume(1);
        byte
    }

    fn prefix(self, limit: usize) -> &'a [u8] {
        let len = limit.min(self.len.get());
        // SAFETY: len is bounded by this retained chunk's admitted prefix.
        let bytes = unsafe { self.source.borrow().current.get_unchecked(..len) };
        self.consume(len);
        bytes
    }

    fn zero(self) -> (&'a [u8], bool) {
        let (bytes, ended) = match scan::Match::find(0, self.bytes()) {
            Some(found) => (found.through(), true),
            None => (self.bytes(), false),
        };
        self.consume(bytes.len());
        (bytes, ended)
    }
}

impl<'a> Chunk<'a, Cursor<'a>> {
    pub(super) fn inflate<const CHECK: bool>(
        self,
        decoder: &mut DecompressorOxide,
        output: &mut [u8],
        written: usize,
    ) -> (Cursor<'a>, TINFLStatus, usize, usize) {
        let length = if CHECK {
            written.saturating_add(STEP).min(output.len())
        } else {
            output.len()
        };
        let output = &mut output[..length];
        let space = output.len() - written;
        let flags = inflate_flags::TINFL_FLAG_HAS_MORE_INPUT
            | inflate_flags::TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF;
        let (status, read, wrote) = decompress(decoder, self.bytes(), output, written, flags);
        let (status, read, wrote) = if read > self.len.get() || wrote > space {
            (TINFLStatus::Failed, 0, 0)
        } else {
            (status, read, wrote)
        };
        (self.consume(read), status, read, wrote)
    }
}

pub(super) struct Input<'a, I>
where
    I: Iterator<Item = &'a [u8]>,
{
    segments: I,
    cursor: Cursor<'a>,
}

/// The current source and offset, moved without the segment iterator.
pub(super) struct Cursor<'a> {
    current: &'a [u8],
    pub(super) consumed: Count,
}

pub(super) enum Step<'a, S> {
    Chunk(Chunk<'a, S>),
    End(End),
}

pub(super) struct End(Count);

impl End {
    pub(super) fn error(self, part: Part) -> Error {
        Error::Eof {
            part,
            offset: self.0,
        }
    }
}

impl<'a, I> Input<'a, I>
where
    I: Iterator<Item = &'a [u8]>,
{
    pub(super) fn new(mut segments: I) -> Self {
        let current = match segments.next() {
            Some(segment) => segment,
            None => &[],
        };
        Self {
            segments,
            cursor: Cursor {
                current,
                consumed: Count::ZERO,
            },
        }
    }

    pub(super) fn into_parts(self) -> (I, Cursor<'a>) {
        (self.segments, self.cursor)
    }

    pub(super) fn header<const CHECK: bool, E>(
        &mut self,
        checksum: Option<&Hasher>,
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<(), Checked<Error, E>> {
        self.cursor
            .header::<CHECK, E>(&mut self.segments, checksum, check)
    }
}

impl<'a> Cursor<'a> {
    fn byte<const CHECK: bool, E>(
        &mut self,
        segments: &mut impl Iterator<Item = &'a [u8]>,
        part: Part,
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<u8, Checked<Error, E>> {
        let chunk = match Chunk::next::<CHECK, E>(self, segments, check)? {
            Step::Chunk(chunk) => chunk,
            Step::End(end) => return Err(end.error(part).into()),
        };
        Ok(chunk.byte())
    }

    fn header_byte<const CHECK: bool, E>(
        &mut self,
        segments: &mut impl Iterator<Item = &'a [u8]>,
        part: Part,
        checksum: &mut impl FnMut(&[u8]),
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<u8, Checked<Error, E>> {
        let byte = self.byte::<CHECK, E>(segments, part, check)?;
        checksum(slice::from_ref(&byte));
        Ok(byte)
    }

    fn header_bytes<const CHECK: bool, E>(
        &mut self,
        segments: &mut impl Iterator<Item = &'a [u8]>,
        mut bytes: usize,
        part: Part,
        checksum: &mut impl FnMut(&[u8]),
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<(), Checked<Error, E>> {
        while bytes != 0 {
            if CHECK {
                check().map_err(Checked::Check)?;
            }
            let chunk = match Chunk::next::<CHECK, E>(&mut *self, segments, check)? {
                Step::Chunk(chunk) => chunk,
                Step::End(end) => return Err(end.error(part).into()),
            };
            let prefix = chunk.prefix(bytes);
            checksum(prefix);
            bytes -= prefix.len();
        }
        Ok(())
    }

    fn header_zero<const CHECK: bool, E>(
        &mut self,
        segments: &mut impl Iterator<Item = &'a [u8]>,
        part: Part,
        checksum: &mut impl FnMut(&[u8]),
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<(), Checked<Error, E>> {
        loop {
            if CHECK {
                check().map_err(Checked::Check)?;
            }
            let chunk = match Chunk::next::<CHECK, E>(&mut *self, segments, check)? {
                Step::Chunk(chunk) => chunk,
                Step::End(end) => return Err(end.error(part).into()),
            };
            let (prefix, ended) = chunk.zero();
            checksum(prefix);
            if ended {
                return Ok(());
            }
        }
    }

    pub(super) fn u32<const CHECK: bool, E>(
        &mut self,
        segments: &mut impl Iterator<Item = &'a [u8]>,
        part: Part,
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<u32, Checked<Error, E>> {
        let mut bytes = [0; 4];
        for byte in &mut bytes {
            *byte = self.byte::<CHECK, E>(segments, part, check)?;
        }
        Ok(u32::from_le_bytes(bytes))
    }

    pub(super) fn remaining<const CHECK: bool, E>(
        &mut self,
        segments: &mut impl Iterator<Item = &'a [u8]>,
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Count, E> {
        let mut bytes = Count::Exact(self.current.len());
        for segment in segments {
            if CHECK {
                check()?;
            }
            bytes = bytes.add(segment.len());
        }
        Ok(bytes)
    }
}

impl<'a> Cursor<'a> {
    pub(super) fn header<const CHECK: bool, E>(
        &mut self,
        segments: &mut impl Iterator<Item = &'a [u8]>,
        checksum: Option<&Hasher>,
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<(), Checked<Error, E>> {
        let offset = self.consumed;
        let first = self.byte::<CHECK, E>(segments, Part::Header, check)?;
        let second = self.byte::<CHECK, E>(segments, Part::Header, check)?;
        let method = self.byte::<CHECK, E>(segments, Part::Header, check)?;
        let flags = self.byte::<CHECK, E>(segments, Part::Header, check)?;
        let head_4 = self.byte::<CHECK, E>(segments, Part::Header, check)?;
        let head_5 = self.byte::<CHECK, E>(segments, Part::Header, check)?;
        let head_6 = self.byte::<CHECK, E>(segments, Part::Header, check)?;
        let head_7 = self.byte::<CHECK, E>(segments, Part::Header, check)?;
        let head_8 = self.byte::<CHECK, E>(segments, Part::Header, check)?;
        let head_9 = self.byte::<CHECK, E>(segments, Part::Header, check)?;

        if [first, second] != [0x1f, 0x8b] {
            return Err(Error::Header {
                offset,
                cause: HeaderFault::Magic {
                    actual: [first, second],
                },
            }
            .into());
        }
        if method != 8 {
            return Err(Error::Header {
                offset: offset.add(2),
                cause: HeaderFault::Method { actual: method },
            }
            .into());
        }

        if flags & FLAG_RESERVED != 0 {
            return Err(Error::Header {
                offset: offset.add(3),
                cause: HeaderFault::Flags { actual: flags },
            }
            .into());
        }
        if flags & FLAG_HEADER_CRC == 0 {
            return self.header_tail::<CHECK, E>(segments, flags, |_| {}, check);
        }
        let mut checksum = checksum.cloned().unwrap_or_default();
        checksum.update(&[
            first, second, method, flags, head_4, head_5, head_6, head_7, head_8, head_9,
        ]);
        self.header_tail::<CHECK, E>(segments, flags, |bytes| checksum.update(bytes), check)?;
        let offset = self.consumed;
        let low = self.byte::<CHECK, E>(segments, Part::HeaderChecksum, check)?;
        let high = self.byte::<CHECK, E>(segments, Part::HeaderChecksum, check)?;
        let expected = u16::from_le_bytes([low, high]);
        let actual = (checksum.finalize() & u32::from(u16::MAX)) as u16;
        if actual != expected {
            return Err(Error::Header {
                offset,
                cause: HeaderFault::Checksum { expected, actual },
            }
            .into());
        }
        Ok(())
    }

    fn header_tail<const CHECK: bool, E>(
        &mut self,
        segments: &mut impl Iterator<Item = &'a [u8]>,
        flags: u8,
        mut checksum: impl FnMut(&[u8]),
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<(), Checked<Error, E>> {
        if flags & FLAG_EXTRA != 0 {
            let low = self.header_byte::<CHECK, E>(segments, Part::Extra, &mut checksum, check)?;
            let high = self.header_byte::<CHECK, E>(segments, Part::Extra, &mut checksum, check)?;
            self.header_bytes::<CHECK, E>(
                segments,
                usize::from(u16::from_le_bytes([low, high])),
                Part::Extra,
                &mut checksum,
                check,
            )?;
        }
        if flags & FLAG_NAME != 0 {
            self.header_zero::<CHECK, E>(segments, Part::Name, &mut checksum, check)?;
        }
        if flags & FLAG_COMMENT != 0 {
            self.header_zero::<CHECK, E>(segments, Part::Comment, &mut checksum, check)?;
        }
        Ok(())
    }
}
