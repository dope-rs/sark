use std::mem::MaybeUninit;
use std::ops::Range;

use memchr::{memchr2, memchr3};
use o3::buffer::{Borrowed, Bytes, Owned, Retained, Shared};

use crate::Result;
use crate::body::InlineToken;
use crate::error::Fail;
use crate::scan::Scan;
use crate::view::JsonBytes;

mod source {
    pub trait Sealed {}
}

#[doc(hidden)]
pub trait ParseSource: source::Sealed + Copy {
    type Frame;
    type Slice;

    fn input(&self) -> &[u8];
    fn project_frame(self, range: Range<usize>) -> Result<Self::Frame>;
    fn project_slice(self, range: Range<usize>) -> Result<Self::Slice>;
    fn decode_frame(raw: &[u8], decoded_len: usize) -> Result<Self::Frame>;
}

impl source::Sealed for &[u8] {}

impl<'req> ParseSource for &'req [u8] {
    type Frame = JsonBytes<'req>;
    type Slice = Bytes<Borrowed<'req>>;

    fn input(&self) -> &[u8] {
        self
    }

    fn project_frame(self, range: Range<usize>) -> Result<Self::Frame> {
        self.get(range)
            .map(JsonBytes::borrowed)
            .ok_or_else(Fail::bad)
    }

    fn project_slice(self, range: Range<usize>) -> Result<Self::Slice> {
        Bytes::<Borrowed<'req>>::from(self)
            .get(range)
            .ok_or_else(Fail::bad)
    }

    fn decode_frame(raw: &[u8], decoded_len: usize) -> Result<Self::Frame> {
        decode_vec(raw, decoded_len).map(JsonBytes::owned)
    }
}

impl source::Sealed for &Shared {}

impl ParseSource for &Shared {
    type Frame = Bytes<Retained>;
    type Slice = Bytes<Retained>;

    fn input(&self) -> &[u8] {
        self.as_slice()
    }

    fn project_frame(self, range: Range<usize>) -> Result<Self::Frame> {
        self.get(range)
            .map(Bytes::<Retained>::from)
            .ok_or_else(Fail::bad)
    }

    fn project_slice(self, range: Range<usize>) -> Result<Self::Slice> {
        self.get(range)
            .map(Bytes::<Retained>::from)
            .ok_or_else(Fail::bad)
    }

    fn decode_frame(raw: &[u8], decoded_len: usize) -> Result<Self::Frame> {
        let decoded = decode_owned(raw, decoded_len)?;
        Ok(Bytes::<Retained>::from(decoded.freeze()))
    }
}

pub struct Parse;

impl Parse {
    pub fn frame<S: ParseSource>(source: S, idx: &mut usize) -> Result<S::Frame> {
        let input = source.input();
        if *idx >= input.len() {
            return Err(Fail::bad());
        }
        if input[*idx] == b'"' {
            let span = Scan::string_span(input, idx)?;
            if span.escaped {
                return S::decode_frame(&input[span.start..span.end], span.decoded_len);
            }
            return source.project_frame(span.start..span.end);
        }
        let start = *idx;
        Scan::skip_value(input, idx)?;
        let end = *idx;
        if end <= start {
            return Err(Fail::bad());
        }
        source.project_frame(start..end)
    }

    pub fn empty_view<'req>() -> JsonBytes<'req> {
        JsonBytes::borrowed(&[])
    }

    pub fn empty_frame() -> Bytes<Retained> {
        Bytes::<Retained>::from(Shared::new())
    }

    pub fn frame_plain<S: ParseSource>(source: S, idx: &mut usize) -> Result<S::Slice> {
        let range = plain_range(source.input(), idx)?;
        source.project_slice(range)
    }

    pub fn inline_plain<const N: usize>(input: &[u8], idx: &mut usize) -> Result<InlineToken<N>> {
        let range = inline_plain_range(input, idx, N.min(u8::MAX as usize))?;
        InlineToken::from_slice(&input[range])
    }

    pub fn frame_raw<S: ParseSource>(source: S, idx: &mut usize) -> Result<S::Slice> {
        let range = raw_range(source.input(), idx)?;
        source.project_slice(range)
    }

    pub fn inline_raw<const N: usize>(input: &[u8], idx: &mut usize) -> Result<InlineToken<N>> {
        let range = inline_raw_range(input, idx, N.min(u8::MAX as usize))?;
        InlineToken::from_slice(&input[range])
    }

    pub fn u64(input: &[u8], idx: &mut usize) -> Result<u64> {
        if *idx >= input.len() {
            return Err(Fail::bad());
        }
        let mut value = 0u64;
        let mut seen = false;
        while *idx < input.len() {
            let b = input[*idx];
            if !b.is_ascii_digit() {
                break;
            }
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add((b - b'0') as u64))
                .ok_or_else(Fail::bad)?;
            *idx += 1;
            seen = true;
        }
        if !seen {
            return Err(Fail::bad());
        }
        Ok(value)
    }

    pub fn bool(input: &[u8], idx: &mut usize) -> Result<bool> {
        if input.get(*idx..(*idx + 4)) == Some(b"true") {
            *idx += 4;
            return Ok(true);
        }
        if input.get(*idx..(*idx + 5)) == Some(b"false") {
            *idx += 5;
            return Ok(false);
        }
        Err(Fail::bad())
    }
}

fn plain_range(input: &[u8], idx: &mut usize) -> Result<Range<usize>> {
    Scan::expect_byte(input, idx, b'"')?;
    let start = *idx;
    let Some(relative) = memchr2(b'"', b'\\', &input[start..]) else {
        *idx = input.len();
        return Err(Fail::bad());
    };
    let end = start + relative;
    *idx = end;
    if input[end] == b'\\' {
        return Err(Fail::bad());
    }
    *idx += 1;
    Ok(start..end)
}

fn raw_range(input: &[u8], idx: &mut usize) -> Result<Range<usize>> {
    let start = *idx;
    if start >= input.len() {
        return Err(Fail::bad());
    }
    let end =
        memchr3(b',', b'}', b']', &input[start..]).map_or(input.len(), |relative| start + relative);
    *idx = end;
    if end <= start {
        return Err(Fail::bad());
    }
    Ok(start..end)
}

fn inline_plain_range(input: &[u8], idx: &mut usize, capacity: usize) -> Result<Range<usize>> {
    Scan::expect_byte(input, idx, b'"')?;
    let start = *idx;
    let scan_end = input.len().min(start.saturating_add(capacity + 1));
    if let Some(relative) = memchr2(b'"', b'\\', &input[start..scan_end]) {
        let end = start + relative;
        *idx = end;
        if input[end] == b'\\' {
            return Err(Fail::bad());
        }
        *idx += 1;
        return Ok(start..end);
    }
    if input.len() > start.saturating_add(capacity) {
        *idx = start + capacity;
    } else {
        *idx = input.len();
    }
    Err(Fail::bad())
}

fn inline_raw_range(input: &[u8], idx: &mut usize, capacity: usize) -> Result<Range<usize>> {
    let start = *idx;
    if start >= input.len() {
        return Err(Fail::bad());
    }
    let scan_end = input.len().min(start.saturating_add(capacity + 1));
    if let Some(relative) = memchr3(b',', b'}', b']', &input[start..scan_end]) {
        let end = start + relative;
        *idx = end;
        if end == start {
            return Err(Fail::bad());
        }
        return Ok(start..end);
    }
    if input.len() > start.saturating_add(capacity) {
        *idx = start + capacity;
        return Err(Fail::bad());
    }
    *idx = input.len();
    if *idx == start {
        return Err(Fail::bad());
    }
    Ok(start..*idx)
}

fn decode_vec(raw: &[u8], decoded_len: usize) -> Result<Vec<u8>> {
    let mut decoded = Vec::with_capacity(decoded_len);
    decode_string(raw, &mut decoded)?;
    Ok(decoded)
}

fn decode_owned(raw: &[u8], decoded_len: usize) -> Result<Owned> {
    Owned::try_build_exact(decoded_len, |writer| {
        writer
            .try_fill(|output| decode_into(raw, output))
            .map_err(|_| Fail::bad())
    })
    .map_err(|_| Fail::bad())
}

fn decode_into<'out>(raw: &[u8], output: &'out mut [MaybeUninit<u8>]) -> Result<&'out mut [u8]> {
    let mut decoded = DecodeSlice { output, written: 0 };
    decode_string(raw, &mut decoded)?;
    if decoded.written != decoded.output.len() {
        return Err(Fail::bad());
    }
    // SAFETY: DecodeSlice advances `written` only after initializing that slot,
    // and the equality above proves every slot in the returned range is initialized.
    Ok(unsafe {
        std::slice::from_raw_parts_mut(decoded.output.as_mut_ptr().cast(), decoded.written)
    })
}

trait DecodeOutput {
    fn write(&mut self, byte: u8) -> Result<()>;
}

impl DecodeOutput for Vec<u8> {
    fn write(&mut self, byte: u8) -> Result<()> {
        self.push(byte);
        Ok(())
    }
}

struct DecodeSlice<'out> {
    output: &'out mut [MaybeUninit<u8>],
    written: usize,
}

impl DecodeOutput for DecodeSlice<'_> {
    fn write(&mut self, byte: u8) -> Result<()> {
        let Some(slot) = self.output.get_mut(self.written) else {
            return Err(Fail::bad());
        };
        slot.write(byte);
        self.written += 1;
        Ok(())
    }
}

fn decode_string(raw: &[u8], output: &mut impl DecodeOutput) -> Result<()> {
    let mut read = 0;
    while read < raw.len() {
        let mut byte = raw[read];
        if byte == b'\\' {
            read += 1;
            byte = match raw.get(read) {
                Some(b'"') => b'"',
                Some(b'\\') => b'\\',
                Some(b'/') => b'/',
                Some(b'b') => 0x08,
                Some(b'f') => 0x0c,
                Some(b'n') => b'\n',
                Some(b'r') => b'\r',
                Some(b't') => b'\t',
                _ => return Err(Fail::bad()),
            };
        }
        output.write(byte)?;
        read += 1;
    }
    Ok(())
}
