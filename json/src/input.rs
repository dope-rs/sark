use std::ops;

use o3::buffer::write::consume;

use crate::{
    Error, Nesting,
    parsing::Depth,
    segment::{Atom, ValueKind},
    source::{
        outputs,
        string::{Checked, String},
    },
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Taken {
    Yes,
    No,
}

pub trait Input {
    fn ws(&self, index: &mut usize);
    fn take_byte(&self, index: &mut usize, want: u8) -> Taken;
    fn expect_byte(&self, index: &mut usize, want: u8) -> crate::Result<()>;
    fn expect_prop(&self, index: &mut usize, first: bool, want: &[u8]) -> crate::Result<()>;
    fn finish(&self, index: &mut usize) -> crate::Result<()>;
    fn name(&self, index: &mut usize) -> crate::Result<Name<'_>>;
    fn take_null(&self, index: &mut usize) -> Taken;
    fn skip_value(&self, index: &mut usize) -> crate::Result<()>;
    #[doc(hidden)]
    fn skip_depth(&self, index: &mut usize, depth: Depth) -> crate::Result<()>;

    #[doc(hidden)]
    fn string_span(&self, index: &mut usize) -> crate::Result<StringSpan<'_>>;

    fn parse_u64(&self, index: &mut usize) -> crate::Result<u64>;
    fn parse_bool(&self, index: &mut usize) -> crate::Result<bool>;
}

impl Input for [u8] {
    fn ws(&self, index: &mut usize) {
        *index = ws_at(self, *index);
    }

    fn take_byte(&self, index: &mut usize, want: u8) -> Taken {
        match byte_at(self, *index, want) {
            Some(next) => {
                *index = next;
                Taken::Yes
            }
            None => Taken::No,
        }
    }

    fn expect_byte(&self, index: &mut usize, want: u8) -> crate::Result<()> {
        let next = byte_at(self, *index, want).ok_or(Error::Invalid)?;
        *index = next;
        Ok(())
    }

    fn expect_prop(&self, index: &mut usize, first: bool, want: &[u8]) -> crate::Result<()> {
        let next = prop_at(self, *index, first, want)?;
        *index = next;
        Ok(())
    }

    fn finish(&self, index: &mut usize) -> crate::Result<()> {
        let next = ws_at(self, *index);
        if next != self.len() {
            return Err(Error::Invalid);
        }
        *index = next;
        Ok(())
    }

    fn name(&self, index: &mut usize) -> crate::Result<Name<'_>> {
        let (span, next) = string_at(self, *index)?;
        *index = next;
        Ok(Name::new(span))
    }

    fn take_null(&self, index: &mut usize) -> Taken {
        match null_at(self, *index) {
            Some(next) => {
                *index = next;
                Taken::Yes
            }
            None => Taken::No,
        }
    }

    fn skip_value(&self, index: &mut usize) -> crate::Result<()> {
        self.skip_depth(index, Depth::ROOT)
    }

    fn skip_depth(&self, index: &mut usize, depth: Depth) -> crate::Result<()> {
        let next = value_with::<true>(self, ws_at(self, *index), None, depth)?;
        *index = next;
        Ok(())
    }

    fn string_span(&self, index: &mut usize) -> crate::Result<StringSpan<'_>> {
        let (span, next) = string_at(self, *index)?;
        *index = next;
        Ok(span)
    }

    fn parse_u64(&self, index: &mut usize) -> crate::Result<u64> {
        let first = *self.get(*index).ok_or(Error::Invalid)?;
        if first == b'-' {
            return Err(Error::Invalid);
        }
        let (ValueKind::Unsigned(value), next) = atom_with::<true>(self, *index, first, None)?
        else {
            return Err(Error::Invalid);
        };
        *index = next;
        Ok(value)
    }

    fn parse_bool(&self, index: &mut usize) -> crate::Result<bool> {
        let (value, next) = bool_at(self, *index)?;
        *index = next;
        Ok(value)
    }
}

pub fn parse_i64(input: &[u8], index: &mut usize) -> crate::Result<i64> {
    let first = *input.get(*index).ok_or(Error::Invalid)?;
    let (kind, next) = atom_with::<true>(input, *index, first, None)?;
    let value = kind.i64()?;
    *index = next;
    Ok(value)
}

#[derive(Clone, Copy)]
pub(crate) struct Content<'a>(&'a [u8]);

impl<'a> Content<'a> {
    pub(crate) const fn bytes(self) -> &'a [u8] {
        self.0
    }
}

#[doc(hidden)]
pub struct StringSpan<'a> {
    pub(crate) content: Content<'a>,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) escaped: bool,
    pub(crate) decoded_len: usize,
}

#[doc(hidden)]
pub struct Name<'a> {
    content: Content<'a>,
    decoded_len: usize,
    escaped: bool,
}

impl<'a> Name<'a> {
    fn new(span: StringSpan<'a>) -> Self {
        Self {
            content: span.content,
            decoded_len: span.decoded_len,
            escaped: span.escaped,
        }
    }

    pub const fn len(&self) -> usize {
        self.decoded_len
    }

    pub const fn is_empty(&self) -> bool {
        self.decoded_len == 0
    }

    pub fn is_equal(&self, expected: &[u8]) -> bool {
        if !self.escaped {
            return self.content.bytes() == expected;
        }
        if self.decoded_len != expected.len() {
            return false;
        }
        outputs::Decoder::new(self.content).eq(expected.iter().copied())
    }
}

fn prop_at(input: &[u8], index: usize, first: bool, want: &[u8]) -> crate::Result<usize> {
    let mut next = ws_at(input, index);
    if !first {
        next = byte_at(input, next, b',').ok_or(Error::Invalid)?;
        next = ws_at(input, next);
    }
    let (span, end) = string_at(input, next)?;
    if !Name::new(span).is_equal(want) {
        return Err(Error::Invalid);
    }
    next = ws_at(input, end);
    next = byte_at(input, next, b':').ok_or(Error::Invalid)?;
    Ok(ws_at(input, next))
}

#[derive(Clone, Copy)]
struct Limit {
    start: usize,
    capacity: usize,
    overhead: usize,
}

impl Limit {
    fn check(self, end: usize) -> crate::Result<()> {
        let len = (end - self.start).saturating_sub(self.overhead);
        consume::Error::check(len, self.capacity).map_err(Error::Token)
    }

    fn scan_end(self, len: usize) -> usize {
        len.min(self.start.saturating_add(self.capacity + self.overhead + 1))
    }
}

fn check_limit(limit: Option<Limit>, end: usize) -> crate::Result<()> {
    if let Some(limit) = limit {
        limit.check(end)?;
    }
    Ok(())
}

pub(crate) fn inline_plain<'a>(
    input: &'a [u8],
    index: &mut usize,
    capacity: u8,
) -> crate::Result<Content<'a>> {
    let limit = Limit {
        start: *index,
        capacity: usize::from(capacity),
        overhead: 2,
    };
    let (span, end) = string_with::<true>(input, *index, Some(limit))?;
    if span.escaped {
        return Err(Error::Invalid);
    }
    *index = end;
    Ok(span.content)
}

pub(crate) fn inline_depth(
    input: &[u8],
    index: &mut usize,
    capacity: u8,
    depth: Depth,
) -> crate::Result<ops::Range<usize>> {
    let start = ws_at(input, *index);
    let limit = Limit {
        start,
        capacity: usize::from(capacity),
        overhead: 0,
    };
    let end = value_with::<true>(input, start, Some(limit), depth)?;
    *index = end;
    Ok(start..end)
}

pub(crate) fn try_raw(input: &str) -> crate::Result<()> {
    let input = input.as_bytes();
    let mut end = value_with::<false>(input, ws_at(input, 0), None, Depth::ROOT)?;
    input.finish(&mut end)
}

fn value_with<const UTF8: bool>(
    input: &[u8],
    index: usize,
    limit: Option<Limit>,
    depth: Depth,
) -> crate::Result<usize> {
    match input.get(index) {
        Some(b'"') => string_with::<UTF8>(input, index, limit).map(|(_, end)| end),
        Some(&open) if matches!(open, b'{' | b'[') => {
            group_at::<UTF8>(input, index, open, limit, depth)
        }
        Some(&first) if matches!(first, b't' | b'f' | b'n' | b'-' | b'0'..=b'9') => {
            atom_with::<false>(input, index, first, limit).map(|(_, end)| end)
        }
        _ => Err(Error::Invalid),
    }
}

#[inline(always)]
fn atom_with<const INTEGER: bool>(
    input: &[u8],
    index: usize,
    first: u8,
    limit: Option<Limit>,
) -> crate::Result<(ValueKind, usize)> {
    let mut atom = Atom::new(first, INTEGER)?;
    let mut next = index;
    while let Some(&byte) = input.get(next) {
        if is_whitespace(byte) || matches!(byte, b',' | b'}' | b']') {
            break;
        }
        atom = atom.step(byte)?;
        if INTEGER && !matches!(atom, Atom::Number { value: Some(_), .. }) {
            return Err(Error::Invalid);
        }
        next += 1;
        check_limit(limit, next)?;
    }
    Ok((atom.finish()?, next))
}

fn string_at(input: &[u8], index: usize) -> crate::Result<(StringSpan<'_>, usize)> {
    string_with::<true>(input, index, None)
}

fn string_with<const UTF8: bool>(
    input: &[u8],
    index: usize,
    limit: Option<Limit>,
) -> crate::Result<(StringSpan<'_>, usize)> {
    let start = byte_at(input, index, b'"').ok_or(Error::Invalid)?;
    check_limit(limit, start)?;
    let mut string = String::<Checked<UTF8>>::new();
    let mut next = start;
    let scan_end = limit.map_or(input.len(), |limit| limit.scan_end(input.len()));
    while next < input.len() {
        // SAFETY: the preceding limit check and loop bound admit this same-input suffix.
        let skipped = string.skip(unsafe { input.get_unchecked(next..scan_end) });
        next += skipped.len();
        check_limit(limit, next)?;
        let Some(&byte) = input.get(next) else {
            break;
        };
        let step = string.step(byte)?;
        check_limit(limit, next + 1)?;
        if step.is_none() {
            let removed = string.removed();
            return Ok((
                StringSpan {
                    // SAFETY: next advanced from start within this unchanged input.
                    content: Content(unsafe { input.get_unchecked(start..next) }),
                    start,
                    end: next,
                    escaped: removed != 0,
                    decoded_len: next - start - removed,
                },
                next + 1,
            ));
        }
        next += 1;
    }
    Err(Error::Invalid)
}

fn group_at<const UTF8: bool>(
    input: &[u8],
    index: usize,
    open: u8,
    limit: Option<Limit>,
    depth: Depth,
) -> crate::Result<usize> {
    let mut next = byte_at(input, index, open).ok_or(Error::Invalid)?;
    check_limit(limit, next)?;
    let mut nesting = Nesting::<UTF8>::start_depth(open, depth)?;
    let scan_end = limit.map_or(input.len(), |limit| limit.scan_end(input.len()));
    while next < input.len() {
        // SAFETY: the preceding limit check and loop bound admit this same-input suffix.
        let skipped = nesting.skip(unsafe { input.get_unchecked(next..scan_end) });
        next += skipped.len();
        check_limit(limit, next)?;
        let Some(&byte) = input.get(next) else {
            break;
        };
        next += 1;
        let done = nesting.step(byte)?;
        check_limit(limit, next)?;
        if done {
            return Ok(next);
        }
    }
    Err(Error::Invalid)
}

fn bool_at(input: &[u8], index: usize) -> crate::Result<(bool, usize)> {
    let first = *input.get(index).ok_or(Error::Invalid)?;
    if !matches!(first, b't' | b'f') {
        return Err(Error::Invalid);
    }
    let (kind, next) = atom_with::<false>(input, index, first, None)?;
    match kind {
        ValueKind::True => Ok((true, next)),
        ValueKind::False => Ok((false, next)),
        _ => Err(Error::Invalid),
    }
}

pub(crate) const fn is_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}

fn ws_at(input: &[u8], index: usize) -> usize {
    let mut next = index;
    while input.get(next).is_some_and(|byte| is_whitespace(*byte)) {
        next += 1;
    }
    next
}

fn byte_at(input: &[u8], index: usize, want: u8) -> Option<usize> {
    if input.get(index) == Some(&want) {
        index.checked_add(1)
    } else {
        None
    }
}

fn null_at(input: &[u8], index: usize) -> Option<usize> {
    let end = index.checked_add(4)?;
    (input.get(index..end) == Some(b"null")).then_some(end)
}

const _: () = assert!(size_of::<Taken>() == size_of::<bool>());
