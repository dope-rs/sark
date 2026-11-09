//! Parsing and state updates for HTTP headers with protocol-level semantics.

use core::num::NonZero;

use crate::{
    field::{
        head::{Error, Flags, headers::Bounds},
        scan::{Bytes, Outcome},
        text::{Ascii, IntError},
    },
    h1::head::request,
};

pub(super) const fn is_whitespace(byte: u8) -> bool {
    byte == b' ' || byte == b'\t'
}

pub(super) fn try_token_line_lit<const N: usize>(raw: &[u8], expected: &[u8; N]) -> Option<Bounds> {
    let start = if raw.first() == Some(&b' ') { 1 } else { 0 };
    let end = start + N;
    if raw.len() < end + 2 {
        return None;
    }
    if !raw[start..end].eq_ignore_ascii_case(expected) {
        return None;
    }
    if raw[end] != b'\r' || raw[end + 1] != b'\n' {
        return None;
    }
    Some(Bounds::new(end, start, end))
}

pub(super) fn clen_line(scan: &mut request::Fields, raw: &[u8]) -> Result<Option<Bounds>, Error> {
    let Some((len, bounds)) = parse_clen(raw)? else {
        return Ok(None);
    };
    scan.message = scan.message.try_length(len).map_err(Error::Message)?;
    Ok(Some(bounds))
}

pub(super) fn te_line(scan: &mut request::Fields, raw: &[u8]) -> Result<Option<Bounds>, Error> {
    let (bounds, value) = match try_token_line_lit(raw, b"chunked") {
        Some(bounds) => (bounds, b"chunked".as_slice()),
        None => {
            let Some(bounds) = trim_line(raw)? else {
                return Ok(None);
            };
            (bounds, bounds.value(raw))
        }
    };
    scan.message = scan.message.try_coding(value).map_err(Error::Message)?;
    Ok(Some(bounds))
}

fn scan_length(raw: &[u8]) -> Result<Option<(usize, usize, usize)>, Error> {
    let mut idx = if raw.first() == Some(&b' ') { 1 } else { 0 };
    while idx < raw.len() && is_whitespace(raw[idx]) {
        idx += 1;
    }
    let start = idx;
    if start == raw.len() {
        return Ok(None);
    }
    // SAFETY: the OWS scan stopped within this same immutable raw.
    let rest = unsafe { raw.get_unchecked(start..) };
    let (value, len) = Ascii::new(rest)
        .parse_prefix()
        .map_err(Error::ContentLength)?;
    let end = start + len;
    Ok(Some((value, start, end)))
}

pub(super) fn trim_line(raw: &[u8]) -> Result<Option<Bounds>, Error> {
    let mut idx = 0usize;
    while idx < raw.len() && is_whitespace(raw[idx]) {
        idx += 1;
    }
    let start = idx;
    let cr = match Bytes::new(raw).value(start) {
        Outcome::Found(cr) => cr,
        Outcome::Partial => return Ok(None),
        Outcome::Invalid { .. } => return Err(Error::Value),
    };
    // SAFETY: this scan found CRLF after start inside the same immutable raw.
    let mut value = unsafe { raw.get_unchecked(start..cr) };
    while matches!(value.last(), Some(b' ' | b'\t')) {
        value = &value[..value.len() - 1];
    }
    let end = start + value.len();
    Ok(Some(Bounds::new(cr, start, end)))
}

fn parse_clen(raw: &[u8]) -> Result<Option<(usize, Bounds)>, Error> {
    let Some((value, start, end)) = scan_length(raw)? else {
        return Ok(None);
    };
    // SAFETY: scan_length counted actual digits within this exact raw's suffix.
    let rest = unsafe { raw.get_unchecked(end..) };
    if rest.starts_with(b"\r\n") {
        return Ok(Some((value, Bounds::new(end, start, end))));
    }
    let mut idx = end;
    while idx < raw.len() && is_whitespace(raw[idx]) {
        idx += 1;
    }
    if idx >= raw.len() {
        return Ok(None);
    }
    if raw[idx] != b'\r' {
        return Err(Error::ContentLength(IntError::InvalidByte));
    }
    if idx + 1 >= raw.len() {
        return Ok(None);
    }
    if raw[idx + 1] != b'\n' {
        return Err(Error::ContentLength(IntError::InvalidByte));
    }
    Ok(Some((value, Bounds::new(idx, start, end))))
}

pub(super) fn ae_line(
    scan: &mut request::Fields,
    flags: &mut Flags,
    raw: &[u8],
) -> Result<Option<Bounds>, Error> {
    let Some(bounds) = trim_line(raw)? else {
        return Ok(None);
    };
    ae_value(scan, flags, bounds.value(raw))?;
    Ok(Some(bounds))
}

const GZIP_SEEN: u16 = 1 << 3;
const GZIP_ALLOWED: u16 = 1 << 4;
const GZIP_EXCLUDED: u16 = 1 << 5;
const WILDCARD_ALLOWED: u16 = 1 << 6;
const WILDCARD_EXCLUDED: u16 = 1 << 7;

const GZIP: usize = 0;
const BR: usize = 1;
const IDENTITY: usize = 2;
const WILDCARD: usize = 3;

/// Coding qualities use 0..=1000; repeated explicit qualities use their minimum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptEncoding([NonZero<u16>; 4]);

impl Default for AcceptEncoding {
    fn default() -> Self {
        Self([NonZero::<u16>::MAX; 4])
    }
}

impl AcceptEncoding {
    pub fn try_parse(raw: &[u8]) -> Result<Self, Error> {
        let (_, encoding) = scan::<true>(raw)?;
        Ok(encoding)
    }

    pub fn try_apply(
        fields: &mut request::Fields,
        flags: &mut Flags,
        raw: &[u8],
    ) -> Result<Self, Error> {
        let (next, encoding) = scan::<true>(raw)?;
        apply(fields, flags, next);
        Ok(encoding)
    }

    pub fn scan_line(
        fields: &mut request::Fields,
        flags: &mut Flags,
        raw: &[u8],
    ) -> Result<Option<(Bounds, Self)>, Error> {
        let Some(bounds) = trim_line(raw)? else {
            return Ok(None);
        };
        let encoding = Self::try_apply(fields, flags, bounds.value(raw))?;
        Ok(Some((bounds, encoding)))
    }

    pub fn append(&mut self, other: Self) {
        for (target, value) in self.0.iter_mut().zip(other.0) {
            *target = (*target).min(value);
        }
    }

    pub fn gzip(self) -> u16 {
        Self::resolve(self.0[GZIP], self.0[WILDCARD])
    }

    pub fn br(self) -> u16 {
        Self::resolve(self.0[BR], self.0[WILDCARD])
    }

    fn resolve(value: NonZero<u16>, wildcard: NonZero<u16>) -> u16 {
        match value {
            value if value != NonZero::<u16>::MAX => value.get() - 1,
            _ if wildcard != NonZero::<u16>::MAX => wildcard.get() - 1,
            _ => 0,
        }
    }

    pub fn identity(self) -> u16 {
        match self.0[IDENTITY] {
            q if q != NonZero::<u16>::MAX => q.get() - 1,
            _ if self.0[WILDCARD] == NonZero::<u16>::MIN => 0,
            _ => 1000,
        }
    }
}

/// Accumulates supported coding admission across all field lines. An explicit
/// gzip member overrides the wildcard, and an explicit zero remains excluded.
pub(super) fn ae_value(
    fields: &mut request::Fields,
    flags: &mut Flags,
    raw: &[u8],
) -> Result<(), Error> {
    let (next, _) = scan::<false>(raw)?;
    apply(fields, flags, next);
    Ok(())
}

fn scan<const FULL: bool>(raw: &[u8]) -> Result<(Flags, AcceptEncoding), Error> {
    let mut next = Flags::default();
    let mut encoding = AcceptEncoding::default();
    for member in raw.split(|&byte| byte == b',') {
        let member = crate::field::Error::trim_ows(member);
        if member.is_empty() {
            continue;
        }
        let end = member
            .iter()
            .position(|&byte| !crate::field::Error::is_name_byte(byte))
            .unwrap_or(member.len());
        if end == 0 {
            return Err(Error::Value);
        }
        let token = &member[..end];
        let quality = quality::<FULL>(&member[end..])?;
        let slot = if token.eq_ignore_ascii_case(b"gzip") {
            next.set(GZIP_SEEN);
            next.set(if quality != 0 {
                GZIP_ALLOWED
            } else {
                GZIP_EXCLUDED
            });
            GZIP
        } else if token == b"*" {
            next.set(if quality != 0 {
                WILDCARD_ALLOWED
            } else {
                WILDCARD_EXCLUDED
            });
            WILDCARD
        } else if FULL && token.eq_ignore_ascii_case(b"br") {
            BR
        } else if FULL && token.eq_ignore_ascii_case(b"identity") {
            IDENTITY
        } else {
            continue;
        };
        if FULL && let Some(value) = NonZero::new(quality + 1) {
            match slot {
                GZIP => encoding.0[GZIP] = encoding.0[GZIP].min(value),
                BR => encoding.0[BR] = encoding.0[BR].min(value),
                IDENTITY => encoding.0[IDENTITY] = encoding.0[IDENTITY].min(value),
                _ => encoding.0[WILDCARD] = encoding.0[WILDCARD].min(value),
            }
        }
    }
    Ok((next, encoding))
}

fn apply(fields: &mut request::Fields, flags: &mut Flags, next: Flags) {
    flags.0 |= next.0;
    let allowed = if flags.is_set(GZIP_SEEN) {
        flags.is_set(GZIP_ALLOWED) && !flags.is_set(GZIP_EXCLUDED)
    } else {
        flags.is_set(WILDCARD_ALLOWED) && !flags.is_set(WILDCARD_EXCLUDED)
    };
    fields.message.set_coding(if allowed {
        request::Coding::Gzip
    } else {
        request::Coding::Plain
    });
}

/// The quality grammar has only four significant digits; parsing it directly
/// keeps malformed controls and parameters out of the admitted field value.
fn quality<const FULL: bool>(raw: &[u8]) -> Result<u16, Error> {
    let raw = crate::field::Error::trim_ows(raw);
    if raw.is_empty() {
        return Ok(if FULL { 1000 } else { 1 });
    }
    let Some(raw) = raw.strip_prefix(b";") else {
        return Err(Error::Value);
    };
    let raw = crate::field::Error::trim_ows(raw);
    let Some((&name, raw)) = raw.split_first() else {
        return Err(Error::Value);
    };
    if !name.eq_ignore_ascii_case(&b'q') {
        return Err(Error::Value);
    }
    let Some(raw) = raw.strip_prefix(b"=") else {
        return Err(Error::Value);
    };
    let Some((&whole, rest)) = raw.split_first() else {
        return Err(Error::Value);
    };
    if !matches!(whole, b'0' | b'1') {
        return Err(Error::Value);
    }
    let fraction = if rest.is_empty() {
        rest
    } else {
        rest.strip_prefix(b".").ok_or(Error::Value)?
    };
    if fraction.len() > 3 {
        return Err(Error::Value);
    }
    let mut quality = u16::from(whole == b'1');
    if FULL {
        quality *= 1000;
    }
    let mut scale = 100;
    for &byte in fraction {
        if !byte.is_ascii_digit() || (whole == b'1' && byte != b'0') {
            return Err(Error::Value);
        }
        if FULL {
            quality += u16::from(byte - b'0') * scale;
            scale /= 10;
        } else {
            quality |= u16::from(byte != b'0');
        }
    }
    Ok(quality)
}
