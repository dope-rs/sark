//! Parsing and state updates for HTTP headers with protocol-level semantics.

use core::num::NonZero;

use crate::{
    field::{
        head::{Error, Flags, headers::Bounds},
        scan::{Bytes, Outcome},
        text::{Ascii, IntError},
    },
    h1::{field, head::request},
};

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

pub(super) fn te_line(scan: &mut request::Fields, raw: &[u8]) -> Result<Option<Bounds>, Error> {
    let (bounds, message) = match try_token_line_lit(raw, b"chunked") {
        Some(bounds) => (bounds, scan.message.try_append(field::Transfer::CHUNKED)),
        None => {
            let Some(bounds) = trim_line(raw)? else {
                return Ok(None);
            };
            (bounds, scan.message.try_coding(bounds.value(raw)))
        }
    };
    scan.message = message.map_err(Error::Message)?;
    Ok(Some(bounds))
}

pub(super) fn clen_line(scan: &mut request::Fields, raw: &[u8]) -> Result<Option<Bounds>, Error> {
    let rest = crate::field::trim_ows_start(raw);
    if rest.is_empty() {
        return Ok(None);
    }
    let start = raw.len() - rest.len();
    let (value, len) = Ascii::new(rest)
        .parse_prefix()
        .map_err(Error::ContentLength)?;
    // SAFETY: parse_prefix counted only digits in this immutable suffix.
    let tail = crate::field::trim_ows_start(unsafe { rest.get_unchecked(len..) });
    match tail {
        [b'\r', b'\n', ..] => {
            scan.message = scan.message.try_length(value).map_err(Error::Message)?;
            Ok(Some(Bounds::new(raw.len() - tail.len(), start, start + len)))
        }
        [] | [b'\r'] => Ok(None),
        _ => Err(Error::ContentLength(IntError::InvalidByte)),
    }
}

pub(super) fn trim_line(raw: &[u8]) -> Result<Option<Bounds>, Error> {
    let rest = crate::field::trim_ows_start(raw);
    let start = raw.len() - rest.len();
    trim_suffix(rest, start, start)
}

pub(super) fn trim_suffix(rest: &[u8], start: usize, base: usize) -> Result<Option<Bounds>, Error> {
    let cr = match Bytes::new(rest).value(0) {
        Outcome::Found(cr) => cr,
        Outcome::Partial => return Ok(None),
        Outcome::Invalid { .. } => return Err(Error::Value),
    };
    let mut end = cr;
    while end > 0 && matches!(rest.get(end - 1), Some(b' ' | b'\t')) {
        end -= 1;
    }
    Ok(Some(Bounds::new(base + cr, start, base + end)))
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
const IDENTITY_SEEN: u16 = 1 << 4;
const GZIP_EXCLUDED: u16 = 1 << 5;
const WILDCARD_ALLOWED: u16 = 1 << 6;
const WILDCARD_EXCLUDED: u16 = 1 << 7;
const IDENTITY_EXCLUDED: u16 = 1 << 8;

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
            if quality == 0 {
                next.set(GZIP_EXCLUDED);
            }
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
        } else if token.eq_ignore_ascii_case(b"identity") {
            next.set(IDENTITY_SEEN);
            if quality == 0 {
                next.set(IDENTITY_EXCLUDED);
            }
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
    let gzip = if flags.is_set(GZIP_SEEN) {
        !flags.is_set(GZIP_EXCLUDED)
    } else {
        flags.is_set(WILDCARD_ALLOWED) && !flags.is_set(WILDCARD_EXCLUDED)
    };
    let identity = if flags.is_set(IDENTITY_SEEN) {
        !flags.is_set(IDENTITY_EXCLUDED)
    } else {
        !flags.is_set(WILDCARD_EXCLUDED)
    };
    fields
        .message
        .set_coding(request::Coding::from_acceptance(identity, gzip));
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
