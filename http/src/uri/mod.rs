mod error;
mod host;
mod pair;
mod parser;
pub mod path;
mod scheme;
pub mod target;

use core::{net, num::NonZero};

pub use error::Error;
pub use host::Host;
pub use scheme::Scheme;

use crate::field::metadata::map::record;

/// An admitted decimal port outside the transport's `u16` domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PortError;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Authority<'a> {
    host: &'a [u8],
    port: Option<&'a [u8]>,
}

impl<'a> Authority<'a> {
    pub(crate) const fn new(host: &'a [u8], port: Option<&'a [u8]>) -> Self {
        Self { host, port }
    }

    pub fn parse(value: &'a [u8]) -> Result<Self, Error> {
        authority(value, Self::new)
    }

    pub(crate) fn validate(value: &[u8]) -> Result<(), Error> {
        authority(value, |_, _| ())
    }

    pub(crate) fn has_nonempty_port(value: &[u8]) -> Result<bool, Error> {
        authority(value, |_, port| port.is_some_and(|port| !port.is_empty()))
    }

    pub fn is_same(self, other: Self, scheme: Scheme) -> bool {
        self.spanned().is_same(other.spanned(), scheme)
    }

    pub fn compare_span(self, other: record::Span<'_>, scheme: Scheme) -> Result<bool, Error> {
        Ok(self.spanned().is_same(Parsed::parse(other)?, scheme))
    }

    pub(crate) fn spanned(self) -> Parsed<'a> {
        Parsed {
            host: record::Span::from_slice(self.host),
            port: self.port.map(record::Span::from_slice),
        }
    }

    pub const fn host(self) -> &'a [u8] {
        self.host
    }

    pub fn hostname(self) -> &'a [u8] {
        if let Some(host) = self
            .host
            .strip_prefix(b"[")
            .and_then(|host| host.strip_suffix(b"]"))
        {
            host
        } else {
            self.host
        }
    }

    pub const fn port(self) -> Option<&'a [u8]> {
        self.port
    }

    /// Converts an admitted decimal port without repeating its syntax check.
    pub fn try_port_u16(self) -> Result<Option<u16>, PortError> {
        let Some(port) = self.port.filter(|port| !port.is_empty()) else {
            return Ok(None);
        };
        let mut value = 0u16;
        for byte in port {
            value = value
                .checked_mul(10)
                .and_then(|value| value.checked_add(u16::from(*byte - b'0')))
                .ok_or(PortError)?;
        }
        Ok(Some(value))
    }

    pub fn parse_ip(value: &[u8]) -> Result<net::IpAddr, Host> {
        if let Some(value) = value.strip_prefix(b"[") {
            let Some(value) = value.strip_suffix(b"]") else {
                return Err(Host::Invalid);
            };
            let value = record::Span::from_slice(value);
            if is_future_span(value) {
                return Err(Host::Future);
            }
            return parser::Parser::new(value)
                .v6()
                .map(net::IpAddr::V6)
                .ok_or(Host::Invalid);
        }
        let span = record::Span::from_slice(value);
        if let Some(ip) = parser::Parser::new(span).v4().map(net::IpAddr::V4) {
            return Ok(ip);
        }
        if let Some(ip) = parser::Parser::new(span).v6().map(net::IpAddr::V6) {
            return Ok(ip);
        }
        let mut name = value.iter();
        let _ = registered::<false>(&mut name);
        if !value.is_empty() && name.as_slice().is_empty() {
            Err(Host::Name)
        } else {
            Err(Host::Invalid)
        }
    }

    pub(crate) fn is_ip(value: &[u8]) -> bool {
        let value = record::Span::from_slice(value);
        is_future_span(value) || parser::Parser::new(value).v6().is_some()
    }
}

/// Authority admission retained on its exact immutable record span.
pub(crate) struct Parsed<'a> {
    host: record::Span<'a>,
    port: Option<record::Span<'a>>,
}

impl<'a> Parsed<'a> {
    pub(crate) fn parse(value: record::Span<'a>) -> Result<Self, Error> {
        authority(value, |host, port| Self { host, port })
    }

    /// Whether this admitted authority has a nonempty explicit decimal port.
    pub(crate) fn has_port(self) -> bool {
        self.port.is_some_and(|port| !port.is_empty())
    }

    pub(crate) fn is_same(&self, other: Parsed<'_>, scheme: Scheme) -> bool {
        pair::is_same(self.host, other.host)
            && is_port_span_equivalent(self.port, other.port, scheme)
    }
}

/// Borrowed URI bytes: one contiguous slice or one two-segment record
/// span, whose geometry [`Source::sub`] proves once for every later use.
trait Source<'a>: Copy {
    fn span(self) -> record::Span<'a>;

    /// The bytes in `start..end`, or `None` when that range is not inside `self`.
    fn sub(self, start: usize, end: usize) -> Option<Self>;

    fn len(self) -> usize {
        self.span().len()
    }

    fn get(self, index: usize) -> Option<u8> {
        self.span().get(index)
    }
}

impl<'a> Source<'a> for &'a [u8] {
    fn span(self) -> record::Span<'a> {
        record::Span::from_slice(self)
    }

    fn sub(self, start: usize, end: usize) -> Option<Self> {
        self.get(start..end)
    }
}

impl<'a> Source<'a> for record::Span<'a> {
    fn span(self) -> record::Span<'a> {
        self
    }

    fn sub(self, start: usize, end: usize) -> Option<Self> {
        record::Span::slice(self, start..end)
    }
}

fn authority<'a, S: Source<'a>, T>(
    value: S,
    finish: impl FnOnce(S, Option<S>) -> T,
) -> Result<T, Error> {
    let span = value.span();
    if span.is_empty() {
        return Err(Error::Empty);
    }
    let literal = value.get(0) == Some(b'[');
    let end = if literal {
        let close = span
            .iter()
            .position(|&byte| byte == b']')
            .ok_or(Error::InvalidHost)?;
        let ip = value.sub(1, close).ok_or(Error::InvalidHost)?.span();
        if close == 1 || (!is_future_span(ip) && parser::Parser::new(ip).v6().is_none()) {
            return Err(Error::InvalidHost);
        }
        close + 1
    } else {
        let mut rest = span.iter();
        let port = registered::<true>(&mut rest);
        if rest.clone().next().is_some() {
            return Err(registered_error(port.is_some(), rest.copied()));
        }
        port.map_or(span.len(), NonZero::get)
    };
    if end == span.len() {
        return Ok(finish(value, None));
    }
    if literal && value.get(end) != Some(b':') {
        return Err(Error::InvalidPort);
    }
    let port = value.sub(end + 1, span.len()).ok_or(Error::InvalidPort)?;
    if literal && !port.span().iter().all(u8::is_ascii_digit) {
        return Err(Error::InvalidPort);
    }
    Ok(finish(
        value.sub(0, end).ok_or(Error::InvalidHost)?,
        Some(port),
    ))
}

fn is_port_span_equivalent(
    left: Option<record::Span<'_>>,
    right: Option<record::Span<'_>>,
    scheme: Scheme,
) -> bool {
    normalized_port_span(left, scheme) == normalized_port_span(right, scheme)
}

fn normalized_port_span(
    port: Option<record::Span<'_>>,
    scheme: Scheme,
) -> Option<record::Span<'_>> {
    let port = port.filter(|port| !port.is_empty())?;
    let first = port.iter().position(|byte| *byte != b'0');
    let start = match first {
        Some(index) => index,
        None => port.len() - 1,
    };
    let port = port.slice(start..port.len())?;
    if (scheme == Scheme::Http && port.is_equal(b"80"))
        || (scheme == Scheme::Https && port.is_equal(b"443"))
    {
        None
    } else {
        Some(port)
    }
}

#[inline(always)]
pub(crate) fn registered<'a, const WITH_PORT: bool>(
    rest: &mut (impl Iterator<Item = &'a u8> + Clone),
) -> Option<NonZero<usize>> {
    let len = rest.clone().count();
    let mut next = rest.clone();
    while let Some(&byte) = next.next() {
        if is_reg_byte(byte)
            || byte == b'%'
                && next.next().is_some_and(u8::is_ascii_hexdigit)
                && next.next().is_some_and(u8::is_ascii_hexdigit)
        {
            *rest = next.clone();
        } else if WITH_PORT && byte == b':' {
            let name = NonZero::new(len - rest.clone().count())?;
            *rest = next.clone();
            while next.next().is_some_and(u8::is_ascii_digit) {
                *rest = next.clone();
            }
            return Some(name);
        } else {
            break;
        }
    }
    None
}

pub(crate) fn registered_error(port: bool, mut tail: impl Iterator<Item = u8>) -> Error {
    if !port || tail.any(|byte| byte == b':') {
        Error::InvalidHost
    } else {
        Error::InvalidPort
    }
}

fn is_reg_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        | (byte.wrapping_sub(b'&') <= b'.' - b'&')
        | matches!(byte, b'!' | b'$' | b';' | b'=' | b'_' | b'~')
}

fn is_future_byte(byte: u8) -> bool {
    is_reg_byte(byte) || byte == b':'
}

fn is_future_span(value: record::Span<'_>) -> bool {
    if !matches!(value.get(0), Some(b'v' | b'V')) {
        return false;
    }
    let Some(dot) = value.iter().position(|byte| *byte == b'.') else {
        return false;
    };
    dot > 1
        && (1..dot).all(|index| {
            value
                .get(index)
                .is_some_and(|byte| byte.is_ascii_hexdigit())
        })
        && dot + 1 < value.len()
        && (dot + 1..value.len()).all(|index| value.get(index).is_some_and(is_future_byte))
}
