mod error;
mod host;
mod pair;
mod parser;
pub mod path;
mod scheme;
pub mod target;

use core::net;

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
        let parts = Parts::<&[u8]>::parse(value)?;
        Ok(Self {
            host: parts.host,
            port: parts.port,
        })
    }

    pub fn is_same(self, other: Self, scheme: Scheme) -> bool {
        self.spanned().is_same(other.spanned(), scheme)
    }

    pub fn compare_span(self, other: record::Span<'_>, scheme: Scheme) -> Result<bool, Error> {
        Ok(self.spanned().is_same(Parsed::parse(other)?, scheme))
    }

    pub(crate) fn spanned(self) -> Parsed<'a> {
        Parsed(Parts {
            host: record::Span::from_slice(self.host),
            port: self.port.map(record::Span::from_slice),
        })
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
        if Self::is_reg(value) {
            Err(Host::Name)
        } else {
            Err(Host::Invalid)
        }
    }

    pub(crate) fn is_port(value: &[u8]) -> bool {
        value
            .strip_prefix(b":")
            .is_some_and(|port| !port.is_empty() && port.iter().all(u8::is_ascii_digit))
    }

    pub(crate) fn is_reg(value: &[u8]) -> bool {
        is_reg_span(record::Span::from_slice(value))
    }

    pub(crate) fn is_ip(value: &[u8]) -> bool {
        let value = record::Span::from_slice(value);
        is_future_span(value) || parser::Parser::new(value).v6().is_some()
    }
}

struct Parts<S> {
    host: S,
    port: Option<S>,
}

/// Authority admission retained on its exact immutable record span.
pub(crate) struct Parsed<'a>(Parts<record::Span<'a>>);

impl<'a> Parsed<'a> {
    pub(crate) fn parse(value: record::Span<'a>) -> Result<Self, Error> {
        Parts::parse(value).map(Self)
    }

    /// Whether this admitted authority has a nonempty explicit decimal port.
    pub(crate) fn has_port(self) -> bool {
        self.0.port.is_some_and(|port| !port.is_empty())
    }

    pub(crate) fn is_same(&self, other: Parsed<'_>, scheme: Scheme) -> bool {
        pair::is_same(self.0.host, other.0.host)
            && is_port_span_equivalent(self.0.port, other.0.port, scheme)
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

fn is_digits<'a, S: Source<'a>>(value: S) -> bool {
    value.span().iter().all(u8::is_ascii_digit)
}

impl<'a, S: Source<'a>> Parts<S> {
    fn parse(value: S) -> Result<Self, Error> {
        let len = value.len();
        if len == 0 {
            return Err(Error::Empty);
        }
        if value.get(0) == Some(b'[') {
            let Some(close) = value.span().iter().position(|byte| *byte == b']') else {
                return Err(Error::InvalidHost);
            };
            let Some(literal) = value.sub(1, close) else {
                return Err(Error::InvalidHost);
            };
            let literal = literal.span();
            if close == 1
                || (!is_future_span(literal) && parser::Parser::new(literal).v6().is_none())
            {
                return Err(Error::InvalidHost);
            }
            let Some(suffix) = value.sub(close + 1, len) else {
                return Err(Error::InvalidHost);
            };
            let port = if suffix.len() == 0 {
                None
            } else {
                if suffix.get(0) != Some(b':') {
                    return Err(Error::InvalidPort);
                }
                let Some(port) = suffix.sub(1, suffix.len()) else {
                    return Err(Error::InvalidPort);
                };
                if !is_digits(port) {
                    return Err(Error::InvalidPort);
                }
                Some(port)
            };
            let Some(host) = value.sub(0, close + 1) else {
                return Err(Error::InvalidHost);
            };
            return Ok(Self { host, port });
        }
        let span = value.span();
        let mut bytes = span.iter();
        let colon = bytes.position(|byte| *byte == b':');
        if bytes.any(|byte| *byte == b':') {
            return Err(Error::InvalidHost);
        }
        let host_end = colon.unwrap_or(len);
        let Some(host) = value.sub(0, host_end) else {
            return Err(Error::InvalidHost);
        };
        if !is_reg_span(host.span()) {
            return Err(Error::InvalidHost);
        }
        let port = match colon {
            Some(index) => {
                let Some(port) = value.sub(index + 1, len) else {
                    return Err(Error::InvalidPort);
                };
                if !is_digits(port) {
                    return Err(Error::InvalidPort);
                }
                Some(port)
            }
            None => None,
        };
        Ok(Self { host, port })
    }
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

fn is_reg_span(value: record::Span<'_>) -> bool {
    if value.is_empty() {
        return false;
    }
    let mut bytes = value.iter().copied();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            if bytes.next().is_none_or(|byte| !byte.is_ascii_hexdigit())
                || bytes.next().is_none_or(|byte| !byte.is_ascii_hexdigit())
            {
                return false;
            }
        } else if !is_reg_byte(byte) {
            return false;
        }
    }
    true
}

fn is_reg_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'-' | b'.'
                | b'_'
                | b'~'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
        )
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
