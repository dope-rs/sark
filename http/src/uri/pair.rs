use core::net;

use crate::{
    field::{metadata::map::record, text},
    uri::{self, parser},
};

pub(super) fn is_same(left: record::Span<'_>, right: record::Span<'_>) -> bool {
    match (left.get(0) == Some(b'['), right.get(0) == Some(b'[')) {
        (true, true) => match (Literal::parse(left), Literal::parse(right)) {
            (Some(left), Some(right)) => left == right,
            (None, _) | (_, None) => false,
        },
        (false, false) => Name(left) == Name(right),
        (true, false) | (false, true) => false,
    }
}

#[derive(PartialEq, Eq)]
enum Literal<'a> {
    V6(net::Ipv6Addr),
    Future(Future<'a>),
}

impl<'a> Literal<'a> {
    /// The literal form of one bracketed authority, or `None` when its brackets
    /// retain neither an IPv6 literal nor a future literal.
    fn parse(value: record::Span<'a>) -> Option<Self> {
        let inner = value.slice(1..value.len().checked_sub(1)?)?;
        if let Some(ip) = parser::Parser::new(inner).v6() {
            Some(Self::V6(ip))
        } else if uri::is_future_span(inner) {
            Some(Self::Future(Future(inner)))
        } else {
            None
        }
    }
}

struct Future<'a>(record::Span<'a>);

impl PartialEq for Future<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(other.0.iter())
                .all(|(left, right)| left.eq_ignore_ascii_case(right))
    }
}

impl Eq for Future<'_> {}

struct Name<'a>(record::Span<'a>);

impl Name<'_> {
    #[inline(always)]
    fn next(value: record::Span<'_>, index: &mut usize) -> Option<u16> {
        let byte = value.get(*index)?;
        let (unit, width) = if byte == b'%' {
            let high = text::hex(value.get(*index + 1)?)?;
            let low = text::hex(value.get(*index + 2)?)?;
            let decoded = (high << 4) | low;
            if decoded.is_ascii_alphanumeric() || matches!(decoded, b'-' | b'.' | b'_' | b'~') {
                (decoded.to_ascii_lowercase() as u16, 3)
            } else {
                (0x100 | decoded as u16, 3)
            }
        } else {
            (byte.to_ascii_lowercase() as u16, 1)
        };
        *index += width;
        Some(unit)
    }
}

impl PartialEq for Name<'_> {
    fn eq(&self, other: &Self) -> bool {
        let mut left_at = 0;
        let mut right_at = 0;
        loop {
            let left = Self::next(self.0, &mut left_at);
            let right = Self::next(other.0, &mut right_at);
            if left != right {
                return false;
            }
            if left.is_none() {
                return true;
            }
        }
    }
}

impl Eq for Name<'_> {}
