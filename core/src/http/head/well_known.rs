use super::KnownHeader;
use super::error::{
    ERR_HEADER_LINE_TOO_LONG, ERR_INVALID_HEADER_NAME, ERR_INVALID_HEADER_VALUE,
    ERR_TOO_MANY_HEADERS,
};
use super::flags::Flags;
use super::input::HeaderLine;
use super::visitor::Visitor;
use crate::error::{Error, Result};
use crate::http::codec;
use sark_protocol::is_header_name_byte;

const MATCH_MASK: u64 = u64::from_le_bytes([0x20, 0x20, 0x20, 0x20, 0x20, 0xff, 0xff, 0xff]);
const PROBE_HOST: u64 = 18446743225259749224u64;
const PROBE_EXPECT: u64 = 18446743401101555813u64;
const PROBE_CONN: u64 = 18446743409842351971u64;
const PROBE_CLEN: u64 = 18446743409943015267u64;
const PROBE_TE: u64 = 18446743469971042932u64;
const PROBE_AE: u64 = 18446743456935273313u64;
const PROBE_UA: u64 = u64::from_le_bytes([b'u', b's', b'e', b'r', b'-', 0xff, 0xff, 0xff]);

pub const MAX_HEADER_LINE_BYTES: usize = 8 * 1024;

pub struct WellKnownHeaders<'a> {
    scan: &'a mut codec::HeaderScan,
    flags: &'a mut Flags,
}

impl<'a> WellKnownHeaders<'a> {
    pub fn new(scan: &'a mut codec::HeaderScan, flags: &'a mut Flags) -> Self {
        Self { scan, flags }
    }

    pub fn apply_contiguous<V: Visitor>(
        &mut self,
        rest: &[u8],
        visitor: &mut V,
        header_count: &mut usize,
        max_header_count: usize,
    ) -> Result<Option<usize>> {
        if rest.len() <= MAX_HEADER_LINE_BYTES {
            return self.apply_contiguous_inner(rest, visitor, header_count, max_header_count);
        }
        let capped = &rest[..MAX_HEADER_LINE_BYTES];
        match self.apply_contiguous_inner(capped, visitor, header_count, max_header_count)? {
            Some(out) => Ok(Some(out)),
            None => Err(Error::bad_request(ERR_HEADER_LINE_TOO_LONG)),
        }
    }

    pub fn apply_unknown_contiguous<V: Visitor>(
        &mut self,
        bytes: &[u8],
        start: usize,
        visitor: &mut V,
        header_count: &mut usize,
        max_header_count: usize,
    ) -> Result<Option<usize>> {
        let Some((name_end, name_term)) = HeaderLine::new(bytes).find_name_end_valid(start)? else {
            return Ok(None);
        };
        let colon_idx = if name_term == b':' {
            if name_end == 0 {
                return Err(Error::bad_request(ERR_INVALID_HEADER_NAME));
            }
            name_end
        } else {
            if name_end + 1 >= bytes.len() {
                return Ok(None);
            }
            if bytes[name_end + 1] == b'\n' {
                if name_end == 0 {
                    return Ok(Some(0));
                }
                return Err(Error::bad_request(ERR_INVALID_HEADER_NAME));
            }
            return Err(Error::bad_request(ERR_INVALID_HEADER_NAME));
        };
        self.apply_unknown_value(bytes, colon_idx, visitor, header_count, max_header_count)
    }

    pub fn apply(
        &mut self,
        line: &[u8],
        colon_idx: usize,
        pretrim_start: Option<usize>,
        pretrim_end: Option<usize>,
    ) -> Result<()> {
        if line.len() > MAX_HEADER_LINE_BYTES {
            return Err(Error::bad_request(ERR_HEADER_LINE_TOO_LONG));
        }
        if colon_idx == 0 {
            return Err(Error::bad_request(ERR_INVALID_HEADER_NAME));
        }
        let Some(name) = line.get(..colon_idx) else {
            return Err(Error::bad_request(ERR_INVALID_HEADER_NAME));
        };
        if name.iter().any(|raw| !is_header_name_byte(*raw)) {
            return Err(Error::bad_request(ERR_INVALID_HEADER_NAME));
        }
        let Some(header) = KnownHeader::from_name(name) else {
            return Ok(());
        };
        let (value_start, value_end) = if let Some(start) = pretrim_start {
            (
                start.min(line.len()),
                pretrim_end.unwrap_or(line.len()).min(line.len()),
            )
        } else {
            let Some(range) = HeaderLine::new(line).trimmed_range(colon_idx + 1, line.len()) else {
                return Err(Error::bad_request(ERR_INVALID_HEADER_VALUE));
            };
            range
        };
        let Some(raw) = line.get(value_start..value_end) else {
            return Err(Error::bad_request(ERR_INVALID_HEADER_VALUE));
        };
        header.apply(self.scan, self.flags, raw)
    }

    fn apply_contiguous_inner<V: Visitor>(
        &mut self,
        rest: &[u8],
        visitor: &mut V,
        header_count: &mut usize,
        max_header_count: usize,
    ) -> Result<Option<usize>> {
        let &[a, b, c, d, e, f, g, h, ..] = rest else {
            return self.apply_unknown_contiguous(rest, 0, visitor, header_count, max_header_count);
        };
        let probe_key = u64::from_le_bytes([a, b, c, d, e, f, g, h]) | MATCH_MASK;
        match probe_key {
            PROBE_HOST => self.apply_known_contiguous(
                rest,
                KnownHeader::Host,
                4,
                8,
                visitor,
                header_count,
                max_header_count,
            ),
            PROBE_EXPECT => self.apply_known_contiguous(
                rest,
                KnownHeader::Expect,
                6,
                7,
                visitor,
                header_count,
                max_header_count,
            ),
            PROBE_CONN => self.apply_known_contiguous(
                rest,
                KnownHeader::Connection,
                10,
                11,
                visitor,
                header_count,
                max_header_count,
            ),
            PROBE_CLEN => self.apply_known_contiguous(
                rest,
                KnownHeader::ContentLength,
                14,
                15,
                visitor,
                header_count,
                max_header_count,
            ),
            PROBE_TE => self.apply_known_contiguous(
                rest,
                KnownHeader::TransferEncoding,
                17,
                18,
                visitor,
                header_count,
                max_header_count,
            ),
            PROBE_AE if rest.get(6) == Some(&b':') => {
                self.apply_unknown_value(rest, 6, visitor, header_count, max_header_count)
            }
            PROBE_AE => self.apply_known_contiguous(
                rest,
                KnownHeader::AcceptEncoding,
                15,
                16,
                visitor,
                header_count,
                max_header_count,
            ),
            PROBE_UA if Self::user_agent_tail_matches(rest) => {
                self.apply_unknown_value(rest, 10, visitor, header_count, max_header_count)
            }
            _ => self.apply_unknown_contiguous(rest, 0, visitor, header_count, max_header_count),
        }
    }

    fn apply_known_contiguous<V: Visitor>(
        &mut self,
        rest: &[u8],
        header: KnownHeader,
        colon_idx: usize,
        minimum_len: usize,
        visitor: &mut V,
        header_count: &mut usize,
        max_header_count: usize,
    ) -> Result<Option<usize>> {
        if rest.len() < minimum_len || !Self::tail_matches(header, rest) {
            return self.apply_unknown_contiguous(rest, 5, visitor, header_count, max_header_count);
        }
        let Some(value_rest) = rest.get(colon_idx + 1..) else {
            return Ok(None);
        };
        let Some((tail_end, value_start, value_end)) =
            header.scan_line(self.scan, self.flags, value_rest)?
        else {
            return Ok(None);
        };
        Self::count_header(header_count, max_header_count)?;
        if V::WANTS_KNOWN {
            let Some(value) = value_rest.get(value_start..value_end) else {
                return Err(Error::bad_request(ERR_INVALID_HEADER_VALUE));
            };
            visitor.known(header, value)?;
        }
        Ok(Some(colon_idx + 1 + tail_end))
    }

    fn apply_unknown_value<V: Visitor>(
        &mut self,
        bytes: &[u8],
        colon_idx: usize,
        visitor: &mut V,
        header_count: &mut usize,
        max_header_count: usize,
    ) -> Result<Option<usize>> {
        let line_end = match crate::http::scan::scan_header_value(bytes, colon_idx + 1) {
            crate::http::scan::HeaderValueOutcome::Found { pos } => pos,
            crate::http::scan::HeaderValueOutcome::Invalid => {
                return Err(Error::bad_request(ERR_INVALID_HEADER_VALUE));
            }
            crate::http::scan::HeaderValueOutcome::None => return Ok(None),
        };
        Self::count_header(header_count, max_header_count)?;
        let Some(name) = bytes.get(..colon_idx) else {
            return Err(Error::bad_request(ERR_INVALID_HEADER_NAME));
        };
        let Some((value_start, value_end)) =
            HeaderLine::new(bytes).trimmed_range(colon_idx + 1, line_end)
        else {
            return Err(Error::bad_request(ERR_INVALID_HEADER_VALUE));
        };
        let Some(value) = bytes.get(value_start..value_end) else {
            return Err(Error::bad_request(ERR_INVALID_HEADER_VALUE));
        };
        visitor.unknown(name, value)?;
        Ok(Some(line_end))
    }

    fn count_header(header_count: &mut usize, max_header_count: usize) -> Result<()> {
        if *header_count >= max_header_count {
            return Err(Error::bad_request(ERR_TOO_MANY_HEADERS));
        }
        *header_count += 1;
        Ok(())
    }

    fn tail_matches(header: KnownHeader, rest: &[u8]) -> bool {
        match header {
            KnownHeader::Host => true,
            KnownHeader::Expect => rest
                .get(5..7)
                .is_some_and(|tail| tail.eq_ignore_ascii_case(b"t:")),
            KnownHeader::Connection => rest
                .get(5..11)
                .is_some_and(|tail| tail.eq_ignore_ascii_case(b"ction:")),
            KnownHeader::ContentLength => rest
                .get(5..15)
                .is_some_and(|tail| tail.eq_ignore_ascii_case(b"nt-length:")),
            KnownHeader::TransferEncoding => rest
                .get(5..18)
                .is_some_and(|tail| tail.eq_ignore_ascii_case(b"fer-encoding:")),
            KnownHeader::AcceptEncoding => rest
                .get(5..16)
                .is_some_and(|tail| tail.eq_ignore_ascii_case(b"t-encoding:")),
        }
    }

    fn user_agent_tail_matches(rest: &[u8]) -> bool {
        rest.get(5..11)
            .is_some_and(|tail| tail.eq_ignore_ascii_case(b"agent:"))
    }
}
