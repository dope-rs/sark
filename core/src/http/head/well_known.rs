#![allow(
    clippy::too_many_arguments,
    clippy::collapsible_match,
    clippy::collapsible_if
)]

use super::apply::{
    ae_line, apply_accept_encoding, apply_connection, apply_content_length, apply_expect,
    apply_host, apply_transfer_encoding, clen_line, conn_line, expect_line, host_line, te_line,
};
use super::byte::{is_header_name_byte, trim_ws_range, value_has_forbidden_byte};
use super::error::{
    ERR_HEADER_LINE_TOO_LONG, ERR_INVALID_HEADER_NAME, ERR_INVALID_HEADER_VALUE,
    ERR_TOO_MANY_HEADERS, bad_request,
};
use super::flags::Flags;
use super::input::{BytesScan, HeadInput, HeaderLineScan};
use super::visitor::{Known, Visitor};
use crate::error::Result;
use crate::http::codec;

const MATCH_MASK: u64 = u64::from_le_bytes([0x20, 0x20, 0x20, 0x20, 0x20, 0xff, 0xff, 0xff]);
const PROBE_HOST: u64 = 18446743225259749224u64;
const PROBE_EXPECT: u64 = 18446743401101555813u64;
const PROBE_CONN: u64 = 18446743409842351971u64;
const PROBE_CLEN: u64 = 18446743409943015267u64;
const PROBE_TE: u64 = 18446743469971042932u64;
const PROBE_AE: u64 = 18446743456935273313u64;
const PROBE_UA: u64 = u64::from_le_bytes([b'u', b's', b'e', b'r', b'-', 0xff, 0xff, 0xff]);

const AE_NAME_LOWER: &[u8; 16] = b"accept-encoding:";

pub const MAX_HEADER_LINE_BYTES: usize = 8 * 1024;

pub fn unknown_line<V: Visitor>(
    bytes: &[u8],
    start: usize,
    visitor: &mut V,
    header_count: &mut usize,
    max_header_count: usize,
) -> Result<Option<usize>> {
    let Some((name_end, name_term)) = BytesScan::find_name_end_valid(bytes, start)? else {
        return Ok(None);
    };
    let colon_idx = if name_term == b':' {
        if name_end == 0 {
            return Err(bad_request(ERR_INVALID_HEADER_NAME));
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
            return Err(bad_request(ERR_INVALID_HEADER_NAME));
        }
        return Err(bad_request(ERR_INVALID_HEADER_NAME));
    };
    unknown_fast_skip(bytes, colon_idx, visitor, header_count, max_header_count)
}

trait WellKnownProbe {
    const COLON_IDX: usize;
    const TOTAL_LEN: usize;
    const KEY: Known;

    fn tail_matches(rest: &[u8]) -> bool;

    fn line(
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        value_rest: &[u8],
    ) -> Result<Option<(usize, usize, usize)>>;

    fn dispatch<V: Visitor>(
        rest: &[u8],
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        visitor: &mut V,
        header_count: &mut usize,
        max_header_count: usize,
    ) -> Result<Option<usize>> {
        if rest.len() >= Self::TOTAL_LEN && Self::tail_matches(rest) {
            let colon_idx = Self::COLON_IDX;
            let Some((tail_end, value_start, value_end)) =
                Self::line(scan, flags, &rest[colon_idx + 1..])?
            else {
                return Ok(None);
            };
            if *header_count == max_header_count {
                return Err(bad_request(ERR_TOO_MANY_HEADERS));
            }
            *header_count += 1;
            if V::WANTS_KNOWN {
                let off = colon_idx + 1;
                visitor.known(Self::KEY, &rest[off + value_start..off + value_end])?;
            }
            return Ok(Some(colon_idx + 1 + tail_end));
        }
        unknown_line(rest, 5, visitor, header_count, max_header_count)
    }
}

struct HostProbe;
impl WellKnownProbe for HostProbe {
    const COLON_IDX: usize = 4;
    const TOTAL_LEN: usize = 8;
    const KEY: Known = Known::Host;
    fn tail_matches(_rest: &[u8]) -> bool {
        true
    }
    fn line(
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        value_rest: &[u8],
    ) -> Result<Option<(usize, usize, usize)>> {
        host_line(scan, flags, value_rest)
    }
}

struct ExpectProbe;
impl WellKnownProbe for ExpectProbe {
    const COLON_IDX: usize = 6;
    const TOTAL_LEN: usize = 7;
    const KEY: Known = Known::Expect;
    fn tail_matches(rest: &[u8]) -> bool {
        (u16::from_le_bytes(rest[5..7].try_into().unwrap()) | 8224u16) == 14964u16
    }
    fn line(
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        value_rest: &[u8],
    ) -> Result<Option<(usize, usize, usize)>> {
        expect_line(scan, flags, value_rest)
    }
}

struct ConnProbe;
impl WellKnownProbe for ConnProbe {
    const COLON_IDX: usize = 10;
    const TOTAL_LEN: usize = 11;
    const KEY: Known = Known::Connection;
    fn tail_matches(rest: &[u8]) -> bool {
        let w: &[u8; 6] = rest[5..11].try_into().unwrap();
        (u32::from_le_bytes(w[0..4].try_into().unwrap()) | 538976288u32) == 1869182051u32
            && (u16::from_le_bytes(w[4..6].try_into().unwrap()) | 8224u16) == 14958u16
    }
    fn line(
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        value_rest: &[u8],
    ) -> Result<Option<(usize, usize, usize)>> {
        conn_line(scan, flags, value_rest)
    }
}

struct ClenProbe;
impl WellKnownProbe for ClenProbe {
    const COLON_IDX: usize = 14;
    const TOTAL_LEN: usize = 15;
    const KEY: Known = Known::ContentLength;
    fn tail_matches(rest: &[u8]) -> bool {
        let w: &[u8; 10] = rest[5..15].try_into().unwrap();
        (u64::from_le_bytes(w[0..8].try_into().unwrap()) | 2314885530818453536u64)
            == 8387794212886508654u64
            && (u64::from_le_bytes(w[2..10].try_into().unwrap()) | 2314885530818453536u64)
                == 4208741839360322605u64
    }
    fn line(
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        value_rest: &[u8],
    ) -> Result<Option<(usize, usize, usize)>> {
        clen_line(scan, flags, value_rest)
    }
}

struct TeProbe;
impl WellKnownProbe for TeProbe {
    const COLON_IDX: usize = 17;
    const TOTAL_LEN: usize = 18;
    const KEY: Known = Known::TransferEncoding;
    fn tail_matches(rest: &[u8]) -> bool {
        let w: &[u8; 13] = rest[5..18].try_into().unwrap();
        (u64::from_le_bytes(w[0..8].try_into().unwrap()) | 2314885530818453536u64)
            == 8026380341737579878u64
            && (u64::from_le_bytes(w[5..13].try_into().unwrap()) | 2314885530818453536u64)
                == 4208453775736660846u64
    }
    fn line(
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        value_rest: &[u8],
    ) -> Result<Option<(usize, usize, usize)>> {
        te_line(scan, flags, value_rest)
    }
}

struct AeProbe;
impl WellKnownProbe for AeProbe {
    const COLON_IDX: usize = 15;
    const TOTAL_LEN: usize = 16;
    const KEY: Known = Known::AcceptEncoding;
    fn tail_matches(rest: &[u8]) -> bool {
        let mut buf = [0u8; 16];
        for i in 0..16 {
            buf[i] = rest[i] | 0x20;
        }
        &buf[..16] == AE_NAME_LOWER.as_slice()
    }
    fn line(
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        value_rest: &[u8],
    ) -> Result<Option<(usize, usize, usize)>> {
        ae_line(scan, flags, value_rest)
    }
}

fn unknown_fast_skip<V: Visitor>(
    bytes: &[u8],
    colon_idx: usize,
    visitor: &mut V,
    header_count: &mut usize,
    max_header_count: usize,
) -> Result<Option<usize>> {
    let Some(line_end) = BytesScan::find_crlf_from(bytes, colon_idx + 1) else {
        return Ok(None);
    };
    if value_has_forbidden_byte(&bytes[colon_idx + 1..line_end]) {
        return Err(bad_request(ERR_INVALID_HEADER_VALUE));
    }
    if *header_count == max_header_count {
        return Err(bad_request(ERR_TOO_MANY_HEADERS));
    }
    *header_count += 1;
    let name = &bytes[..colon_idx];
    let (vs, ve) = trim_ws_range(bytes, colon_idx + 1, line_end);
    visitor.unknown(name, &bytes[vs..ve])?;
    Ok(Some(line_end))
}

fn ua_tail_matches(rest: &[u8]) -> bool {
    rest.len() >= 11
        && {
            (u32::from_le_bytes(rest[5..9].try_into().unwrap()) | 0x20202020u32)
                == u32::from_le_bytes([b'a', b'g', b'e', b'n'])
        }
        && (rest[9] | 0x20) == b't'
        && rest[10] == b':'
}

pub fn apply_well_known_header_contig<V: Visitor>(
    rest: &[u8],
    scan: &mut codec::HeaderScan,
    flags: &mut Flags,
    visitor: &mut V,
    header_count: &mut usize,
    max_header_count: usize,
) -> Result<Option<usize>> {
    if rest.len() <= MAX_HEADER_LINE_BYTES {
        return apply_well_known_header_contig_inner(
            rest,
            scan,
            flags,
            visitor,
            header_count,
            max_header_count,
        );
    }
    let capped = &rest[..MAX_HEADER_LINE_BYTES];
    match apply_well_known_header_contig_inner(
        capped,
        scan,
        flags,
        visitor,
        header_count,
        max_header_count,
    )? {
        Some(out) => Ok(Some(out)),
        None => Err(bad_request(ERR_HEADER_LINE_TOO_LONG)),
    }
}

fn apply_well_known_header_contig_inner<V: Visitor>(
    rest: &[u8],
    scan: &mut codec::HeaderScan,
    flags: &mut Flags,
    visitor: &mut V,
    header_count: &mut usize,
    max_header_count: usize,
) -> Result<Option<usize>> {
    if rest.is_empty() {
        return Ok(None);
    }
    if rest.len() < 8 {
        return unknown_line(rest, 0, visitor, header_count, max_header_count);
    }
    let probe_word = u64::from_le_bytes(rest[..8].try_into().unwrap());
    let probe_key = probe_word | MATCH_MASK;
    match probe_key {
        PROBE_HOST => {
            HostProbe::dispatch(rest, scan, flags, visitor, header_count, max_header_count)
        }
        PROBE_EXPECT => {
            ExpectProbe::dispatch(rest, scan, flags, visitor, header_count, max_header_count)
        }
        PROBE_CONN => {
            ConnProbe::dispatch(rest, scan, flags, visitor, header_count, max_header_count)
        }
        PROBE_CLEN => {
            ClenProbe::dispatch(rest, scan, flags, visitor, header_count, max_header_count)
        }
        PROBE_TE => TeProbe::dispatch(rest, scan, flags, visitor, header_count, max_header_count),
        PROBE_AE => {
            if rest[6] == b':' {
                unknown_fast_skip(rest, 6, visitor, header_count, max_header_count)
            } else {
                AeProbe::dispatch(rest, scan, flags, visitor, header_count, max_header_count)
            }
        }
        PROBE_UA => {
            if ua_tail_matches(rest) {
                unknown_fast_skip(rest, 10, visitor, header_count, max_header_count)
            } else {
                unknown_line(rest, 0, visitor, header_count, max_header_count)
            }
        }
        _ => unknown_line(rest, 0, visitor, header_count, max_header_count),
    }
}

pub fn apply_well_known_header<I: HeadInput + ?Sized>(
    input: &I,
    line: &[u8],
    line_start: usize,
    colon_idx: usize,
    pretrim_start: Option<usize>,
    pretrim_end: Option<usize>,
    scan: &mut codec::HeaderScan,
    flags: &mut Flags,
    scan_info: Option<&HeaderLineScan>,
) -> Result<()> {
    let _ = input;
    let _ = line_start;
    let _ = scan_info;
    if line.len() > MAX_HEADER_LINE_BYTES {
        return Err(bad_request(ERR_HEADER_LINE_TOO_LONG));
    }
    if colon_idx == 0 {
        return Err(bad_request(ERR_INVALID_HEADER_NAME));
    }
    let name = &line[..colon_idx];
    for &raw in name {
        if !is_header_name_byte(raw) {
            return Err(bad_request(ERR_INVALID_HEADER_NAME));
        }
    }
    enum Action {
        Unknown,
        Host,
        Connection,
        ContentLength,
        TransferEncoding,
        Expect,
        AcceptEncoding,
    }
    let mut action = Action::Unknown;
    match name.len() {
        4 => {
            if name.eq_ignore_ascii_case(b"host") {
                action = Action::Host;
            }
        }
        6 => {
            if name.eq_ignore_ascii_case(b"expect") {
                action = Action::Expect;
            }
        }
        10 => {
            if name.eq_ignore_ascii_case(b"connection") {
                action = Action::Connection;
            }
        }
        14 => {
            if name.eq_ignore_ascii_case(b"content-length") {
                action = Action::ContentLength;
            }
        }
        15 => {
            if name.eq_ignore_ascii_case(b"accept-encoding") {
                action = Action::AcceptEncoding;
            }
        }
        17 => {
            if name.eq_ignore_ascii_case(b"transfer-encoding") {
                action = Action::TransferEncoding;
            }
        }
        _ => {}
    }
    if matches!(action, Action::Unknown) {
        return Ok(());
    }
    let (value_start, value_end) = if let Some(start) = pretrim_start {
        (
            start.min(line.len()),
            pretrim_end.unwrap_or(line.len()).min(line.len()),
        )
    } else {
        trim_ws_range(line, colon_idx + 1, line.len())
    };
    let raw = &line[value_start..value_end];
    match action {
        Action::Host => apply_host(scan, flags),
        Action::Connection => apply_connection(scan, flags, raw),
        Action::ContentLength => apply_content_length(scan, flags, raw),
        Action::TransferEncoding => apply_transfer_encoding(scan, flags, raw),
        Action::Expect => apply_expect(scan, flags, raw),
        Action::AcceptEncoding => apply_accept_encoding(scan, flags, raw),
        Action::Unknown => Ok(()),
    }
}
