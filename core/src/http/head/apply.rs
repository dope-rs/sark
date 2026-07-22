use super::byte::is_ascii_ws;
use super::flags::{Flags, SeenHeaderHandler};
use crate::error::{Error, Result};
use crate::http::codec;
use crate::utils::bytes::{Ascii, Word};

pub const CSV_CLOSE_BIT: u8 = 1 << 0;
pub const CSV_KEEP_ALIVE_BIT: u8 = 1 << 1;
pub const CSV_CHUNKED_BIT: u8 = 1 << 2;
pub const CSV_CONTINUE_BIT: u8 = 1 << 3;

struct ContentLengthHeader;
struct TransferEncodingHeader;
struct ExpectHeader;
struct ConnHeader;
struct HostSeenHeader;

impl SeenHeaderHandler for ContentLengthHeader {
    const SEEN_BIT: u16 = Flags::SEEN_CONTENT_LENGTH;
    const DUPLICATE_ERR: &'static str = "Duplicate header not allowed: ContentLength";
}

impl SeenHeaderHandler for TransferEncodingHeader {
    const SEEN_BIT: u16 = Flags::SEEN_TRANSFER_ENCODING;
    const DUPLICATE_ERR: &'static str = "Duplicate header not allowed: TransferEncoding";
}

impl SeenHeaderHandler for ExpectHeader {
    const SEEN_BIT: u16 = Flags::SEEN_EXPECT;
    const DUPLICATE_ERR: &'static str = "Duplicate header not allowed: Expect";
}

impl SeenHeaderHandler for ConnHeader {
    const SEEN_BIT: u16 = Flags::SEEN_CONNECTION;
    const DUPLICATE_ERR: &'static str = "Duplicate header not allowed: Connection";
}

impl SeenHeaderHandler for HostSeenHeader {
    const SEEN_BIT: u16 = Flags::SEEN_HOST;
    const DUPLICATE_ERR: &'static str = "Duplicate header not allowed: Host";
}

trait KnownHeaderImpl: SeenHeaderHandler + Sized {
    type Parsed;

    fn parse_value(value: &[u8]) -> Result<Self::Parsed>;
    fn parse_line(raw: &[u8]) -> Result<(Self::Parsed, usize)>;
    fn apply_parsed(
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        parsed: Self::Parsed,
    ) -> Result<()>;

    fn dispatch_value(scan: &mut codec::HeaderScan, flags: &mut Flags, value: &[u8]) -> Result<()> {
        flags.mark_seen::<Self>()?;
        let parsed = Self::parse_value(value)?;
        Self::apply_parsed(scan, flags, parsed)
    }

    fn dispatch_line(scan: &mut codec::HeaderScan, flags: &mut Flags, raw: &[u8]) -> Result<usize> {
        flags.mark_seen::<Self>()?;
        let (parsed, value_len) = Self::parse_line(raw)?;
        Self::apply_parsed(scan, flags, parsed)?;
        Ok(value_len)
    }
}

impl KnownHeaderImpl for ContentLengthHeader {
    type Parsed = usize;

    fn parse_value(value: &[u8]) -> Result<usize> {
        Ascii::parse_usize(value).ok_or_else(invalid_content_length)
    }
    fn parse_line(raw: &[u8]) -> Result<(usize, usize)> {
        parse_content_length_line(raw)
    }
    fn apply_parsed(scan: &mut codec::HeaderScan, _flags: &mut Flags, len: usize) -> Result<()> {
        if let Some(existing) = scan.content_length
            && existing != len
        {
            return Err(Error::BadRequest(
                "Multiple Content-Length headers are not allowed".into(),
            ));
        }
        scan.content_length = Some(len);
        Ok(())
    }
}

impl KnownHeaderImpl for TransferEncodingHeader {
    type Parsed = bool;

    fn parse_value(value: &[u8]) -> Result<bool> {
        te_request_chunked_final(value)
    }
    fn parse_line(raw: &[u8]) -> Result<(bool, usize)> {
        match trim_line(raw)? {
            Some((_tail_end, value_start, value_end)) => {
                let is_chunked = te_request_chunked_final(&raw[value_start..value_end])?;
                Ok((is_chunked, value_end - value_start))
            }
            None => {
                let is_chunked = te_request_chunked_final(raw)?;
                Ok((is_chunked, raw.len()))
            }
        }
    }
    fn apply_parsed(
        scan: &mut codec::HeaderScan,
        _flags: &mut Flags,
        is_chunked: bool,
    ) -> Result<()> {
        scan.has_transfer_encoding = true;
        if is_chunked {
            scan.is_chunked_transfer = true;
        }
        Ok(())
    }
}

impl KnownHeaderImpl for ExpectHeader {
    type Parsed = u8;

    fn parse_value(value: &[u8]) -> Result<u8> {
        Ok(parse_csv(value, CSV_CONTINUE_BIT))
    }
    fn parse_line(raw: &[u8]) -> Result<(u8, usize)> {
        Ok(parse_csv_line(raw, CSV_CONTINUE_BIT))
    }
    fn apply_parsed(scan: &mut codec::HeaderScan, _flags: &mut Flags, found: u8) -> Result<()> {
        scan.has_expect = true;
        if (found & CSV_CONTINUE_BIT) != 0 {
            scan.expect_continue = true;
        }
        Ok(())
    }
}

impl KnownHeaderImpl for HostSeenHeader {
    type Parsed = ();

    fn parse_value(_value: &[u8]) -> Result<()> {
        Ok(())
    }
    fn parse_line(raw: &[u8]) -> Result<((), usize)> {
        Ok(((), trim_len(raw)))
    }
    fn apply_parsed(_scan: &mut codec::HeaderScan, flags: &mut Flags, _: ()) -> Result<()> {
        flags.set(Flags::HAS_HOST);
        Ok(())
    }
}

impl KnownHeaderImpl for ConnHeader {
    type Parsed = u8;

    fn parse_value(value: &[u8]) -> Result<u8> {
        Ok(parse_csv(value, CSV_CLOSE_BIT | CSV_KEEP_ALIVE_BIT))
    }
    fn parse_line(raw: &[u8]) -> Result<(u8, usize)> {
        Ok(parse_csv_line(raw, CSV_CLOSE_BIT | CSV_KEEP_ALIVE_BIT))
    }
    fn apply_parsed(_scan: &mut codec::HeaderScan, flags: &mut Flags, found: u8) -> Result<()> {
        if (found & CSV_CLOSE_BIT) != 0 {
            flags.set(Flags::CONNECTION_CLOSE);
        }
        if (found & CSV_KEEP_ALIVE_BIT) != 0 {
            flags.set(Flags::CONNECTION_KEEP_ALIVE);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub enum KnownHeader {
    AcceptEncoding,
    ContentLength,
    TransferEncoding,
    Expect,
    Host,
    Connection,
}

impl KnownHeader {
    pub fn from_name(name: &[u8]) -> Option<Self> {
        match name.len() {
            4 if name.eq_ignore_ascii_case(b"host") => Some(Self::Host),
            6 if name.eq_ignore_ascii_case(b"expect") => Some(Self::Expect),
            10 if name.eq_ignore_ascii_case(b"connection") => Some(Self::Connection),
            14 if name.eq_ignore_ascii_case(b"content-length") => Some(Self::ContentLength),
            15 if name.eq_ignore_ascii_case(b"accept-encoding") => Some(Self::AcceptEncoding),
            17 if name.eq_ignore_ascii_case(b"transfer-encoding") => Some(Self::TransferEncoding),
            _ => None,
        }
    }

    pub fn apply(
        self,
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        value: &[u8],
    ) -> Result<()> {
        match self {
            Self::AcceptEncoding => apply_accept_encoding(scan, flags, value),
            Self::ContentLength => ContentLengthHeader::dispatch_value(scan, flags, value),
            Self::TransferEncoding => TransferEncodingHeader::dispatch_value(scan, flags, value),
            Self::Expect => ExpectHeader::dispatch_value(scan, flags, value),
            Self::Host => HostSeenHeader::dispatch_value(scan, flags, value),
            Self::Connection => ConnHeader::dispatch_value(scan, flags, value),
        }
    }

    pub fn apply_line(
        self,
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        raw: &[u8],
    ) -> Result<usize> {
        match self {
            Self::AcceptEncoding => Ok(ae_line(scan, flags, raw)?
                .map(|(tail_end, _, _)| tail_end)
                .unwrap_or(0)),
            Self::ContentLength => ContentLengthHeader::dispatch_line(scan, flags, raw),
            Self::TransferEncoding => TransferEncodingHeader::dispatch_line(scan, flags, raw),
            Self::Expect => ExpectHeader::dispatch_line(scan, flags, raw),
            Self::Host => HostSeenHeader::dispatch_line(scan, flags, raw),
            Self::Connection => ConnHeader::dispatch_line(scan, flags, raw),
        }
    }

    pub fn scan_line(
        self,
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        raw: &[u8],
    ) -> Result<Option<(usize, usize, usize)>> {
        match self {
            Self::AcceptEncoding => ae_line(scan, flags, raw),
            Self::ContentLength => clen_line(scan, flags, raw),
            Self::TransferEncoding => te_line(scan, flags, raw),
            Self::Expect => expect_line(scan, flags, raw),
            Self::Host => host_line(scan, flags, raw),
            Self::Connection => conn_line(scan, flags, raw),
        }
    }
}

fn apply_accept_encoding(
    scan: &mut codec::HeaderScan,
    _flags: &mut Flags,
    raw: &[u8],
) -> Result<()> {
    let mut start = 0usize;
    while start <= raw.len() {
        let end = raw[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|p| start + p)
            .unwrap_or(raw.len());
        let mut vs = start;
        let mut ve = end;
        while vs < ve && is_ascii_ws(raw[vs]) {
            vs += 1;
        }
        while ve > vs && is_ascii_ws(raw[ve - 1]) {
            ve -= 1;
        }
        if vs < ve {
            let main = ae_token_main(&raw[vs..ve]);
            if main.eq_ignore_ascii_case(b"gzip") {
                scan.accept_encoding_gzip = true;
                return Ok(());
            }
        }
        if end >= raw.len() {
            break;
        }
        start = end + 1;
    }
    Ok(())
}

fn host_line(
    _scan: &mut codec::HeaderScan,
    flags: &mut Flags,
    raw: &[u8],
) -> Result<Option<(usize, usize, usize)>> {
    let Some((tail_end, value_start, value_end)) = trim_line(raw)? else {
        return Ok(None);
    };
    flags.mark_seen::<HostSeenHeader>()?;
    flags.set(Flags::HAS_HOST);
    Ok(Some((tail_end, value_start, value_end)))
}

fn expect_line(
    scan: &mut codec::HeaderScan,
    flags: &mut Flags,
    raw: &[u8],
) -> Result<Option<(usize, usize, usize)>> {
    if let Some(out) = expect_fast_continue(raw, scan, flags)? {
        return Ok(Some(out));
    }
    let Some((found, tail_end, value_start, value_end)) = csv_line(raw, CSV_CONTINUE_BIT)? else {
        return Ok(None);
    };
    flags.mark_seen::<ExpectHeader>()?;
    scan.has_expect = true;
    if (found & CSV_CONTINUE_BIT) != 0 {
        scan.expect_continue = true;
    }
    Ok(Some((tail_end, value_start, value_end)))
}

fn try_token_line_lit<const N: usize>(raw: &[u8], expected: &[u8; N]) -> Option<(usize, usize)> {
    let value_start = if raw.first() == Some(&b' ') { 1 } else { 0 };
    let body_end = value_start + N;
    if raw.len() < body_end + 2 {
        return None;
    }
    if !Word::swar_eq_ci::<N>(&raw[value_start..], expected) {
        return None;
    }
    if raw[body_end] != b'\r' || raw[body_end + 1] != b'\n' {
        return None;
    }
    Some((value_start, body_end))
}

fn expect_fast_continue(
    raw: &[u8],
    scan: &mut codec::HeaderScan,
    flags: &mut Flags,
) -> Result<Option<(usize, usize, usize)>> {
    let Some((value_start, body_end)) = try_token_line_lit(raw, b"100-continue") else {
        return Ok(None);
    };
    flags.mark_seen::<ExpectHeader>()?;
    scan.has_expect = true;
    scan.expect_continue = true;
    Ok(Some((body_end, value_start, body_end)))
}

fn conn_line(
    _scan: &mut codec::HeaderScan,
    flags: &mut Flags,
    raw: &[u8],
) -> Result<Option<(usize, usize, usize)>> {
    if let Some(out) =
        conn_fast_token::<10, { Flags::CONNECTION_KEEP_ALIVE }>(raw, b"keep-alive", flags)?
    {
        return Ok(Some(out));
    }
    if let Some(out) = conn_fast_token::<5, { Flags::CONNECTION_CLOSE }>(raw, b"close", flags)? {
        return Ok(Some(out));
    }
    let Some((found, tail_end, value_start, value_end)) =
        csv_line(raw, CSV_CLOSE_BIT | CSV_KEEP_ALIVE_BIT)?
    else {
        return Ok(None);
    };
    flags.mark_seen::<ConnHeader>()?;
    if (found & CSV_CLOSE_BIT) != 0 {
        flags.set(Flags::CONNECTION_CLOSE);
    }
    if (found & CSV_KEEP_ALIVE_BIT) != 0 {
        flags.set(Flags::CONNECTION_KEEP_ALIVE);
    }
    Ok(Some((tail_end, value_start, value_end)))
}

fn conn_fast_token<const N: usize, const FLAG: u16>(
    raw: &[u8],
    expected: &[u8; N],
    flags: &mut Flags,
) -> Result<Option<(usize, usize, usize)>> {
    let Some((value_start, body_end)) = try_token_line_lit(raw, expected) else {
        return Ok(None);
    };
    flags.mark_seen::<ConnHeader>()?;
    flags.set(FLAG);
    Ok(Some((body_end, value_start, body_end)))
}

fn clen_fast(
    raw: &[u8],
    scan: &mut codec::HeaderScan,
    flags: &mut Flags,
) -> Result<Option<(usize, usize, usize)>> {
    let value_start = if raw.first() == Some(&b' ') { 1 } else { 0 };
    if raw.len() <= value_start + 2 {
        return Ok(None);
    }
    let mut acc: usize = 0;
    let mut idx = value_start;
    while idx < raw.len() {
        let b = raw[idx];
        if !b.is_ascii_digit() {
            break;
        }
        let Some(next) = acc
            .checked_mul(10)
            .and_then(|m| m.checked_add((b - b'0') as usize))
        else {
            return Ok(None);
        };
        acc = next;
        idx += 1;
    }
    if idx == value_start {
        return Ok(None);
    }
    let value_end = idx;
    if value_end + 1 >= raw.len() {
        return Ok(None);
    }
    if raw[value_end] != b'\r' || raw[value_end + 1] != b'\n' {
        return Ok(None);
    }
    flags.mark_seen::<ContentLengthHeader>()?;
    if let Some(existing) = scan.content_length
        && existing != acc
    {
        return Err(Error::BadRequest(
            "Multiple Content-Length headers are not allowed".into(),
        ));
    }
    scan.content_length = Some(acc);
    Ok(Some((value_end, value_start, value_end)))
}

fn clen_line(
    scan: &mut codec::HeaderScan,
    flags: &mut Flags,
    raw: &[u8],
) -> Result<Option<(usize, usize, usize)>> {
    if let Some(out) = clen_fast(raw, scan, flags)? {
        return Ok(Some(out));
    }
    let Some((len, tail_end, value_start, value_end)) = parse_clen(raw)? else {
        return Ok(None);
    };
    flags.mark_seen::<ContentLengthHeader>()?;
    if let Some(existing) = scan.content_length
        && existing != len
    {
        return Err(Error::BadRequest(
            "Multiple Content-Length headers are not allowed".into(),
        ));
    }
    scan.content_length = Some(len);
    Ok(Some((tail_end, value_start, value_end)))
}

fn te_line(
    scan: &mut codec::HeaderScan,
    flags: &mut Flags,
    raw: &[u8],
) -> Result<Option<(usize, usize, usize)>> {
    if let Some(out) = te_fast_chunked(raw, scan, flags)? {
        return Ok(Some(out));
    }
    let Some((tail_end, value_start, value_end)) = trim_line(raw)? else {
        return Ok(None);
    };
    flags.mark_seen::<TransferEncodingHeader>()?;
    scan.has_transfer_encoding = true;
    if te_request_chunked_final(&raw[value_start..value_end])? {
        scan.is_chunked_transfer = true;
    }
    Ok(Some((tail_end, value_start, value_end)))
}

fn te_invalid_transfer_encoding() -> Error {
    Error::BadRequest("Invalid Transfer-Encoding".into())
}

fn te_invalid_ordering() -> Error {
    Error::BadRequest("Invalid Transfer-Encoding ordering".into())
}

fn te_request_chunked_final(value: &[u8]) -> Result<bool> {
    let mut saw_chunked = false;
    let mut last_is_chunked = false;
    let mut start = 0usize;
    let mut idx = 0usize;
    let len = value.len();
    loop {
        if idx == len || value[idx] == b',' {
            let mut lo = start;
            let mut hi = idx;
            while lo < hi && is_ascii_ws(value[lo]) {
                lo += 1;
            }
            while hi > lo && is_ascii_ws(value[hi - 1]) {
                hi -= 1;
            }
            if hi <= lo {
                return Err(te_invalid_transfer_encoding());
            }
            let token = &value[lo..hi];
            if !token.is_ascii() {
                return Err(te_invalid_transfer_encoding());
            }
            let is_chunked = token_eq_ci(token, b"chunked");
            if saw_chunked && (!last_is_chunked || !is_chunked) {
                return Err(te_invalid_ordering());
            }
            if is_chunked {
                saw_chunked = true;
            }
            last_is_chunked = is_chunked;
            if idx == len {
                break;
            }
            start = idx + 1;
        }
        idx += 1;
    }
    Ok(saw_chunked && last_is_chunked)
}

fn te_fast_chunked(
    raw: &[u8],
    scan: &mut codec::HeaderScan,
    flags: &mut Flags,
) -> Result<Option<(usize, usize, usize)>> {
    let Some((value_start, body_end)) = try_token_line_lit(raw, b"chunked") else {
        return Ok(None);
    };
    flags.mark_seen::<TransferEncodingHeader>()?;
    scan.has_transfer_encoding = true;
    scan.is_chunked_transfer = true;
    Ok(Some((body_end, value_start, body_end)))
}

fn ae_line(
    scan: &mut codec::HeaderScan,
    _flags: &mut Flags,
    raw: &[u8],
) -> Result<Option<(usize, usize, usize)>> {
    let Some((found, tail_end, value_start, value_end)) = ae_csv_line(raw)? else {
        return Ok(None);
    };
    if found {
        scan.accept_encoding_gzip = true;
    }
    Ok(Some((tail_end, value_start, value_end)))
}

fn ae_csv_line(raw: &[u8]) -> Result<Option<(bool, usize, usize, usize)>> {
    walk_csv_line(raw, false, |found, token| {
        found || token_eq_ci(ae_token_main(token), b"gzip")
    })
}

fn ae_token_main(token: &[u8]) -> &[u8] {
    let mut end = 0usize;
    while end < token.len() {
        let b = token[end];
        if b == b';' || is_ascii_ws(b) {
            break;
        }
        end += 1;
    }
    &token[..end]
}

fn invalid_content_length() -> Error {
    Error::BadRequest("Invalid Content-Length".into())
}

fn invalid_header_value() -> Error {
    Error::BadRequest("Invalid header value".into())
}

fn scan_content_length_value(raw: &[u8]) -> Result<Option<(usize, usize, usize)>> {
    let mut idx = 0usize;
    while idx < raw.len() && is_ascii_ws(raw[idx]) {
        idx += 1;
    }
    let start = idx;
    if start == raw.len() {
        return Ok(None);
    }
    while idx < raw.len() && raw[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == start {
        return Err(invalid_content_length());
    }
    let end = idx;
    let value = Ascii::parse_usize(&raw[start..end]).ok_or_else(invalid_content_length)?;
    Ok(Some((value, start, end)))
}

struct CsvScan<'a> {
    raw: &'a [u8],
    pos: usize,
}

impl<'a> CsvScan<'a> {
    fn new(raw: &'a [u8]) -> Self {
        Self { raw, pos: 0 }
    }
}

impl<'a> Iterator for CsvScan<'a> {
    type Item = (&'a [u8], usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos > self.raw.len() {
            return None;
        }
        while self.pos < self.raw.len() && is_ascii_ws(self.raw[self.pos]) {
            self.pos += 1;
        }
        let start = self.pos;
        while self.pos < self.raw.len() && self.raw[self.pos] != b',' {
            self.pos += 1;
        }
        let end = self.pos;
        let mut stop = end;
        while stop > start && is_ascii_ws(self.raw[stop - 1]) {
            stop -= 1;
        }
        self.pos = end.saturating_add(1);
        if start > end {
            return None;
        }
        Some((&self.raw[start..stop], stop))
    }
}

fn parse_csv(raw: &[u8], want: u8) -> u8 {
    let mut found = 0u8;
    for (token, _) in CsvScan::new(raw) {
        if found == want {
            break;
        }
        found |= match_csv_token(token, want);
    }
    found
}

fn trim_len(raw: &[u8]) -> usize {
    let mut start = 0usize;
    while start < raw.len() && is_ascii_ws(raw[start]) {
        start += 1;
    }
    let mut end = raw.len();
    while end > start && is_ascii_ws(raw[end - 1]) {
        end -= 1;
    }
    end - start
}

fn trim_line(raw: &[u8]) -> Result<Option<(usize, usize, usize)>> {
    let mut idx = 0usize;
    while idx < raw.len() && is_ascii_ws(raw[idx]) {
        idx += 1;
    }
    let value_start = idx;
    let Some(rel) = memchr::memchr2(b'\r', b'\n', &raw[value_start..]) else {
        return Ok(None);
    };
    let cr = value_start + rel;
    if raw[cr] == b'\n' {
        return Err(invalid_header_value());
    }
    if cr + 1 >= raw.len() {
        return Ok(None);
    }
    if raw[cr + 1] != b'\n' {
        return Err(invalid_header_value());
    }
    let mut value_end = cr;
    while value_end > value_start && is_ascii_ws(raw[value_end - 1]) {
        value_end -= 1;
    }
    Ok(Some((cr, value_start, value_end)))
}

fn parse_content_length_line(raw: &[u8]) -> Result<(usize, usize)> {
    let (value, start, end) = scan_content_length_value(raw)?.ok_or_else(invalid_content_length)?;
    let mut idx = end;
    while idx < raw.len() && is_ascii_ws(raw[idx]) {
        idx += 1;
    }
    if idx != raw.len() {
        return Err(invalid_content_length());
    }
    Ok((value, end - start))
}

fn parse_clen(raw: &[u8]) -> Result<Option<(usize, usize, usize, usize)>> {
    let Some((value, value_start, value_end)) = scan_content_length_value(raw)? else {
        return Ok(None);
    };
    let mut idx = value_end;
    while idx < raw.len() && is_ascii_ws(raw[idx]) {
        idx += 1;
    }
    if idx >= raw.len() {
        return Ok(None);
    }
    if raw[idx] != b'\r' {
        return Err(invalid_content_length());
    }
    if idx + 1 >= raw.len() {
        return Ok(None);
    }
    if raw[idx + 1] != b'\n' {
        return Err(invalid_content_length());
    }
    Ok(Some((value, idx, value_start, value_end)))
}

fn trim_left(raw: &[u8]) -> usize {
    let mut start = 0usize;
    while start < raw.len() && is_ascii_ws(raw[start]) {
        start += 1;
    }
    start
}

fn token_eq_ci<const N: usize>(token: &[u8], expected: &[u8; N]) -> bool {
    token.len() == N && Word::swar_eq_ci::<N>(token, expected)
}

fn match_csv_token(token: &[u8], want: u8) -> u8 {
    if (want & CSV_CLOSE_BIT) != 0 && token_eq_ci(token, b"close") {
        return CSV_CLOSE_BIT;
    }
    if (want & CSV_KEEP_ALIVE_BIT) != 0 && token_eq_ci(token, b"keep-alive") {
        return CSV_KEEP_ALIVE_BIT;
    }
    if (want & CSV_CHUNKED_BIT) != 0 && token_eq_ci(token, b"chunked") {
        return CSV_CHUNKED_BIT;
    }
    if (want & CSV_CONTINUE_BIT) != 0 && token_eq_ci(token, b"100-continue") {
        return CSV_CONTINUE_BIT;
    }
    0
}

fn parse_csv_line(raw: &[u8], want: u8) -> (u8, usize) {
    let mut found = 0u8;
    let mut value_len = 0usize;
    for (token, stop) in CsvScan::new(raw) {
        if found == want {
            break;
        }
        if !token.is_empty() {
            value_len = stop;
            found |= match_csv_token(token, want);
        }
    }
    (found, value_len.saturating_sub(trim_left(raw)))
}

fn csv_line(raw: &[u8], want: u8) -> Result<Option<(u8, usize, usize, usize)>> {
    walk_csv_line(raw, 0u8, |found, token| {
        found | match_csv_token(token, want)
    })
}

fn walk_csv_line<A: Copy, F: FnMut(A, &[u8]) -> A>(
    raw: &[u8],
    init: A,
    mut fold: F,
) -> Result<Option<(A, usize, usize, usize)>> {
    let mut acc = init;
    let mut idx = 0usize;
    let mut first_non_ws: Option<usize> = None;
    let mut last_non_ws = 0usize;
    loop {
        let Some(rel) = memchr::memchr3(b',', b'\r', b'\n', &raw[idx..]) else {
            return Ok(None);
        };
        let end = idx + rel;
        if raw[end] == b'\n' {
            return Err(invalid_header_value());
        }
        let mut t_lo = idx;
        while t_lo < end && is_ascii_ws(raw[t_lo]) {
            t_lo += 1;
        }
        let mut t_hi = end;
        while t_hi > t_lo && is_ascii_ws(raw[t_hi - 1]) {
            t_hi -= 1;
        }
        if t_hi > t_lo {
            if first_non_ws.is_none() {
                first_non_ws = Some(t_lo);
            }
            last_non_ws = t_hi;
            acc = fold(acc, &raw[t_lo..t_hi]);
        }
        if raw[end] == b'\r' {
            if end + 1 >= raw.len() {
                return Ok(None);
            }
            if raw[end + 1] != b'\n' {
                return Err(invalid_header_value());
            }
            let value_start = first_non_ws.unwrap_or(0);
            let value_end = if first_non_ws.is_some() {
                last_non_ws
            } else {
                value_start
            };
            return Ok(Some((acc, end, value_start, value_end)));
        }
        idx = end + 1;
    }
}
