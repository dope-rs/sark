use crate::Result;
use crate::error::Fail;

pub struct Scan;

impl Scan {
    pub fn ws(input: &[u8], idx: &mut usize) {
        while *idx < input.len() && input[*idx].is_ascii_whitespace() {
            *idx += 1;
        }
    }

    pub fn eat_byte(input: &[u8], idx: &mut usize, want: u8) -> bool {
        if *idx < input.len() && input[*idx] == want {
            *idx += 1;
            true
        } else {
            false
        }
    }

    pub fn expect_byte(input: &[u8], idx: &mut usize, want: u8) -> Result<()> {
        if Self::eat_byte(input, idx, want) {
            return Ok(());
        }
        Err(Fail::bad())
    }

    pub fn expect_prop(input: &[u8], idx: &mut usize, first: bool, want: &[u8]) -> Result<()> {
        Self::ws(input, idx);
        if !first {
            Self::expect_byte(input, idx, b',')?;
            Self::ws(input, idx);
        }
        Self::expect_byte(input, idx, b'"')?;
        let end = idx.saturating_add(want.len());
        if input.get(*idx..end) != Some(want) {
            return Err(Fail::bad());
        }
        *idx = end;
        Self::expect_byte(input, idx, b'"')?;
        Self::ws(input, idx);
        Self::expect_byte(input, idx, b':')?;
        Self::ws(input, idx);
        Ok(())
    }

    pub fn seek_name(input: &[u8], idx: &mut usize, want: &[u8]) -> Result<()> {
        let need = want.len() + 3;
        while idx.saturating_add(need) <= input.len() {
            if input[*idx] == b'"' {
                let key_start = idx.saturating_add(1);
                let key_end = key_start.saturating_add(want.len());
                if input.get(key_start..key_end) == Some(want)
                    && input.get(key_end) == Some(&b'"')
                    && input.get(key_end + 1) == Some(&b':')
                {
                    *idx = key_end + 2;
                    return Ok(());
                }
            }
            *idx += 1;
        }
        Err(Fail::bad())
    }

    pub fn eat_null(input: &[u8], idx: &mut usize) -> bool {
        if input.get(*idx..(*idx + 4)) == Some(b"null") {
            *idx += 4;
            true
        } else {
            false
        }
    }

    pub fn skip_plain_string(input: &[u8], idx: &mut usize) -> Result<()> {
        Self::expect_byte(input, idx, b'"')?;
        while *idx < input.len() {
            match input[*idx] {
                b'"' => {
                    *idx += 1;
                    return Ok(());
                }
                b'\\' => return Err(Fail::bad()),
                _ => *idx += 1,
            }
        }
        Err(Fail::bad())
    }

    pub fn skip_value(input: &[u8], idx: &mut usize) -> Result<()> {
        Self::ws(input, idx);
        if *idx >= input.len() {
            return Err(Fail::bad());
        }
        match input[*idx] {
            b'"' => {
                let _ = Self::str_slice(input, idx)?;
                Ok(())
            }
            b'{' => Self::skip_group(input, idx, b'{', b'}'),
            b'[' => Self::skip_group(input, idx, b'[', b']'),
            b't' => {
                let _ = crate::parse::Parse::bool(input, idx)?;
                Ok(())
            }
            b'f' => {
                let _ = crate::parse::Parse::bool(input, idx)?;
                Ok(())
            }
            b'n' => {
                if input.get(*idx..(*idx + 4)) == Some(b"null") {
                    *idx += 4;
                    Ok(())
                } else {
                    Err(Fail::bad())
                }
            }
            b'-' | b'0'..=b'9' => Self::skip_number(input, idx),
            _ => Err(Fail::bad()),
        }
    }

    fn skip_number(input: &[u8], idx: &mut usize) -> Result<()> {
        let start = *idx;
        let _ = Self::eat_byte(input, idx, b'-');
        if !Self::skip_digits(input, idx) {
            return Err(Fail::bad());
        }
        if Self::eat_byte(input, idx, b'.') && !Self::skip_digits(input, idx) {
            return Err(Fail::bad());
        }
        if *idx < input.len() && (input[*idx] == b'e' || input[*idx] == b'E') {
            *idx += 1;
            if *idx < input.len() && (input[*idx] == b'+' || input[*idx] == b'-') {
                *idx += 1;
            }
            if !Self::skip_digits(input, idx) {
                return Err(Fail::bad());
            }
        }
        if *idx == start {
            return Err(Fail::bad());
        }
        Ok(())
    }

    fn skip_digits(input: &[u8], idx: &mut usize) -> bool {
        let start = *idx;
        while *idx < input.len() && input[*idx].is_ascii_digit() {
            *idx += 1;
        }
        *idx != start
    }

    pub fn str_slice<'a>(input: &'a [u8], idx: &mut usize) -> Result<&'a [u8]> {
        Self::expect_byte(input, idx, b'"')?;
        let start = *idx;
        while *idx < input.len() {
            match input[*idx] {
                b'\\' => {
                    *idx += 2;
                }
                b'"' => {
                    let end = *idx;
                    *idx += 1;
                    return Ok(&input[start..end]);
                }
                _ => {
                    *idx += 1;
                }
            }
        }
        Err(Fail::bad())
    }

    pub(super) fn decode_str(input: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(input.len());
        let mut idx = 0usize;
        while idx < input.len() {
            match input[idx] {
                b'\\' => {
                    idx += 1;
                    if idx >= input.len() {
                        return Err(Fail::bad());
                    }
                    match input[idx] {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(0x08),
                        b'f' => out.push(0x0c),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        _ => return Err(Fail::bad()),
                    }
                }
                b => out.push(b),
            }
            idx += 1;
        }
        Ok(out)
    }

    fn skip_group(input: &[u8], idx: &mut usize, open: u8, close: u8) -> Result<()> {
        Self::expect_byte(input, idx, open)?;
        let mut depth = 1usize;
        while *idx < input.len() {
            match input[*idx] {
                b'"' => {
                    let _ = Self::str_slice(input, idx)?;
                }
                b if b == open => {
                    depth += 1;
                    *idx += 1;
                }
                b if b == close => {
                    depth -= 1;
                    *idx += 1;
                    if depth == 0 {
                        return Ok(());
                    }
                }
                _ => {
                    *idx += 1;
                }
            }
        }
        Err(Fail::bad())
    }
}
