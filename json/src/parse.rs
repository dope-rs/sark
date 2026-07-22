use o3::buffer::{Bytes, Retained, Shared};

use crate::Result;
use crate::body::InlineToken;
use crate::error::Fail;
use crate::scan::Scan;

pub struct Parse;

impl Parse {
    pub fn frame(owner: &Shared, idx: &mut usize) -> Result<Bytes<Retained>> {
        let input = owner.as_slice();
        if *idx >= input.len() {
            return Err(Fail::bad());
        }
        if input[*idx] == b'"' {
            let start = *idx;
            let _ = Scan::str_slice(input, idx)?;
            let raw_start = start.saturating_add(1);
            let raw_end = idx.saturating_sub(1);
            if raw_end < raw_start {
                return Err(Fail::bad());
            }
            let mut esc = raw_start;
            while esc < raw_end {
                if input[esc] == b'\\' {
                    let decoded = Shared::from(Scan::decode_str(&input[raw_start..raw_end])?);
                    return Ok(Bytes::<Retained>::from(decoded));
                }
                esc += 1;
            }
            return Ok(Bytes::<Retained>::from(owner.slice(raw_start..raw_end)));
        }
        let start = *idx;
        Scan::skip_value(input, idx)?;
        let end = *idx;
        if end <= start {
            return Err(Fail::bad());
        }
        Ok(Bytes::<Retained>::from(owner.slice(start..end)))
    }

    pub fn empty_frame() -> Bytes<Retained> {
        Bytes::<Retained>::from(Shared::new())
    }

    pub fn frame_plain(owner: &Shared, idx: &mut usize) -> Result<Bytes<Retained>> {
        let input = owner.as_slice();
        Scan::expect_byte(input, idx, b'"')?;
        let start = *idx;
        while *idx < input.len() {
            match input[*idx] {
                b'"' => {
                    let end = *idx;
                    *idx += 1;
                    return Ok(Bytes::<Retained>::from(owner.slice(start..end)));
                }
                b'\\' => return Err(Fail::bad()),
                _ => *idx += 1,
            }
        }
        Err(Fail::bad())
    }

    pub fn inline_plain<const N: usize>(input: &[u8], idx: &mut usize) -> Result<InlineToken<N>> {
        Scan::expect_byte(input, idx, b'"')?;
        let mut out = InlineToken::<N>::new();
        while *idx < input.len() {
            match input[*idx] {
                b'"' => {
                    *idx += 1;
                    return Ok(out);
                }
                b'\\' => return Err(Fail::bad()),
                b => {
                    out.push(b)?;
                    *idx += 1;
                }
            }
        }
        Err(Fail::bad())
    }

    pub fn frame_raw(owner: &Shared, idx: &mut usize) -> Result<Bytes<Retained>> {
        let input = owner.as_slice();
        let start = *idx;
        while *idx < input.len() {
            match input[*idx] {
                b',' | b'}' | b']' => break,
                _ => *idx += 1,
            }
        }
        let end = *idx;
        if end <= start {
            return Err(Fail::bad());
        }
        Ok(Bytes::<Retained>::from(owner.slice(start..end)))
    }

    pub fn inline_raw<const N: usize>(input: &[u8], idx: &mut usize) -> Result<InlineToken<N>> {
        let mut out = InlineToken::<N>::new();
        while *idx < input.len() {
            match input[*idx] {
                b',' | b'}' | b']' => break,
                b => {
                    out.push(b)?;
                    *idx += 1;
                }
            }
        }
        if out.is_empty() {
            return Err(Fail::bad());
        }
        Ok(out)
    }

    pub fn u64(input: &[u8], idx: &mut usize) -> Result<u64> {
        if *idx >= input.len() {
            return Err(Fail::bad());
        }
        let mut value = 0u64;
        let mut seen = false;
        while *idx < input.len() {
            let b = input[*idx];
            if !b.is_ascii_digit() {
                break;
            }
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add((b - b'0') as u64))
                .ok_or_else(Fail::bad)?;
            *idx += 1;
            seen = true;
        }
        if !seen {
            return Err(Fail::bad());
        }
        Ok(value)
    }

    pub fn bool(input: &[u8], idx: &mut usize) -> Result<bool> {
        if input.get(*idx..(*idx + 4)) == Some(b"true") {
            *idx += 4;
            return Ok(true);
        }
        if input.get(*idx..(*idx + 5)) == Some(b"false") {
            *idx += 5;
            return Ok(false);
        }
        Err(Fail::bad())
    }
}
