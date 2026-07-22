use std::cell::OnceCell;

use o3::buffer::{Bytes, Retained};
use sark_core::error::Result;
use sark_core::http::head::{HeadInput, HeaderLine};

use crate::service::HeaderValue;

pub(super) struct InputHeaderValue<'a, I: HeadInput + ?Sized> {
    input: &'a I,
    start: usize,
    end: usize,
    frame: OnceCell<Bytes<Retained>>,
}

impl<'a, I: HeadInput + ?Sized> InputHeaderValue<'a, I> {
    pub(super) fn new(input: &'a I, range: std::ops::Range<usize>) -> Self {
        Self {
            input,
            start: range.start,
            end: range.end,
            frame: OnceCell::new(),
        }
    }

    fn span(&self) -> std::ops::Range<usize> {
        self.start..self.end
    }

    fn frame(&self) -> &Bytes<Retained> {
        self.frame
            .get_or_init(|| match self.input.copy_range_frame(self.span()) {
                Some(v) => v,
                None => {
                    debug_assert!(
                        false,
                        "header value invariant: input range must be readable"
                    );
                    Bytes::<Retained>::copy_from_slice(b"")
                }
            })
    }

    fn eq_input(&self, expected: &[u8], ignore_ascii_case: bool) -> bool {
        let range = self.span();
        if expected.len() != range.end.saturating_sub(range.start) {
            return false;
        }
        if let Some(bytes) = self.input.slice_range(range.clone()) {
            return if ignore_ascii_case {
                bytes.eq_ignore_ascii_case(expected)
            } else {
                bytes == expected
            };
        }
        let mut matched = true;
        let mut off = 0usize;
        self.input.for_each_slice(range, |chunk| {
            if !matched {
                return;
            }
            let end = off + chunk.len();
            let want = &expected[off..end];
            matched = if ignore_ascii_case {
                chunk.eq_ignore_ascii_case(want)
            } else {
                chunk == want
            };
            off = end;
        });
        matched && off == expected.len()
    }

    fn parse_decimal_u64(&self) -> Result<u64> {
        let range = self.span();
        if range.start >= range.end {
            return Err(sark_core::error::Error::invalid_integer_header());
        }
        let mut saw_digit = false;
        let mut trailing_ws = false;
        let mut out = 0u64;
        let mut invalid = None;
        self.input.for_each_slice(range, |bytes| {
            if invalid.is_some() {
                return;
            }
            for &b in bytes {
                if HeaderLine::is_whitespace(b) {
                    if saw_digit {
                        trailing_ws = true;
                    }
                    continue;
                }
                if !b.is_ascii_digit() || trailing_ws {
                    invalid = Some(sark_core::error::Error::invalid_integer_header());
                    return;
                }
                saw_digit = true;
                let digit = u64::from(b - b'0');
                out = match out.checked_mul(10).and_then(|v| v.checked_add(digit)) {
                    Some(v) => v,
                    None => {
                        invalid = Some(sark_core::error::Error::invalid_integer_header());
                        return;
                    }
                };
            }
        });
        if let Some(err) = invalid {
            return Err(err);
        }
        if !saw_digit {
            return Err(sark_core::error::Error::invalid_integer_header());
        }
        Ok(out)
    }
}

impl<I: HeadInput + ?Sized> HeaderValue for InputHeaderValue<'_, I> {
    fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    fn eq_bytes(&self, expected: &[u8]) -> bool {
        self.eq_input(expected, false)
    }

    fn eq_ignore_ascii_case(&self, expected: &[u8]) -> bool {
        self.eq_input(expected, true)
    }

    fn as_range(&self) -> std::ops::Range<usize> {
        self.span()
    }

    fn copy_frame(&self) -> sark_core::http::Bytes<Retained> {
        if let Some(bytes) = self.input.slice_range(self.span()) {
            return Bytes::<Retained>::copy_from_slice(bytes);
        }
        self.frame().clone()
    }

    fn parse_usize(&self) -> Result<usize> {
        let out = self.parse_decimal_u64()?;
        usize::try_from(out).map_err(|_| sark_core::error::Error::invalid_integer_header())
    }

    fn parse_u64(&self) -> Result<u64> {
        self.parse_decimal_u64()
    }
}
