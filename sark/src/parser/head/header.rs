use sark_core::error::Result;
use sark_core::http::codec;
use sark_core::http::head::{
    ERR_INVALID_HEADER_NAME, Flags, HeadInput, HeaderLineScan, KnownHeader, bad_request,
    is_ascii_ws, is_header_name_byte,
};

use crate::service::HeadParts;

pub struct HeaderApply;

impl HeaderApply {
    #[allow(clippy::too_many_arguments)]
    pub fn generic<I, K, P>(
        input: &I,
        line: &[u8],
        line_start: usize,
        colon_idx: usize,
        pretrim_start: Option<usize>,
        pretrim_end: Option<usize>,
        parts: &mut P,
        scan: &mut codec::HeaderScan,
        flags: &mut Flags,
        scan_info: Option<&HeaderLineScan>,
    ) -> Result<()>
    where
        I: HeadInput + ?Sized,
        P: HeadParts<K>,
    {
        if colon_idx == 0 {
            return Err(bad_request(ERR_INVALID_HEADER_NAME));
        }
        let _ = scan_info;
        let name = &line[..colon_idx];
        for &b in name {
            if !is_header_name_byte(b) {
                return Err(bad_request(ERR_INVALID_HEADER_NAME));
            }
        }
        let known_header = KnownHeader::from_name(name);
        if let Some(header) = known_header
            && !P::NEED_KNOWN_HEADER
        {
            header.apply_line(scan, flags, &line[colon_idx + 1..])?;
            return Ok(());
        }
        let mut value_start = colon_idx + 1;
        let mut value_end = line.len();
        if let Some(start) = pretrim_start {
            value_start = start.min(line.len());
            value_end = pretrim_end.unwrap_or(line.len()).min(line.len());
        } else {
            while value_start < line.len() && is_ascii_ws(line[value_start]) {
                value_start += 1;
            }
            while value_end > value_start && is_ascii_ws(line[value_end - 1]) {
                value_end -= 1;
            }
        }
        let raw = &line[value_start..value_end];
        match known_header {
            Some(header) => {
                header.apply(scan, flags, raw)?;
            }
            None => {
                let abs = (line_start + value_start)..(line_start + value_end);
                let value = super::value::InputHeaderValue::new(input, abs);
                parts.set_header_name(name, &value)?;
            }
        }
        Ok(())
    }
}
