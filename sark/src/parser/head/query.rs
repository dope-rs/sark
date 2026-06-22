use sark_core::error::Result;

use super::value::InputHeaderValue;
use crate::service::HeadParts;

pub(crate) fn parse_query_fields_input<K, P>(
    input: &[u8],
    range: std::ops::Range<usize>,
    parts: &mut P,
) -> Result<()>
where
    P: HeadParts<K>,
{
    if range.start >= range.end {
        return Ok(());
    }
    let bytes = &input[range.clone()];
    let mut seg_start = 0usize;
    let mut eq_idx: Option<usize> = None;
    let mut idx = 0usize;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b'=' {
            if eq_idx.is_none() {
                eq_idx = Some(idx);
            }
            idx += 1;
            continue;
        }
        if b == b'&' {
            if seg_start < idx {
                let key_end = eq_idx.unwrap_or(idx);
                let value_start = eq_idx.map_or(idx, |eq| eq.saturating_add(1));
                let value =
                    InputHeaderValue::new(input, (range.start + value_start)..(range.start + idx));
                let name = &bytes[seg_start..key_end];
                parts.set_query_name(name, &value)?;
            }
            seg_start = idx + 1;
            eq_idx = None;
        }
        idx += 1;
    }
    if seg_start < bytes.len() {
        let key_end = eq_idx.unwrap_or(bytes.len());
        let value_start = eq_idx.map_or(bytes.len(), |eq| eq.saturating_add(1));
        let value = InputHeaderValue::new(input, (range.start + value_start)..range.end);
        let name = &bytes[seg_start..key_end];
        parts.set_query_name(name, &value)?;
    }
    Ok(())
}
