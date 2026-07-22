mod consts;
mod framing;
mod head_write;
mod headers;
mod out;

pub(super) use consts::{CRLF, SERVER_DATE_TERMINATOR_LEN};
pub(in crate::http::response) use consts::{DATE_LEN, PLACEHOLDER_DATE};
pub(super) use framing::{ContentLength, TransferEncodingChunked};
pub(super) use head_write::HeadWrite;
pub(super) use headers::HeaderSection;
pub(super) use out::Out;

pub fn apply_head_skip(
    template: &mut Vec<u8>,
    date_offset: usize,
    emit_date: bool,
    emit_server: bool,
) -> Option<usize> {
    if emit_date && emit_server {
        return Some(date_offset);
    }
    let term_start = date_offset - consts::DATE_PREFIX.len() - consts::SERVER_LINE.len();
    let term_end = term_start + SERVER_DATE_TERMINATOR_LEN;
    let mut tail = Vec::with_capacity(SERVER_DATE_TERMINATOR_LEN);
    if emit_server {
        tail.extend_from_slice(consts::SERVER_LINE);
    }
    let new_offset = if emit_date {
        tail.extend_from_slice(consts::DATE_PREFIX);
        let off = term_start + tail.len();
        tail.extend_from_slice(&[0u8; DATE_LEN]);
        tail.extend_from_slice(CRLF);
        Some(off)
    } else {
        None
    };
    tail.extend_from_slice(CRLF);
    template.splice(term_start..term_end, tail);
    new_offset
}
