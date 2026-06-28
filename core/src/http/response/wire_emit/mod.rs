mod consts;
mod framing;
mod head_write;
mod headers;
mod out;

pub(super) use consts::{CL_PREFIX, CRLF, SERVER_DATE_TERMINATOR_LEN, STATUS_LINE_PREFIX};
pub(in crate::http::response) use consts::{DATE_LEN, NO_DATE};
pub(super) use framing::{ContentLength, TransferEncodingChunked};
pub(super) use head_write::HeadWrite;
pub(super) use headers::HeaderSection;
pub(super) use out::Out;

/// Rewrites the cached head template's `Server`/`Date` terminator in place to
/// honor a route's `#[skip(...)]` policy. The template is built once and cached,
/// so this runs once per route — not per request.
///
/// Argument order matches the rest of the codebase: `(date, server)`. Returns
/// the date-placeholder offset to patch each request, or [`NO_DATE`] when
/// `emit_date` is false.
pub fn apply_head_skip(
    template: &mut Vec<u8>,
    date_offset: usize,
    emit_date: bool,
    emit_server: bool,
) -> usize {
    if emit_date && emit_server {
        return date_offset;
    }
    // Tail layout from `Out::put_server_date_terminator`:
    //   SERVER_LINE | DATE_PREFIX | <DATE_LEN date> | CRLF | CRLF
    let term_start = date_offset - consts::DATE_PREFIX.len() - consts::SERVER_LINE.len();
    let mut tail = Vec::with_capacity(SERVER_DATE_TERMINATOR_LEN);
    if emit_server {
        tail.extend_from_slice(consts::SERVER_LINE);
    }
    let new_offset = if emit_date {
        tail.extend_from_slice(consts::DATE_PREFIX);
        let off = term_start + tail.len();
        tail.extend_from_slice(&[0u8; DATE_LEN]);
        tail.extend_from_slice(CRLF);
        off
    } else {
        NO_DATE
    };
    tail.extend_from_slice(CRLF);
    template.truncate(term_start);
    template.extend_from_slice(&tail);
    new_offset
}
