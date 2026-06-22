pub(super) const SERVER_LINE: &[u8] = b"Server: sark\r\n";
pub(super) const DATE_PREFIX: &[u8] = b"Date: ";
pub(in crate::http::response) const CRLF: &[u8] = b"\r\n";
pub(in crate::http::response) const CL_PREFIX: &[u8] = b"Content-Length: ";
pub(super) const TE_LINE: &[u8] = b"Transfer-Encoding: chunked\r\n";

pub(in crate::http::response) const STATUS_LINE_PREFIX: &[u8] = b"HTTP/1.1 ";

pub(in crate::http::response) const SERVER_DATE_TERMINATOR_LEN: usize =
    SERVER_LINE.len() + DATE_PREFIX.len() + 29 + CRLF.len() + CRLF.len();
