use super::consts::{CRLF, SERVER_DATE_TERMINATOR_LEN, STATUS_LINE_PREFIX};
use super::framing::Framing;
use super::headers::HeaderSection;
use super::out::Out;

pub(in crate::http::response) struct HeadWrite<'a, H, F>
where
    H: HeaderSection + ?Sized,
    F: Framing,
{
    pub(in crate::http::response) status_str: &'a [u8],
    pub(in crate::http::response) reason: &'a [u8],
    pub(in crate::http::response) headers: &'a H,
    pub(in crate::http::response) framing: F,
}

impl<'a, H, F> HeadWrite<'a, H, F>
where
    H: HeaderSection + ?Sized,
    F: Framing,
{
    pub(in crate::http::response) fn wire_len(&self) -> usize {
        STATUS_LINE_PREFIX.len()
            + self.status_str.len()
            + 1
            + self.reason.len()
            + CRLF.len()
            + self.headers.header_len()
            + self.framing.framing_len()
            + SERVER_DATE_TERMINATOR_LEN
    }

    pub(in crate::http::response) fn write(
        &self,
        out: &mut [u8],
        off: &mut usize,
        date: &[u8; 29],
    ) {
        Out::put_status_line(out, off, self.status_str, self.reason);
        self.headers.write_headers(out, off);
        self.framing.write_framing(out, off);
        Out::put_server_date_terminator(out, off, date);
    }
}
