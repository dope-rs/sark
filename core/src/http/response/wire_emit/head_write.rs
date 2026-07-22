use super::WireWriter;
use super::consts::{CRLF, SERVER_DATE_TERMINATOR_LEN, STATUS_LINE_PREFIX};
use super::framing::Framing;
use super::headers::HeaderSection;

pub(in crate::http::response) struct WrittenHead {
    pub(in crate::http::response) len: usize,
    pub(in crate::http::response) date_offset: usize,
}

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

    pub(in crate::http::response) fn write(&self, out: &mut [u8], date: &[u8; 29]) -> WrittenHead {
        let mut out = WireWriter::new(out);
        out.put_status_line(self.status_str, self.reason);
        self.headers.write_headers(&mut out);
        self.framing.write_framing(&mut out);
        let date_offset = out.put_server_date_terminator(date);
        WrittenHead {
            len: out.len(),
            date_offset,
        }
    }
}
