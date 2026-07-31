use http::StatusCode;

use super::WireWriter;
use super::consts::SERVER_DATE_TERMINATOR_LEN;
use super::framing::Framing;
use super::headers::HeaderSection;
use super::status_line_len;

pub(in crate::http::response) struct WrittenHead {
    pub(in crate::http::response) len: usize,
    pub(in crate::http::response) date_offset: usize,
}

pub(in crate::http::response) struct HeadWrite<'a, H, F>
where
    H: HeaderSection + ?Sized,
    F: Framing,
{
    pub(in crate::http::response) status: StatusCode,
    pub(in crate::http::response) headers: &'a H,
    pub(in crate::http::response) framing: F,
}

impl<'a, H, F> HeadWrite<'a, H, F>
where
    H: HeaderSection + ?Sized,
    F: Framing,
{
    pub(in crate::http::response) fn wire_len(&self) -> usize {
        status_line_len(self.status)
            + self.headers.header_len()
            + self.framing.framing_len()
            + SERVER_DATE_TERMINATOR_LEN
    }

    pub(in crate::http::response) fn write(&self, out: &mut [u8], date: &[u8; 29]) -> WrittenHead {
        let mut out = WireWriter::new(out);
        out.put_status_line(self.status);
        self.headers.write_headers(&mut out);
        self.framing.write_framing(&mut out);
        let date_offset = out.put_server_date_terminator(date);
        WrittenHead {
            len: out.len(),
            date_offset,
        }
    }
}
