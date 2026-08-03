use http::StatusCode;

use super::ProvenWireWriter;
use super::consts::SERVER_DATE_TERMINATOR_LEN;
use super::framing::Framing;
use super::headers::HeaderSection;

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
        let fixed = ProvenWireWriter::status_line_len(self.status)
            + self.framing.framing_len()
            + SERVER_DATE_TERMINATOR_LEN;
        self.headers.wire_len().saturating_add(fixed)
    }

    pub(in crate::http::response) fn emit(
        &self,
        out: &mut ProvenWireWriter<'_>,
        date: &[u8; 29],
    ) -> usize {
        debug_assert_eq!(out.len(), 0);
        out.put_status_line(self.status);
        self.headers.write_headers(out);
        self.framing.write_framing(out);
        out.put_server_date_terminator(date)
    }
}
