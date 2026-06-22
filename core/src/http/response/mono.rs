use http::{HeaderValue, StatusCode};
use o3::buffer::Shared;

use super::wire_emit::{CL_PREFIX, CRLF, Out, SERVER_DATE_TERMINATOR_LEN, STATUS_LINE_PREFIX};
use super::{HeadInner, HeaderList, HeadersInner, HotBodyInner, HotHeadInner, IntoHeaderName};

#[derive(Clone, Debug)]
pub struct MonoResponseInner<'req> {
    pub(super) status: StatusCode,
    pub(super) headers: Option<Box<HeaderList>>,
    pub(super) head: HotHeadInner<'req>,
    pub(super) body: HotBodyInner<'req>,
}

impl<'req> MonoResponseInner<'req> {
    pub fn from_static_slice_body(
        status: StatusCode,
        static_headers: &'static [u8],
        headers: HeadersInner<'req>,
        body: &'static [u8],
    ) -> Self {
        Self {
            status,
            headers: None,
            head: HotHeadInner::Direct(HeadInner::new(static_headers, headers)),
            body: HotBodyInner::StaticSlice(body),
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &HeaderList {
        match &self.headers {
            Some(h) => h.as_ref(),
            None => HeaderList::empty_static(),
        }
    }

    pub fn headers_mut(&mut self) -> &mut HeaderList {
        self.headers
            .get_or_insert_with(|| Box::new(HeaderList::new()))
            .as_mut()
    }

    pub fn insert_header<N>(&mut self, name: N, value: HeaderValue) -> &mut Self
    where
        N: IntoHeaderName,
    {
        let _ = self.headers_mut().insert(name, value);
        self
    }

    pub fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        let body_len = self.body.body_len();
        let mut off = self.write_head_into(out, date, body_len)?;
        if out.len() - off < body_len {
            return None;
        }
        off += self.body.write_to(&mut out[off..off + body_len]);
        Some(off)
    }

    pub fn write_head_only(
        &self,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Option<(usize, &'static [u8])> {
        let body = match &self.body {
            HotBodyInner::StaticSlice(s) => *s,
            _ => return None,
        };
        let off = self.write_head_into(out, date, body.len())?;
        Some((off, body))
    }

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let body_len = self.body.body_len();
        let off = self.write_head_into(out, date, body_len)?;
        Some((off, self.body.into_shared()))
    }

    pub fn preserialize_static(&self) -> Option<(Vec<u8>, usize, &'static [u8])> {
        let body = match &self.body {
            HotBodyInner::StaticSlice(s) => *s,
            _ => return None,
        };
        let dummy_date: &[u8; 29] = b"Mon, 01 Jan 2000 00:00:00 GMT";
        let mut buf = vec![0u8; 4096];
        let n = self.write_head_into(&mut buf, dummy_date, body.len())?;
        buf.truncate(n);
        let date_offset = buf
            .windows(29)
            .position(|w| w == dummy_date)
            .expect("preserialize_static: dummy date must appear");
        Some((buf, date_offset, body))
    }

    fn write_head_into(&self, out: &mut [u8], date: &[u8; 29], body_len: usize) -> Option<usize> {
        let status_str = self.status.as_str().as_bytes();
        let reason = self
            .status
            .canonical_reason()
            .map(str::as_bytes)
            .unwrap_or(b"");

        let mut cl_raw = [0u8; 20];
        let cl_n = crate::http::codec::Wire::write_dec(body_len, &mut cl_raw);
        let cl_body = &cl_raw[..cl_n];

        let head_wire_len = match &self.head {
            HotHeadInner::Wire(bytes) => bytes.len(),
            HotHeadInner::Direct(head) => head.wire_len(),
        };
        let dyn_hdr_len = self.headers.as_deref().map_or(0, |h| h.wire_len());

        let head_total = STATUS_LINE_PREFIX.len()
            + status_str.len()
            + 1
            + reason.len()
            + CRLF.len()
            + head_wire_len
            + dyn_hdr_len
            + CL_PREFIX.len()
            + cl_body.len()
            + CRLF.len()
            + SERVER_DATE_TERMINATOR_LEN;
        let _ = body_len;
        if out.len() < head_total {
            return None;
        }

        let mut off = 0usize;
        Out::put_status_line(out, &mut off, status_str, reason);

        match &self.head {
            HotHeadInner::Wire(bytes) => Out::put(out, &mut off, bytes),
            HotHeadInner::Direct(head) => {
                let written = head
                    .write_slice(&mut out[off..])
                    .expect("wire_len reserved above");
                off += written;
            }
        }

        if let Some(h) = self.headers.as_deref() {
            for (name, value) in h.iter() {
                Out::put(out, &mut off, name.as_str().as_bytes());
                Out::put(out, &mut off, b": ");
                Out::put(out, &mut off, value.as_bytes());
                Out::put(out, &mut off, CRLF);
            }
        }

        Out::put(out, &mut off, CL_PREFIX);
        Out::put(out, &mut off, cl_body);
        Out::put(out, &mut off, CRLF);
        Out::put_server_date_terminator(out, &mut off, date);
        Some(off)
    }
}
