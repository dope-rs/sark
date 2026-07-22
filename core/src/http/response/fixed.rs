use http::StatusCode;
use o3::buffer::{Owned, Shared};

use super::wire_emit::{
    ContentLength, DATE_LEN, HeadWrite, HeaderSection, PLACEHOLDER_DATE, WireWriter,
};
use super::{DEFAULT_HEADER_CAPACITY, HeadInner, Headers};

const GZIP_HEADERS: &[u8] = b"Content-Encoding: gzip\r\nVary: Accept-Encoding\r\n";

struct GzipHeaders<'a, 'req, const N: usize>(&'a HeadInner<'req, N>);

impl<const N: usize> HeaderSection for GzipHeaders<'_, '_, N> {
    fn header_len(&self) -> usize {
        self.0.wire_len() + GZIP_HEADERS.len()
    }

    fn write_headers(&self, out: &mut WireWriter<'_>) {
        self.0.write_headers(out);
        out.put(GZIP_HEADERS);
    }
}

#[derive(Clone, Debug)]
pub struct FixedResponse<'req, const N: usize = DEFAULT_HEADER_CAPACITY> {
    pub(super) status: StatusCode,
    pub(super) head: HeadInner<'req, N>,
    pub(super) body: Shared,
}

impl<'req, const N: usize> FixedResponse<'req, N> {
    pub fn direct<B>(
        status: StatusCode,
        static_headers: &'static [u8],
        headers: Headers<'req, N>,
        body: B,
    ) -> Self
    where
        B: Into<Shared>,
    {
        let body = body.into();
        Self {
            status,
            head: HeadInner::new(static_headers, headers),
            body,
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn body_ref(&self) -> &[u8] {
        self.body.as_ref()
    }

    pub fn has_content_encoding(&self) -> bool {
        self.head.headers().has_content_encoding()
    }

    pub fn wire_headers(&self) -> Shared {
        let mut out = Owned::with_capacity(self.head.wire_len());
        self.head.write_into_owned(&mut out);
        out.freeze()
    }

    fn head_write(&self) -> (HeadWrite<'_, HeadInner<'req, N>, ContentLength>, &[u8]) {
        (
            self.head_write_with_len(self.body.len()),
            self.body.as_ref(),
        )
    }

    fn head_write_with_len(
        &self,
        body_len: usize,
    ) -> HeadWrite<'_, HeadInner<'req, N>, ContentLength> {
        HeadWrite {
            status_str: self.status.as_str().as_bytes(),
            reason: self
                .status
                .canonical_reason()
                .map(str::as_bytes)
                .unwrap_or(b""),
            headers: &self.head,
            framing: ContentLength(body_len),
        }
    }

    pub fn preserialize(&self) -> (Vec<u8>, usize) {
        let (head, body) = self.head_write();
        let mut buf = vec![0u8; head.wire_len() + body.len()];
        let written = head.write(&mut buf, PLACEHOLDER_DATE);
        WireWriter::at(&mut buf, written.len).put(body);
        (buf, written.date_offset)
    }

    pub fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        let (head, body) = self.head_write();
        if out.len() < head.wire_len() + body.len() {
            return None;
        }
        let written = head.write(out, date);
        let mut out = WireWriter::at(out, written.len);
        out.put(body);
        Some(out.len())
    }

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let (head, _) = self.head_write();
        if out.len() < head.wire_len() {
            return None;
        }
        let written = head.write(out, date);
        Some((written.len, self.body))
    }

    pub fn write_gzip_head(
        self,
        out: &mut [u8],
        date: &[u8; 29],
        body_len: usize,
    ) -> Option<usize> {
        let headers = GzipHeaders(&self.head);
        let head = HeadWrite {
            status_str: self.status.as_str().as_bytes(),
            reason: self
                .status
                .canonical_reason()
                .map(str::as_bytes)
                .unwrap_or(b""),
            headers: &headers,
            framing: ContentLength(body_len),
        };
        if out.len() < head.wire_len() {
            return None;
        }
        Some(head.write(out, date).len)
    }
}

impl FixedResponse<'static> {
    pub fn write_preserialized(
        out: &mut [u8],
        template: &[u8],
        date_offset: Option<usize>,
        date: &[u8; 29],
    ) -> Option<usize> {
        let total = template.len();
        if out.len() < total {
            return None;
        }
        out[..total].copy_from_slice(template);
        if let Some(off) = date_offset {
            out[off..off + DATE_LEN].copy_from_slice(date);
        }
        Some(total)
    }
}
