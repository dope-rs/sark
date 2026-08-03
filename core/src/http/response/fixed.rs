use http::StatusCode;
use o3::buffer::Shared;

use super::wire_emit::{
    ContentLength, DATE_LEN, HeadWrite, HeaderSection, PLACEHOLDER_DATE, ProvenWireWriter,
};
use super::{
    DEFAULT_HEADER_CAPACITY, HeadInner, HeaderTemplate, Headers, StaticHeaderBytes,
    StaticHeaderFields, StaticHeaders,
};

const GZIP_HEADERS: &[u8] = b"Content-Encoding: gzip\r\nVary: Accept-Encoding\r\n";

struct GzipHeaders<'a, 'req, const N: usize, S>(&'a HeadInner<'req, N, S>);

impl<const N: usize, S> HeaderSection for GzipHeaders<'_, '_, N, S>
where
    S: StaticHeaders,
{
    fn wire_len(&self) -> usize {
        HeaderSection::wire_len(self.0).saturating_add(GZIP_HEADERS.len())
    }

    fn write_headers(&self, out: &mut ProvenWireWriter<'_>) {
        self.0.write_headers(out);
        out.put(GZIP_HEADERS);
    }
}

#[derive(Clone, Debug)]
pub struct FixedResponse<'req, const N: usize = DEFAULT_HEADER_CAPACITY, S = StaticHeaderBytes> {
    pub(super) status: StatusCode,
    pub(super) head: HeadInner<'req, N, S>,
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
}

impl<'req, const N: usize> FixedResponse<'req, N, StaticHeaderFields> {
    #[doc(hidden)]
    pub fn structured<B>(
        status: StatusCode,
        static_headers: &'static HeaderTemplate,
        headers: Headers<'req, N>,
        body: B,
    ) -> Self
    where
        B: Into<Shared>,
    {
        let body = body.into();
        Self {
            status,
            head: HeadInner::structured(static_headers, headers),
            body,
        }
    }
}

impl<'req, const N: usize, S> FixedResponse<'req, N, S>
where
    S: StaticHeaders,
{
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
        self.head.wire_headers()
    }

    fn head_write(&self) -> (HeadWrite<'_, HeadInner<'req, N, S>, ContentLength>, &[u8]) {
        (
            self.head_write_with_len(self.body.len()),
            self.body.as_ref(),
        )
    }

    fn head_write_with_len(
        &self,
        body_len: usize,
    ) -> HeadWrite<'_, HeadInner<'req, N, S>, ContentLength> {
        HeadWrite {
            status: self.status,
            headers: &self.head,
            framing: ContentLength(body_len),
        }
    }

    pub fn preserialize(&self) -> (Vec<u8>, usize) {
        let (head, body) = self.head_write();
        let total = head.wire_len().saturating_add(body.len());
        let mut buf = vec![0u8; total];
        let mut out = ProvenWireWriter::exact(&mut buf);
        let date_offset = head.emit(&mut out, PLACEHOLDER_DATE);
        out.put(body);
        out.finish(total);
        (buf, date_offset)
    }

    pub fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        let (head, body) = self.head_write();
        let total = head.wire_len().checked_add(body.len())?;
        let mut out = ProvenWireWriter::new(out, total)?;
        head.emit(&mut out, date);
        out.put(body);
        Some(out.finish(total))
    }

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let (head, _) = self.head_write();
        let head_len = head.wire_len();
        let mut out = ProvenWireWriter::new(out, head_len)?;
        head.emit(&mut out, date);
        Some((out.finish(head_len), self.body))
    }

    pub fn write_gzip_head(
        self,
        out: &mut [u8],
        date: &[u8; 29],
        body_len: usize,
    ) -> Option<usize> {
        let headers = GzipHeaders(&self.head);
        let head = HeadWrite {
            status: self.status,
            headers: &headers,
            framing: ContentLength(body_len),
        };
        let head_len = head.wire_len();
        let mut out = ProvenWireWriter::new(out, head_len)?;
        head.emit(&mut out, date);
        Some(out.finish(head_len))
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
