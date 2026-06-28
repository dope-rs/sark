use http::StatusCode;
use o3::buffer::{Owned, Shared};

use super::wire_emit::{ContentLength, DATE_LEN, HeadWrite, NO_DATE, Out};
use super::{HeadInner, HeadersInner, InlineHeaderValue};

#[derive(Clone, Debug)]
pub struct FixedResponseInner<'req> {
    pub(super) status: StatusCode,
    pub(super) head: HeadInner<'req>,
    pub(super) body: Shared,
    pub(super) body_len_ascii: InlineHeaderValue,
}

pub type FixedResponse = FixedResponseInner<'static>;

impl<'req> FixedResponseInner<'req> {
    pub fn direct<B>(
        status: StatusCode,
        static_headers: &'static [u8],
        headers: HeadersInner<'req>,
        body: B,
    ) -> Self
    where
        B: Into<Shared>,
    {
        let body = body.into();
        Self {
            status,
            head: HeadInner::new(static_headers, headers),
            body_len_ascii: InlineHeaderValue::from_decimal(body.len()),
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
        self.head.write_into(&mut out);
        out.freeze()
    }

    pub fn write_preserialized(
        out: &mut [u8],
        template: &[u8],
        date_offset: usize,
        date: &[u8; 29],
    ) -> Option<usize> {
        let total = template.len();
        if out.len() < total {
            return None;
        }
        out[..total].copy_from_slice(template);
        if date_offset != NO_DATE {
            out[date_offset..date_offset + DATE_LEN].copy_from_slice(date);
        }
        Some(total)
    }

    pub fn preserialize(&self) -> (Vec<u8>, usize) {
        let dummy_date: &[u8; 29] = b"Mon, 01 Jan 2000 00:00:00 GMT";
        let mut buf = vec![0u8; 4096];
        let n = self
            .write_into_slice(&mut buf, dummy_date)
            .expect("preserialize: 4096 must be enough for any response");
        buf.truncate(n);
        let date_offset = buf
            .windows(29)
            .position(|w| w == dummy_date)
            .expect("preserialize: dummy date must appear in output");
        (buf, date_offset)
    }

    pub fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        let status_str = self.status.as_str().as_bytes();
        let reason = self
            .status
            .canonical_reason()
            .map(str::as_bytes)
            .unwrap_or(b"");
        let cl_body = self.body_len_ascii.as_bytes();
        let body = self.body.as_ref();

        let head = HeadWrite {
            status_str,
            reason,
            headers: &self.head,
            framing: ContentLength(cl_body),
        };
        let total = head.wire_len() + body.len();
        if out.len() < total {
            return None;
        }

        let mut off = 0usize;
        head.write(out, &mut off, date);
        Out::put(out, &mut off, body);
        Some(off)
    }

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let status_str = self.status.as_str().as_bytes();
        let reason = self
            .status
            .canonical_reason()
            .map(str::as_bytes)
            .unwrap_or(b"");
        let cl_body = self.body_len_ascii.as_bytes();
        let head = HeadWrite {
            status_str,
            reason,
            headers: &self.head,
            framing: ContentLength(cl_body),
        };
        if out.len() < head.wire_len() {
            return None;
        }
        let mut off = 0usize;
        head.write(out, &mut off, date);
        Some((off, self.body))
    }

    pub fn apply_gzip(&mut self, compressed: Shared) {
        const GZIP_EXTRA: &[u8] = b"Content-Encoding: gzip\r\nVary: Accept-Encoding\r\n";
        self.body_len_ascii = InlineHeaderValue::from_decimal(compressed.len());
        self.body = compressed;
        self.head.set_extra_static(GZIP_EXTRA);
    }
}
