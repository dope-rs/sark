use http::StatusCode;
use o3::buffer::{Owned, Shared};

use super::wire_emit::{ContentLength, DATE_LEN, HeadWrite, Out, PLACEHOLDER_DATE};
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

    fn head_write(&self) -> (HeadWrite<'_, HeadInner<'req>, ContentLength<'_>>, &[u8]) {
        let head = HeadWrite {
            status_str: self.status.as_str().as_bytes(),
            reason: self
                .status
                .canonical_reason()
                .map(str::as_bytes)
                .unwrap_or(b""),
            headers: &self.head,
            framing: ContentLength(self.body_len_ascii.as_bytes()),
        };
        (head, self.body.as_ref())
    }

    pub fn preserialize(&self) -> (Vec<u8>, usize) {
        let (head, body) = self.head_write();
        let mut buf = vec![0u8; head.wire_len() + body.len()];
        let mut off = 0usize;
        let date_offset = head.write(&mut buf, &mut off, PLACEHOLDER_DATE);
        Out::put(&mut buf, &mut off, body);
        (buf, date_offset)
    }

    pub fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        let (head, body) = self.head_write();
        if out.len() < head.wire_len() + body.len() {
            return None;
        }
        let mut off = 0usize;
        head.write(out, &mut off, date);
        Out::put(out, &mut off, body);
        Some(off)
    }

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let (head, _) = self.head_write();
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
