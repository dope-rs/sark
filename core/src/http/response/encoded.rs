use http::StatusCode;
use o3::buffer::{Owned, Shared};

use super::wire_emit::{ContentLength, HeadWrite, PLACEHOLDER_DATE};
use super::{DEFAULT_HEADER_CAPACITY, HeadInner, HeadersInner};

pub trait EncodedBody: Sized {
    fn encoded_len(&self) -> usize;

    fn encode_into(&self, out: &mut [u8]);

    fn into_shared(self, encoded_len: usize) -> Shared;
}

pub struct EncodedResponseInner<'req, B, const N: usize = DEFAULT_HEADER_CAPACITY> {
    status: StatusCode,
    head: HeadInner<'req, N>,
    body: B,
    body_len: usize,
}

pub type EncodedResponse<B> = EncodedResponseInner<'static, B>;

impl<'req, B, const N: usize> EncodedResponseInner<'req, B, N>
where
    B: EncodedBody,
{
    pub fn direct(
        status: StatusCode,
        static_headers: &'static [u8],
        headers: HeadersInner<'req, N>,
        body: B,
    ) -> Self {
        let body_len = body.encoded_len();
        Self {
            status,
            head: HeadInner::new(static_headers, headers),
            body,
            body_len,
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn wire_headers(&self) -> Shared {
        let mut out = Owned::with_capacity(self.head.wire_len());
        self.head.write_into_owned(&mut out);
        out.freeze()
    }

    pub(crate) fn encoded_body(&self) -> Shared {
        let mut body = vec![0; self.body_len];
        self.body.encode_into(&mut body);
        Shared::from(body)
    }

    fn head_write(&self) -> HeadWrite<'_, HeadInner<'req, N>, ContentLength> {
        HeadWrite {
            status_str: self.status.as_str().as_bytes(),
            reason: self
                .status
                .canonical_reason()
                .map(str::as_bytes)
                .unwrap_or(b""),
            headers: &self.head,
            framing: ContentLength(self.body_len),
        }
    }

    pub fn preserialize(&self) -> (Vec<u8>, usize) {
        let head = self.head_write();
        let head_len = head.wire_len();
        let body_len = self.body_len;
        let mut out = vec![0u8; head_len + body_len];
        let mut offset = 0usize;
        let date_offset = head.write(&mut out, &mut offset, PLACEHOLDER_DATE);
        self.body.encode_into(&mut out[offset..offset + body_len]);
        (out, date_offset)
    }

    pub fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        let head = self.head_write();
        let head_len = head.wire_len();
        let body_len = self.body_len;
        let total = head_len.checked_add(body_len)?;
        if out.len() < total {
            return None;
        }
        let mut offset = 0usize;
        head.write(out, &mut offset, date);
        self.body.encode_into(&mut out[offset..total]);
        Some(total)
    }

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let head = self.head_write();
        if out.len() < head.wire_len() {
            return None;
        }
        let mut offset = 0usize;
        head.write(out, &mut offset, date);
        Some((offset, self.body.into_shared(self.body_len)))
    }
}
