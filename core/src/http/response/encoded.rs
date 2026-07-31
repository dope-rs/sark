use http::StatusCode;
use o3::buffer::Shared;

use super::wire_emit::{ContentLength, HeadWrite, PLACEHOLDER_DATE};
use super::{
    DEFAULT_HEADER_CAPACITY, HeadInner, HeaderTemplate, Headers, StaticHeaderBytes,
    StaticHeaderFields, StaticHeaders,
};

pub trait EncodedBody: Sized {
    type Error;

    fn encoded_len(&self) -> usize;

    fn encode_into(&self, out: &mut [u8]) -> Result<(), Self::Error>;

    fn into_shared(self, encoded_len: usize) -> Result<Shared, Self::Error> {
        let mut out = vec![0; encoded_len];
        self.encode_into(&mut out)?;
        Ok(Shared::from(out))
    }
}

pub struct EncodedResponse<'req, B, const N: usize = DEFAULT_HEADER_CAPACITY, S = StaticHeaderBytes>
{
    pub(super) status: StatusCode,
    pub(super) head: HeadInner<'req, N, S>,
    pub(super) body: B,
    pub(super) body_len: usize,
}

impl<'req, B, const N: usize> EncodedResponse<'req, B, N>
where
    B: EncodedBody,
{
    pub fn direct(
        status: StatusCode,
        static_headers: &'static [u8],
        headers: Headers<'req, N>,
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
}

impl<'req, B, const N: usize> EncodedResponse<'req, B, N, StaticHeaderFields>
where
    B: EncodedBody,
{
    #[doc(hidden)]
    pub fn structured(
        status: StatusCode,
        static_headers: &'static HeaderTemplate,
        headers: Headers<'req, N>,
        body: B,
    ) -> Self {
        let body_len = body.encoded_len();
        Self {
            status,
            head: HeadInner::structured(static_headers, headers),
            body,
            body_len,
        }
    }
}

impl<'req, B, const N: usize, S> EncodedResponse<'req, B, N, S>
where
    B: EncodedBody,
    S: StaticHeaders,
{
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn wire_headers(&self) -> Shared {
        self.head.wire_headers()
    }

    pub(crate) fn encoded_body(&self) -> Option<Shared> {
        let mut body = vec![0; self.body_len];
        self.body.encode_into(&mut body).ok()?;
        Some(Shared::from(body))
    }

    fn head_write(&self) -> HeadWrite<'_, HeadInner<'req, N, S>, ContentLength> {
        HeadWrite {
            status: self.status,
            headers: &self.head,
            framing: ContentLength(self.body_len),
        }
    }

    pub fn preserialize(&self) -> Option<(Vec<u8>, usize)> {
        let head = self.head_write();
        let head_len = head.wire_len();
        let body_len = self.body_len;
        let mut out = vec![0u8; head_len + body_len];
        let written = head.write(&mut out, PLACEHOLDER_DATE);
        self.body
            .encode_into(&mut out[written.len..written.len + body_len])
            .ok()?;
        Some((out, written.date_offset))
    }

    pub fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        let head = self.head_write();
        let head_len = head.wire_len();
        let body_len = self.body_len;
        let total = head_len.checked_add(body_len)?;
        if out.len() < total {
            return None;
        }
        let written = head.write(out, date);
        self.body.encode_into(&mut out[written.len..total]).ok()?;
        Some(total)
    }

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let head = self.head_write();
        if out.len() < head.wire_len() {
            return None;
        }
        let written = head.write(out, date);
        let body = self.body.into_shared(self.body_len).ok()?;
        Some((written.len, body))
    }
}
