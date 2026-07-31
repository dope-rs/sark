use http::StatusCode;
use o3::buffer::Shared;

use super::wire_emit::{ContentLength, HeadWrite, PLACEHOLDER_DATE, WireWriter};
use super::{
    DEFAULT_HEADER_CAPACITY, HeadInner, HeaderTemplate, Headers, StaticHeaderBytes,
    StaticHeaderFields, StaticHeaders,
};

/// A response whose body is guaranteed to live for the entire process.
///
/// Keeping the static slice directly avoids paying for the largest variant of
/// `Body` on generated static-response routes.
#[derive(Clone, Debug)]
pub struct StaticResponseInner<
    'req,
    const N: usize = DEFAULT_HEADER_CAPACITY,
    S = StaticHeaderBytes,
> {
    pub(super) status: StatusCode,
    pub(super) head: HeadInner<'req, N, S>,
    pub(super) body: &'static [u8],
}

impl<'req, const N: usize> StaticResponseInner<'req, N> {
    pub fn direct(
        status: StatusCode,
        static_headers: &'static [u8],
        headers: Headers<'req, N>,
        body: &'static [u8],
    ) -> Self {
        Self {
            status,
            head: HeadInner::new(static_headers, headers),
            body,
        }
    }
}

impl<'req, const N: usize> StaticResponseInner<'req, N, StaticHeaderFields> {
    #[doc(hidden)]
    pub fn structured(
        status: StatusCode,
        static_headers: &'static HeaderTemplate,
        headers: Headers<'req, N>,
        body: &'static [u8],
    ) -> Self {
        Self {
            status,
            head: HeadInner::structured(static_headers, headers),
            body,
        }
    }
}

impl<'req, const N: usize, S> StaticResponseInner<'req, N, S>
where
    S: StaticHeaders,
{
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn body_ref(&self) -> &'static [u8] {
        self.body
    }

    pub fn wire_headers(&self) -> Shared {
        self.head.wire_headers()
    }

    fn head_write(&self) -> HeadWrite<'_, HeadInner<'req, N, S>, ContentLength> {
        HeadWrite {
            status: self.status,
            headers: &self.head,
            framing: ContentLength(self.body.len()),
        }
    }

    pub fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        let head = self.head_write();
        let total = head.wire_len().checked_add(self.body.len())?;
        if out.len() < total {
            return None;
        }
        let written = head.write(out, date);
        let mut out = WireWriter::at(out, written.len);
        out.put(self.body);
        Some(out.len())
    }

    pub fn write_head_only(
        &self,
        out: &mut [u8],
        date: &[u8; 29],
    ) -> Option<(usize, &'static [u8])> {
        let head = self.head_write();
        if out.len() < head.wire_len() {
            return None;
        }
        let written = head.write(out, date);
        Some((written.len, self.body))
    }

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let (off, body) = self.write_head_only(out, date)?;
        Some((off, Shared::from_static(body)))
    }

    pub fn preserialize_static(&self) -> (Vec<u8>, usize, &'static [u8]) {
        let head = self.head_write();
        let mut out = vec![0u8; head.wire_len()];
        let written = head.write(&mut out, PLACEHOLDER_DATE);
        (out, written.date_offset, self.body)
    }
}
