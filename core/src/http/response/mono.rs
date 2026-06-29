use http::{HeaderValue, StatusCode};
use o3::buffer::Shared;

use super::wire_emit::{CRLF, ContentLength, HeadWrite, HeaderSection, Out, PLACEHOLDER_DATE};
use super::{HeadInner, HeaderList, HeadersInner, HotBodyInner, HotHeadInner, IntoHeaderName};

struct MonoHeaders<'a, 'req> {
    head: &'a HotHeadInner<'req>,
    dynamic: Option<&'a HeaderList>,
}

impl HeaderSection for MonoHeaders<'_, '_> {
    fn header_len(&self) -> usize {
        let head = match self.head {
            HotHeadInner::Wire(bytes) => bytes.len(),
            HotHeadInner::Direct(head) => head.wire_len(),
        };
        head + self.dynamic.map_or(0, HeaderList::wire_len)
    }

    fn write_headers(&self, out: &mut [u8], off: &mut usize) {
        match self.head {
            HotHeadInner::Wire(bytes) => Out::put(out, off, bytes),
            HotHeadInner::Direct(head) => {
                let written = head
                    .write_slice(&mut out[*off..])
                    .expect("HeadWrite invariant: head buffer reserved via wire_len");
                *off += written;
            }
        }
        if let Some(h) = self.dynamic {
            for (name, value) in h.iter() {
                Out::put(out, off, name.as_str().as_bytes());
                Out::put(out, off, b": ");
                Out::put(out, off, value.as_bytes());
                Out::put(out, off, CRLF);
            }
        }
    }
}

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
        let (mut off, _) = self.write_head_into(out, date)?;
        let body_len = self.body.body_len();
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
        let (off, _) = self.write_head_into(out, date)?;
        Some((off, body))
    }

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let (off, _) = self.write_head_into(out, date)?;
        Some((off, self.body.into_shared()))
    }

    pub fn preserialize_static(&self) -> Option<(Vec<u8>, usize, &'static [u8])> {
        let body = match &self.body {
            HotBodyInner::StaticSlice(s) => *s,
            _ => return None,
        };
        let mut buf = vec![0u8; self.with_head(|head| head.wire_len())];
        let (_, date_offset) = self.write_head_into(&mut buf, PLACEHOLDER_DATE)?;
        Some((buf, date_offset, body))
    }

    fn with_head<R>(
        &self,
        f: impl FnOnce(&HeadWrite<'_, MonoHeaders<'_, 'req>, ContentLength<'_>>) -> R,
    ) -> R {
        let mut cl_raw = [0u8; 20];
        let cl_n = crate::http::codec::Wire::write_dec(self.body.body_len(), &mut cl_raw);
        let section = MonoHeaders {
            head: &self.head,
            dynamic: self.headers.as_deref(),
        };
        let head = HeadWrite {
            status_str: self.status.as_str().as_bytes(),
            reason: self
                .status
                .canonical_reason()
                .map(str::as_bytes)
                .unwrap_or(b""),
            headers: &section,
            framing: ContentLength(&cl_raw[..cl_n]),
        };
        f(&head)
    }

    fn write_head_into(&self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, usize)> {
        self.with_head(|head| {
            if out.len() < head.wire_len() {
                return None;
            }
            let mut off = 0usize;
            let date_offset = head.write(out, &mut off, date);
            Some((off, date_offset))
        })
    }
}
