use http::{HeaderValue, StatusCode};
use o3::buffer::Shared;

use super::wire_emit::{CRLF, ContentLength, HeadWrite, HeaderSection, Out};
use super::{DEFAULT_HEADER_CAPACITY, HeaderList, HotBodyInner, HotHeadInner, IntoHeaderName};

struct MonoHeaders<'a, 'req, const N: usize> {
    head: &'a HotHeadInner<'req, N>,
    dynamic: Option<&'a HeaderList>,
}

impl<const N: usize> HeaderSection for MonoHeaders<'_, '_, N> {
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
pub struct MonoResponseInner<'req, const N: usize = DEFAULT_HEADER_CAPACITY> {
    pub(super) status: StatusCode,
    pub(super) headers: Option<Box<HeaderList>>,
    pub(super) head: HotHeadInner<'req, N>,
    pub(super) body: HotBodyInner<'req>,
}

impl<'req, const N: usize> MonoResponseInner<'req, N> {
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

    pub fn insert_header<H>(&mut self, name: H, value: HeaderValue) -> &mut Self
    where
        H: IntoHeaderName,
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

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let (off, _) = self.write_head_into(out, date)?;
        Some((off, self.body.into_shared()))
    }

    fn with_head<R>(
        &self,
        f: impl FnOnce(&HeadWrite<'_, MonoHeaders<'_, 'req, N>, ContentLength>) -> R,
    ) -> R {
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
            framing: ContentLength(self.body.body_len()),
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
