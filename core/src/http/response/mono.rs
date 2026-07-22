use http::{HeaderName, HeaderValue, StatusCode};
use o3::buffer::Shared;

use super::wire_emit::{CRLF, ContentLength, HeadWrite, HeaderSection, WireWriter};
use super::{DEFAULT_HEADER_CAPACITY, HeaderList, HotBodyInner, HotHeadInner};

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

    fn write_headers(&self, out: &mut WireWriter<'_>) {
        match self.head {
            HotHeadInner::Wire(bytes) => out.put(bytes),
            HotHeadInner::Direct(head) => head.write_headers(out),
        }
        if let Some(h) = self.dynamic {
            for (name, value) in h.iter() {
                out.put(name.as_str().as_bytes());
                out.put(b": ");
                out.put(value.as_bytes());
                out.put(CRLF);
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

    pub fn insert_header(&mut self, name: HeaderName, value: HeaderValue) -> &mut Self {
        let _ = self.headers_mut().insert(name, value);
        self
    }

    pub fn wire_headers(&self) -> Shared {
        let section = MonoHeaders {
            head: &self.head,
            dynamic: self.headers.as_deref(),
        };
        let mut bytes = vec![0; section.header_len()];
        section.write_headers(&mut WireWriter::new(&mut bytes));
        Shared::from(bytes)
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
            let written = head.write(out, date);
            Some((written.len, written.date_offset))
        })
    }
}
