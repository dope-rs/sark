use http::{HeaderName, HeaderValue, StatusCode};
use o3::buffer::Shared;

use super::wire_emit::{CRLF, ContentLength, HeadWrite, HeaderSection, ProvenWireWriter};
use super::{Body, DEFAULT_HEADER_CAPACITY, HeaderList, HotHeadInner};

struct MonoHeaders<'a, 'req, const N: usize> {
    head: &'a HotHeadInner<'req, N>,
    dynamic: Option<&'a HeaderList>,
}

impl<const N: usize> HeaderSection for MonoHeaders<'_, '_, N> {
    fn wire_len(&self) -> usize {
        let head = match self.head {
            HotHeadInner::Wire(bytes) => bytes.len(),
            HotHeadInner::Direct(head) => HeaderSection::wire_len(head),
        };
        head.saturating_add(self.dynamic.map_or(0, HeaderList::wire_len))
    }

    fn write_headers(&self, out: &mut ProvenWireWriter<'_>) {
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
    pub(super) body: Body<'req>,
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
        let section = self.header_section();
        let len = section.wire_len();
        let mut bytes = vec![0; len];
        let mut out = ProvenWireWriter::exact(&mut bytes);
        section.write_headers(&mut out);
        out.finish(len);
        Shared::from(bytes)
    }

    pub fn write_into_slice(&self, out: &mut [u8], date: &[u8; 29]) -> Option<usize> {
        let section = self.header_section();
        let head = HeadWrite {
            status: self.status,
            headers: &section,
            framing: ContentLength(self.body.body_len()),
        };
        let total = head.wire_len().checked_add(self.body.body_len())?;
        let mut out = ProvenWireWriter::new(out, total)?;
        head.emit(&mut out, date);
        out.put(self.body.as_bytes());
        Some(out.finish(total))
    }

    pub fn write_head_split(self, out: &mut [u8], date: &[u8; 29]) -> Option<(usize, Shared)> {
        let section = self.header_section();
        let head = HeadWrite {
            status: self.status,
            headers: &section,
            framing: ContentLength(self.body.body_len()),
        };
        let head_len = head.wire_len();
        let mut out = ProvenWireWriter::new(out, head_len)?;
        head.emit(&mut out, date);
        let written = out.finish(head_len);
        Some((written, self.body.into_shared()))
    }

    fn header_section(&self) -> MonoHeaders<'_, 'req, N> {
        MonoHeaders {
            head: &self.head,
            dynamic: self.headers.as_deref(),
        }
    }
}
