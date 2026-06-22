use o3::buffer::Owned;

use super::headers::HeadersInner;

#[derive(Clone, Debug)]
pub struct HeadInner<'req> {
    static_headers: &'static [u8],
    extra_static: &'static [u8],
    headers: HeadersInner<'req>,
}

impl<'req> HeadInner<'req> {
    pub fn new(static_headers: &'static [u8], headers: HeadersInner<'req>) -> Self {
        Self {
            static_headers,
            extra_static: b"",
            headers,
        }
    }

    pub fn static_headers(&self) -> &'static [u8] {
        self.static_headers
    }

    pub fn headers(&self) -> &HeadersInner<'req> {
        &self.headers
    }

    pub(super) fn headers_mut(&mut self) -> &mut HeadersInner<'req> {
        &mut self.headers
    }

    pub fn set_extra_static(&mut self, extra: &'static [u8]) {
        self.extra_static = extra;
    }

    pub fn wire_len(&self) -> usize {
        self.static_headers.len() + self.extra_static.len() + self.headers.wire_len()
    }

    pub fn write_into(&self, out: &mut Owned) {
        out.extend_from_slice(self.static_headers);
        out.extend_from_slice(self.extra_static);
        self.headers.write_into(out);
    }

    pub fn write_slice(&self, out: &mut [u8]) -> Option<usize> {
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        let n = self.static_headers.len();
        out[..n].copy_from_slice(self.static_headers);
        let e = self.extra_static.len();
        out[n..n + e].copy_from_slice(self.extra_static);
        let m = self.headers.write(&mut out[n + e..]);
        Some(n + e + m)
    }
}

impl crate::http::response::wire_emit::HeaderSection for HeadInner<'_> {
    fn header_len(&self) -> usize {
        Self::wire_len(self)
    }
    fn write_headers(&self, out: &mut [u8], off: &mut usize) {
        let written = self
            .write_slice(&mut out[*off..])
            .expect("HeadWrite invariant: head buffer reserved via wire_len");
        *off += written;
    }
}
