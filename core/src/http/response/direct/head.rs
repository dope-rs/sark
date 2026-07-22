use super::headers::{DEFAULT_HEADER_CAPACITY, HeadersInner};

#[derive(Clone, Debug)]
pub struct HeadInner<'req, const N: usize = DEFAULT_HEADER_CAPACITY> {
    static_headers: &'static [u8],
    headers: HeadersInner<'req, N>,
}

impl<'req, const N: usize> HeadInner<'req, N> {
    pub fn new(static_headers: &'static [u8], headers: HeadersInner<'req, N>) -> Self {
        Self {
            static_headers,
            headers,
        }
    }

    pub fn static_headers(&self) -> &'static [u8] {
        self.static_headers
    }

    pub fn headers(&self) -> &HeadersInner<'req, N> {
        &self.headers
    }

    pub(super) fn headers_mut(&mut self) -> &mut HeadersInner<'req, N> {
        &mut self.headers
    }

    pub fn wire_len(&self) -> usize {
        self.static_headers.len() + self.headers.wire_len()
    }

    pub fn write_into(&self, out: &mut Vec<u8>) {
        self.write_into_buffer(out);
    }

    pub(crate) fn write_into_owned(&self, out: &mut o3::buffer::Owned) {
        self.write_into_buffer(out);
    }

    fn write_into_buffer(&self, out: &mut impl super::WireBuffer) {
        out.extend_from_slice(self.static_headers);
        self.headers.write_into_buffer(out);
    }

    pub fn write_slice(&self, out: &mut [u8]) -> Option<usize> {
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        let n = self.static_headers.len();
        out[..n].copy_from_slice(self.static_headers);
        let m = self.headers.write(&mut out[n..]);
        Some(n + m)
    }
}

impl<const N: usize> crate::http::response::wire_emit::HeaderSection for HeadInner<'_, N> {
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
