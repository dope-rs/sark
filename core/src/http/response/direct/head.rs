use super::headers::{DEFAULT_HEADER_CAPACITY, Headers};

#[derive(Clone, Debug)]
pub struct HeadInner<'req, const N: usize = DEFAULT_HEADER_CAPACITY> {
    static_headers: &'static [u8],
    headers: Headers<'req, N>,
}

impl<'req, const N: usize> HeadInner<'req, N> {
    pub fn new(static_headers: &'static [u8], headers: Headers<'req, N>) -> Self {
        Self {
            static_headers,
            headers,
        }
    }

    pub fn static_headers(&self) -> &'static [u8] {
        self.static_headers
    }

    pub fn headers(&self) -> &Headers<'req, N> {
        &self.headers
    }

    pub(super) fn headers_mut(&mut self) -> &mut Headers<'req, N> {
        &mut self.headers
    }

    pub fn wire_len(&self) -> usize {
        self.static_headers.len() + self.headers.wire_len()
    }

    pub(crate) fn write_into_owned(&self, out: &mut o3::buffer::Owned) {
        out.extend_from_slice(self.static_headers);
        self.headers.write_into_owned(out);
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
    fn write_headers(&self, out: &mut crate::http::response::wire_emit::WireWriter<'_>) {
        out.put(self.static_headers);
        self.headers.write_wire(out);
    }
}
