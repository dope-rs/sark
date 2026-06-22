use super::out::Out;

pub(in crate::http::response) trait HeaderSection {
    fn header_len(&self) -> usize;
    fn write_headers(&self, out: &mut [u8], off: &mut usize);
}

impl HeaderSection for [u8] {
    fn header_len(&self) -> usize {
        self.len()
    }
    fn write_headers(&self, out: &mut [u8], off: &mut usize) {
        Out::put(out, off, self);
    }
}
