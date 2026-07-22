use super::WireWriter;
use o3::buffer::Shared;

pub(in crate::http::response) trait HeaderSection {
    fn header_len(&self) -> usize;
    fn write_headers(&self, out: &mut WireWriter<'_>);
}

impl HeaderSection for [u8] {
    fn header_len(&self) -> usize {
        self.len()
    }
    fn write_headers(&self, out: &mut WireWriter<'_>) {
        out.put(self);
    }
}

impl HeaderSection for [Shared] {
    fn header_len(&self) -> usize {
        self.iter().map(Shared::len).sum()
    }

    fn write_headers(&self, out: &mut WireWriter<'_>) {
        for header in self {
            out.put(header);
        }
    }
}
