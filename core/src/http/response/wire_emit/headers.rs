use super::ProvenWireWriter;
use o3::buffer::Shared;

pub(in crate::http::response) trait HeaderSection {
    fn wire_len(&self) -> usize;
    fn write_headers(&self, out: &mut ProvenWireWriter<'_>);
}

impl HeaderSection for [u8] {
    fn wire_len(&self) -> usize {
        self.len()
    }
    fn write_headers(&self, out: &mut ProvenWireWriter<'_>) {
        out.put(self);
    }
}

impl HeaderSection for [Shared] {
    fn wire_len(&self) -> usize {
        self.iter()
            .fold(0usize, |total, header| total.saturating_add(header.len()))
    }

    fn write_headers(&self, out: &mut ProvenWireWriter<'_>) {
        for header in self {
            out.put(header);
        }
    }
}
