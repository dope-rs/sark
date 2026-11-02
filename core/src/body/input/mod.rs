use o3::buffer::segments;

mod checked;
mod parts;
pub(super) mod private;

pub use checked::Checked;
pub use parts::Parts;

/// One ingress value and its exact wire-accounting dimensions.
pub struct Input<D> {
    data: D,
    bytes: usize,
    chunks: segments::Count,
}

impl<D> Input<D> {
    pub const fn new(data: D, bytes: usize, chunks: segments::Count) -> Self {
        Self {
            data,
            bytes,
            chunks,
        }
    }

    pub fn into_parts(self) -> (D, usize, segments::Count) {
        (self.data, self.bytes, self.chunks)
    }
}
