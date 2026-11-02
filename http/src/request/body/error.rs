use o3::buffer::segments;

use crate::payload::owned;

/// Exact failure to expose a complete retained body as contiguous bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    Borrowed { len: usize, count: segments::Count },
    Retention { len: usize, stored: usize },
    Owned(owned::Error),
}
