use o3::buffer::Shared;

use super::{DEFAULT_HEADER_CAPACITY, HeadInner};

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum HotHeadInner<'req, const N: usize = DEFAULT_HEADER_CAPACITY> {
    Wire(Shared),
    Direct(HeadInner<'req, N>),
}

impl<'req, const N: usize> HotHeadInner<'req, N> {
    pub(super) fn into_bytes(self) -> Shared {
        match self {
            Self::Wire(bytes) => bytes,
            Self::Direct(head) => head.wire_headers(),
        }
    }
}
