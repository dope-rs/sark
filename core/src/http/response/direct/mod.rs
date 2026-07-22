mod head;
mod headers;
mod plan;
mod value;

use o3::buffer::Owned;

trait WireBuffer {
    fn extend_from_slice(&mut self, bytes: &[u8]);
}

impl WireBuffer for Vec<u8> {
    fn extend_from_slice(&mut self, bytes: &[u8]) {
        Vec::extend_from_slice(self, bytes);
    }
}

impl WireBuffer for Owned {
    fn extend_from_slice(&mut self, bytes: &[u8]) {
        Owned::extend_from_slice(self, bytes);
    }
}

pub use head::HeadInner;
pub(super) use headers::INLINE_HOT_TEXT_PARTS;
pub use headers::{
    DEFAULT_HEADER_CAPACITY, HeaderNameToken, HeaderStaticValueToken, Headers, HeadersInner,
};
pub use plan::{ResponsePlan, ResponsePlanInner};
pub use value::{
    HeaderItem, HeaderItemInner, HeaderValueInner, InlineHeaderValue, IntoHeaderValue, TextSpec,
};
