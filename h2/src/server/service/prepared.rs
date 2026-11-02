use sark_core::{
    decode::{exec, request, state},
    request::Head,
};
use sark_http::field::metadata::packed;

#[doc(hidden)]
pub struct Prepared {
    head: Head<packed::Decoded>,
}

impl Prepared {
    pub(super) const fn new(head: Head<packed::Decoded>) -> Self {
        Self { head }
    }

    pub(super) fn into_head(self) -> Head<packed::Decoded> {
        self.head
    }
}

impl request::Prepared for Prepared {
    type Execution = exec::Sync;
    type Family = state::Uniforms<Self>;
}
