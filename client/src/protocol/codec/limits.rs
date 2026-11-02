use sark_http::h1::head;

const DEFAULT_HEAD_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_BODY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    head_bytes: usize,
    body_bytes: usize,
}

impl Limits {
    pub const DEFAULT: Self = Self::new(DEFAULT_HEAD_BYTES, DEFAULT_BODY_BYTES);

    pub const fn new(head: usize, body: usize) -> Self {
        Self {
            head_bytes: head,
            body_bytes: body,
        }
    }

    pub const fn head_bytes(self) -> usize {
        self.head_bytes
    }

    pub const fn body_bytes(self) -> usize {
        self.body_bytes
    }

    pub(super) const fn head(self) -> head::Limit {
        head::Limit::RESPONSE.with_bytes(self.head_bytes)
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::DEFAULT
    }
}
