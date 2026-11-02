use std::num::NonZero;

use sark_client::protocol::codec::{Codec, Limits};

const DEFAULT_RESIDENT_CAPACITY: usize =
    Limits::DEFAULT.head_bytes() + Limits::DEFAULT.body_bytes();
const DEFAULT_QUEUE_CAPACITY: usize = 64 * 1024 * 1024;
pub(super) const SEGMENTS: NonZero<u32> = NonZero::<u32>::MIN.saturating_add(16 * 1024 - 1);

pub(super) struct Response {
    pub(super) head_limit: usize,
    pub(super) body_limit: usize,
    pub(super) resident_capacity: usize,
    pub(super) queue_capacity: usize,
}

impl Response {
    pub(super) const fn codec(&self) -> Codec {
        Codec::new(Limits::new(self.head_limit, self.body_limit))
    }
}

impl Default for Response {
    fn default() -> Self {
        let limits = Limits::DEFAULT;
        Self {
            head_limit: limits.head_bytes(),
            body_limit: limits.body_bytes(),
            resident_capacity: DEFAULT_RESIDENT_CAPACITY,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}
