mod buffer;

pub(crate) use buffer::Buffer;

pub(super) const HEAD_CAPACITY: usize = 64 * 1024;

pub(super) const BODY_CAPACITY: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy)]
pub(crate) enum Window {
    Head,
    Body,
}
