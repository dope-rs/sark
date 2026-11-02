mod draft;
mod name;
pub mod stages;
mod state;
mod value;
pub mod write;
mod writer;

pub use draft::Draft;
pub use name::Name;
pub(crate) use state::State;
pub use value::Value;
pub use writer::Writer;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Capacity {
    pub required: usize,
    pub limit: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Range {
    requested: usize,
    start: u32,
    end: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Span {
    start: u32,
    extent: u32,
    capacity: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Capacity { required: usize, limit: usize },
    Index { index: usize, len: usize },
    Length { len: usize, additional: usize },
    Name { actual: usize, limit: usize },
    Range(Range),
    Span(Span),
}

impl Range {
    pub(in crate::payload::compress::dynamic) const fn new(
        value: usize,
        start: u32,
        end: u32,
    ) -> Self {
        Self {
            requested: value,
            start,
            end,
        }
    }

    pub const fn requested(self) -> usize {
        self.requested
    }

    pub const fn start(self) -> usize {
        self.start as usize
    }

    pub const fn end(self) -> usize {
        self.end as usize
    }
}

impl Span {
    pub(in crate::payload::compress::dynamic) const fn new(
        start: u32,
        extent: u32,
        capacity: u32,
    ) -> Self {
        Self {
            start,
            extent,
            capacity,
        }
    }

    pub const fn start(self) -> usize {
        self.start as usize
    }

    pub const fn extent(self) -> usize {
        self.extent as usize
    }

    pub const fn capacity(self) -> usize {
        self.capacity as usize
    }
}

const _: () = assert!(core::mem::size_of::<Capacity>() == 2 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Range>() == 2 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Span>() == 3 * core::mem::size_of::<u32>());
const _: () = assert!(core::mem::size_of::<Error>() <= 3 * core::mem::size_of::<usize>());
