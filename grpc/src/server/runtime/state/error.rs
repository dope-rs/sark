use std::num;

use o3::{
    alloc,
    depot::{capacity, heap},
};

#[derive(Debug)]
pub enum Error {
    Storage(alloc::Error),
    Deadlines(heap::Error),
    Connections {
        capacity: num::NonZero<u32>,
    },
    Extent {
        requested: capacity::Count,
        capacity: usize,
    },
}
