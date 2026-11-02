//! Exact generated HTTP/2 task preparation failures.
use o3::{alloc, depot::capacity};

use crate::conn::config;

#[derive(Debug)]
pub enum Failure<E> {
    Protocol(config::Error),
    Router(E),
    TaskCapacity(capacity::Error),
    Tasks(alloc::Error),
    ZeroTaskCapacity,
}
