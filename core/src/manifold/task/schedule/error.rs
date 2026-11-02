use o3::{alloc, depot::capacity};

/// Failure to construct a bounded task scheduler.
#[derive(Debug)]
pub enum Error {
    Layout(capacity::Error),
    Tasks(alloc::Error),
    Ready(alloc::Error),
    Orders(alloc::Error),
}
