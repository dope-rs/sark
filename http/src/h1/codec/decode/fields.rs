use o3::alloc;

use crate::{field::storage, h1::field::Entry};

#[derive(Debug)]
pub enum Error {
    Name(storage::Error),
    Value(storage::Error),
    Allocation(alloc::Error),
}

/// Consumer for one syntactically admitted HTTP/1 field section.
/// Fields borrow the wire input; the sink chooses borrowing, compact state,
/// or explicit materialization without another wire traversal.
pub trait Sink<'input> {
    type Error;

    fn push(&mut self, name: &'input [u8], value: &'input [u8]) -> Result<(), Self::Error>;

    /// Retains the scanner's syntax proof when materializing an admitted field.
    fn entry(&mut self, field: Entry<'input>) -> Result<(), Self::Error> {
        self.push(field.name(), field.value())
    }
}

const _: () = assert!(core::mem::size_of::<Error>() <= 3 * core::mem::size_of::<usize>());
