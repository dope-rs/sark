use core::mem;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Name { offset: usize, actual: u8 },
    Value { offset: usize, actual: u8 },
    Ending { offset: usize, actual: u8 },
    TooLarge,
    TooMany,
    TooLong,
}

const _: () = assert!(mem::size_of::<Error>() <= 2 * mem::size_of::<usize>());
