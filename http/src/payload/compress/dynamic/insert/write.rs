#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Capacity { required: usize, limit: usize },
    Length { len: usize, additional: usize },
}

const _: () = assert!(core::mem::size_of::<Error>() <= 3 * core::mem::size_of::<usize>());
