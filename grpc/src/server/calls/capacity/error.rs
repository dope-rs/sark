/// Active-call capacity reached by one connection-local scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Error {
    active: usize,
    limit: usize,
}

impl Error {
    pub(in crate::server::calls) const fn new(active: usize, limit: usize) -> Self {
        Self { active, limit }
    }
}

const _: () = assert!(core::mem::size_of::<Error>() == 2 * core::mem::size_of::<usize>());

impl From<Error> for (usize, usize) {
    fn from(error: Error) -> Self {
        (error.active, error.limit)
    }
}
