use crate::frame::numbers;

#[derive(Clone, Copy)]
pub(in crate::server::task) enum Error {
    Full,
    Occupied,
}

impl Error {
    pub(in crate::server::task) const fn code(self) -> numbers::ErrorCode {
        match self {
            Self::Full => numbers::ErrorCode::RefusedStream,
            Self::Occupied => numbers::ErrorCode::InternalError,
        }
    }
}
