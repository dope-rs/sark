use crate::{
    frame,
    manifold::{self, requests, server},
};

#[derive(Debug)]
pub(in crate::manifold::server) enum Error {
    Protocol(manifold::Error),
    Request(requests::Error),
}

impl Error {
    pub(in crate::manifold::server) fn code(self) -> frame::ErrorCode {
        match self {
            Self::Protocol(error) => error.code(),
            Self::Request(error) => server::request_error_code(error),
        }
    }
}
