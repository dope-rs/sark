use sark_core::manifold::server::app;

use crate::conn;

mod read;
mod turn;

pub(in crate::server) use read::Read;
pub(in crate::server) use turn::Turn;

pub(in crate::server) trait Fault: From<conn::Error> {
    fn connection(&self) -> Option<&conn::Error>;
}

impl Fault for conn::Error {
    fn connection(&self) -> Option<&conn::Error> {
        Some(self)
    }
}

impl<E> Fault for app::Error<conn::Error, E> {
    fn connection(&self) -> Option<&conn::Error> {
        match self {
            Self::Connection(error) => Some(error),
            Self::Application(_) => None,
        }
    }
}
