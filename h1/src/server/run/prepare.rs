use dope::net;

use crate::server::date;

/// Failure while allocating one driver-local HTTP/1 slot.
#[derive(Debug)]
pub enum Error {
    Resource(net::Resource),
    Date(date::Error),
}

impl From<net::Resource> for Error {
    fn from(error: net::Resource) -> Self {
        Self::Resource(error)
    }
}

impl From<date::Error> for Error {
    fn from(error: date::Error) -> Self {
        Self::Date(error)
    }
}
