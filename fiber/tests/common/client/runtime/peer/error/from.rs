use std::io;

use crate::common::client::runtime::peer::Error;

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
