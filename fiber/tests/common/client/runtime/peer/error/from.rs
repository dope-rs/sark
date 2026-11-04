use crate::common::client::runtime::peer::Error;

impl<E> From<E> for Error<E> {
    fn from(error: E) -> Self {
        Self::Io(error)
    }
}
