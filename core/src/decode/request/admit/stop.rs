use core::convert::Infallible;

/// The exact result of a guard that ended admission.
pub trait Stop<T, E> {
    fn into_result(self) -> Result<T, E>;
}

impl<T, E> Stop<T, E> for Infallible {
    fn into_result(self) -> Result<T, E> {
        match self {}
    }
}

impl<T, E> Stop<T, E> for Result<T, E> {
    fn into_result(self) -> Result<T, E> {
        self
    }
}
