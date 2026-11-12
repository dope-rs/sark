/// Preserves the producing protocol or application failure.
#[derive(Debug)]
pub enum Error<A> {
    Connection(crate::conn::Error),
    Application(A),
}

impl<A> From<crate::conn::Error> for Error<A> {
    fn from(error: crate::conn::Error) -> Self {
        Self::Connection(error)
    }
}
