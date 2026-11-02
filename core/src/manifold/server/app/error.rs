/// Preserves the producing protocol or application failure.
#[derive(Debug)]
pub enum Error<C, A> {
    Connection(C),
    Application(A),
}

impl<C, A> From<C> for Error<C, A> {
    fn from(error: C) -> Self {
        Self::Connection(error)
    }
}
