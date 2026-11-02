use dope::runtime::civil;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Civil(civil::Error),
    Range,
}

impl From<civil::Error> for Error {
    fn from(error: civil::Error) -> Self {
        Self::Civil(error)
    }
}
