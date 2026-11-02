use core::convert::Infallible;

use crate::h1::head::ParseError;

mod head;

pub use head::Head;

#[derive(PartialEq, Eq)]
pub enum Admission {
    NeedMore,
    Bad(ParseError<Infallible>),
    Ready,
}

impl Admission {
    #[cold]
    pub fn admit(input: &[u8]) -> Self {
        match Head::scan(input, input.len()).map(|head| head.is_some()) {
            Ok(true) => Self::Ready,
            Ok(false) => Self::NeedMore,
            Err(cause) => Self::Bad(cause),
        }
    }
}
