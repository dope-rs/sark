use std::{convert::Infallible, mem};

/// Compile-time proof that a route-local failure is uninhabited.
pub trait Never {
    fn never<T>(self) -> T;
}

impl Never for Infallible {
    fn never<T>(self) -> T {
        match self {}
    }
}

const _: () = assert!(mem::size_of::<Infallible>() == 0);
