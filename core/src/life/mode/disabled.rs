//! Disabled lifecycle observation.

use std::convert::Infallible;

use o3::buffer::hold::evidence;

use crate::{
    body::credit,
    life,
    life::{endpoint, exchange, sequence},
};

#[derive(Clone, Copy, Default)]
pub struct Disabled;

impl<C: Copy, S: Copy> life::Mode<C, S> for Disabled {
    const ENABLED: bool = false;

    type Retention = evidence::Off;
    type Observer = Infallible;

    type Body<'a>
        = credit::Disabled
    where
        Self: 'a,
        C: 'a,
        S: 'a;

    fn observer(&mut self) -> Option<&mut Self::Observer> {
        None
    }

    fn body<'a>(&'a mut self, _: exchange::Id<C, S>) -> Self::Body<'a>
    where
        C: 'a,
        S: 'a,
    {
        credit::Disabled::new()
    }
}

impl<E, O> endpoint::Mode<E, O> for Disabled {
    const ENABLED: bool = false;

    type Observer = Infallible;

    fn observer(&mut self) -> Option<&mut Self::Observer> {
        None
    }
}

impl<C: Copy> sequence::Mode<C> for Disabled {
    type Session = ();
    type TransportTag = ();
    type Trace = Infallible;

    fn trace(&self) -> Option<&Self::Trace> {
        None
    }

    fn trace_mut(&mut self) -> Option<&mut Self::Trace> {
        None
    }
}
