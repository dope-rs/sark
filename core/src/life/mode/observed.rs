//! Enabled lifecycle observation.

use o3::buffer::hold::evidence;
use sequence::observed::{Session, Tag};

use crate::life::{self, endpoint, exchange, sequence};

#[repr(transparent)]
#[derive(Clone, Copy, Default)]
pub struct Observed<O>(O);

impl<O> Observed<O> {
    pub const fn new(observer: O) -> Self {
        Self(observer)
    }

    pub const fn observer(&self) -> &O {
        &self.0
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.0
    }

    pub fn into_inner(self) -> O {
        self.0
    }
}

impl<E, T, O> endpoint::Mode<E, T> for Observed<O>
where
    O: endpoint::Observer<E, T>,
{
    const ENABLED: bool = true;

    type Observer = O;

    fn observer(&mut self) -> Option<&mut Self::Observer> {
        Some(&mut self.0)
    }
}

impl<C: Copy, S: Copy, O> life::Mode<C, S> for Observed<O>
where
    O: life::Observer<C, S>,
{
    const ENABLED: bool = true;

    type Retention = evidence::On;
    type Observer = O;

    type Body<'a>
        = life::BodyScope<'a, C, S, O>
    where
        Self: 'a,
        C: 'a,
        S: 'a;

    fn observer(&mut self) -> Option<&mut Self::Observer> {
        Some(&mut self.0)
    }

    fn body<'a>(&'a mut self, id: exchange::Id<C, S>) -> Self::Body<'a>
    where
        C: 'a,
        S: 'a,
    {
        life::BodyScope {
            id,
            out: &mut self.0,
        }
    }
}

impl<C, O> sequence::Mode<C> for Observed<O>
where
    C: Copy,
    O: life::Observer<C, u64>,
{
    type Session = Session;
    type TransportTag = Tag;
}

const _: () = assert!(core::mem::size_of::<Observed<()>>() == 0);
