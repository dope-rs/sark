use crate::{deadline, life};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id<C, S> {
    connection: C,
    stream: S,
}

impl<C, S> Id<C, S> {
    pub const fn new(conn: C, stream: S) -> Self {
        Self {
            connection: conn,
            stream,
        }
    }
}

impl<C: Copy, S: Copy> Id<C, S> {
    pub const fn connection(self) -> C {
        self.connection
    }

    pub const fn stream(self) -> S {
        self.stream
    }

    pub fn begin_expiry<M: life::Mode<C, S>>(
        self,
        mode: &mut M,
        phase: deadline::Phase,
    ) -> life::Expiry<C, S> {
        mode.observe(life::Event::ExchangeDeadlineExpired {
            exchange: self,
            phase,
        });
        life::Expiry::new(self, phase)
    }

    pub fn retire<M: life::Mode<C, S>>(self, mode: &mut M, outcome: life::Outcome) {
        mode.observe(life::Event::RequestOwnerScopeClosed(self));
        mode.observe(life::Event::Terminal {
            exchange: self,
            outcome,
        });
        mode.observe(life::Event::ExchangeRetired(self));
    }
}
