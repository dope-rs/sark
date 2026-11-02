use crate::{
    frame, manifold,
    manifold::{requests::book::timers, server::deadline},
};

pub(in crate::manifold::server) enum Retire {
    Target(manifold::Error),
    Schedule(timers::Error),
}

impl Retire {
    pub(in crate::manifold::server) fn code(self) -> frame::ErrorCode {
        match self {
            Self::Target(error) => error.code(),
            Self::Schedule(error) => deadline::code(error),
        }
    }
}
