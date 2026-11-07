//! State owned by one live gRPC call.

use o3::buffer::storage;
use sark_core::life::session;

use crate::{
    details::{Kind, Mismatch},
    server::{runtime::state::active, service::handler::Production},
};

pub(crate) mod chunks;
pub(crate) mod credits;
mod handler;
pub(in crate::server) mod input;
pub(crate) mod messages;
mod opened;

#[doc(hidden)]
pub use handler::Handler;
#[doc(hidden)]
pub use opened::Opened;

pub(in crate::server) struct State<'r, T, C: session::Storage = storage::Shared> {
    pub(in crate::server) entry: active::Entry<'r>,
    phase: Phase<T, C>,
}

enum Phase<T, C: session::Storage> {
    Reserved,
    Active(Active<T, C>),
    Done,
}

pub(in crate::server) struct Active<T, C: session::Storage> {
    pub(in crate::server) input: Option<input::Input<C>>,
    pub(in crate::server) handler: T,
    pub(in crate::server) output: Production,
}

impl<T, C: session::Storage> Phase<T, C> {
    fn kind(&self) -> Kind {
        match self {
            Self::Reserved => Kind::Reserved,
            Self::Active(_) => Kind::Active,
            Self::Done => Kind::Done,
        }
    }
}

impl<'r, T, C: session::Storage> State<'r, T, C> {
    pub(in crate::server) const fn reserved(entry: active::Entry<'r>) -> Self {
        Self {
            entry,
            phase: Phase::Reserved,
        }
    }

    pub(in crate::server) fn initialize(
        &mut self,
        input: input::Input<C>,
        handler: T,
        output: Production,
    ) -> Result<(), Mismatch> {
        if !matches!(self.phase, Phase::Reserved) {
            return Err(Mismatch {
                expected: Kind::Reserved,
                actual: self.phase.kind(),
            });
        }
        self.phase = Phase::Active(Active {
            input: Some(input),
            handler,
            output,
        });
        Ok(())
    }

    pub(in crate::server) fn active(&mut self) -> Option<&mut Active<T, C>> {
        match &mut self.phase {
            Phase::Active(call) => Some(call),
            Phase::Reserved | Phase::Done => None,
        }
    }

    pub(in crate::server) fn complete(&mut self) -> Result<(), Mismatch> {
        let actual = self.phase.kind();
        let Phase::Active(_) = &self.phase else {
            return Err(Mismatch {
                expected: Kind::Active,
                actual,
            });
        };
        self.phase = Phase::Done;
        Ok(())
    }

    /// A failed request may end before initialization or after application completion.
    pub(in crate::server) fn reject(&mut self) {
        self.phase = Phase::Done;
    }

    pub(in crate::server) fn production(&self) -> Option<Production> {
        match &self.phase {
            Phase::Active(call) => Some(call.output),
            Phase::Reserved | Phase::Done => None,
        }
    }

    pub(in crate::server) fn is_done(&self) -> bool {
        matches!(self.phase, Phase::Done)
    }
}
