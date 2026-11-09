use o3::buffer::{hold, hold::evidence};
use sark_core::life::{self, audit, exchange, sequence};

use crate::wire::output;

pub trait Mode: evidence::Mode + Sized {
    fn complete(held: output::Retention) -> Completion<Self>;

    fn charge(proof: Self::Projection) -> hold::Charge;
}

impl Mode for evidence::Off {
    fn complete(held: output::Retention) -> Completion<Self> {
        match held {
            output::Retention::Response(owner) | output::Retention::Linked(owner) => {
                Completion::Response {
                    owner,
                    linked: Default::default(),
                }
            }
            output::Retention::Rejected => Completion::Rejected,
        }
    }

    fn charge(proof: Self::Projection) -> hold::Charge {
        let evidence::Off = proof;
        hold::Charge::default()
    }
}

impl Mode for evidence::On {
    fn complete(held: output::Retention) -> Completion<Self> {
        match held {
            output::Retention::Response(owner) => Completion::Response {
                owner,
                linked: hold::Charge::default(),
            },
            output::Retention::Linked(owner) => Completion::Response {
                owner,
                linked: owner.charge,
            },
            output::Retention::Rejected => Completion::Rejected,
        }
    }

    fn charge(proof: Self::Projection) -> hold::Charge {
        proof
    }
}

/// Final response ownership used to retire one HTTP/1 task.
pub enum Completion<R: Mode = evidence::Off> {
    Response {
        owner: output::Owner,
        linked: R::Projection,
    },
    Completed,
    Rejected,
}

/// Whether this turn first observed a task becoming ready.
#[derive(Clone, Copy)]
#[repr(u8)]
pub enum Readiness {
    Staged,
    NewlyReady,
}

/// One HTTP/1 task transition.
#[must_use]
pub enum Turn<R: Mode = evidence::Off> {
    Idle,
    Deferred,
    Pending,
    StagedDeferred,
    ReadyDeferred,
    TransportCancelled,
    ConnectionCancelled,
    StagedCancelled,
    Streaming {
        ready: Readiness,
    },
    Response {
        ready: Readiness,
        owner: output::Owner,
        linked: R::Projection,
    },
    Completed {
        ready: Readiness,
    },
    Rejected {
        ready: Readiness,
    },
}

impl<R: Mode> Turn<R> {
    pub fn retired(ready: Readiness, done: Completion<R>) -> Self {
        match done {
            Completion::Response { owner, linked } => Self::Response {
                ready,
                owner,
                linked,
            },
            Completion::Completed => Self::Completed { ready },
            Completion::Rejected => Self::Rejected { ready },
        }
    }

    pub fn settle<C: Copy, M: sequence::Mode<C, Retention = R>>(
        self,
        mode: &mut M,
        session: &mut M::Session,
        id: exchange::Id<C, u64>,
    ) {
        match self {
            Self::Idle | Self::Deferred => (),
            Self::Pending => {
                if let Some(out) = life::Mode::observer(mode) {
                    life::Observer::observe(out, life::Event::TaskPollPending(id));
                }
            }
            Self::StagedDeferred => {
                Self::audit(mode, id, 1);
            }
            Self::ReadyDeferred => {
                if let Some(out) = life::Mode::observer(mode) {
                    life::Observer::observe(out, life::Event::TaskPollReady(id));
                }
                Self::audit(mode, id, 1);
            }
            Self::TransportCancelled => {
                Self::cancel(mode, id, life::Outcome::Rejected);
            }
            Self::ConnectionCancelled => {
                Self::cancel(mode, id, life::Outcome::Cancelled);
            }
            Self::StagedCancelled => {
                Self::audit(mode, id, 0);
                id.retire(mode, life::Outcome::Cancelled);
            }
            Self::Streaming { ready } => {
                Self::ready(mode, id, ready, 1);
                sequence::Mode::commit(mode, session, id, 0, hold::Charge::default());
                sequence::Mode::accept(mode, session, id, hold::Charge::default());
            }
            Self::Response {
                ready,
                owner,
                linked,
            } => {
                Self::ready(mode, id, ready, 0);
                let charge = owner.charge;
                if M::ENABLED {
                    sequence::Mode::commit_link(
                        mode,
                        session,
                        id,
                        owner.len,
                        charge,
                        R::charge(linked),
                    );
                }
                sequence::Mode::accept(mode, session, id, charge);
                id.retire(mode, life::Outcome::Completed);
            }
            Self::Completed { ready } => {
                Self::ready(mode, id, ready, 0);
                id.retire(mode, life::Outcome::Completed);
            }
            Self::Rejected { ready } => {
                Self::ready(mode, id, ready, 0);
                id.retire(mode, life::Outcome::Rejected);
            }
        }
    }

    fn ready<C: Copy, M: life::Mode<C, u64>>(
        mode: &mut M,
        id: exchange::Id<C, u64>,
        ready: Readiness,
        slots: usize,
    ) {
        match ready {
            Readiness::Staged => Self::audit(mode, id, slots),
            Readiness::NewlyReady => {
                if let Some(out) = life::Mode::observer(mode) {
                    life::Observer::observe(out, life::Event::TaskPollReady(id));
                }
                if slots != 0 {
                    Self::audit(mode, id, slots);
                }
            }
        }
    }

    fn cancel<C: Copy, M: life::Mode<C, u64>>(
        mode: &mut M,
        id: exchange::Id<C, u64>,
        state: life::Outcome,
    ) {
        if let Some(out) = life::Mode::observer(mode) {
            life::Observer::observe(out, life::Event::TaskCancelled(id));
        }
        id.retire(mode, state);
    }

    fn audit<C: Copy, M: life::Mode<C, u64>>(mode: &mut M, id: exchange::Id<C, u64>, slots: usize) {
        let Some(out) = life::Mode::observer(mode) else {
            return;
        };
        life::Observer::audit(
            out,
            id.connection(),
            audit::Snapshot::default().task_slots(slots),
        );
    }
}

const _: () = {
    assert!(size_of::<Readiness>() == size_of::<bool>());
    assert!(align_of::<Readiness>() == align_of::<bool>());
    assert!(size_of::<Completion>() <= 5 * size_of::<usize>());
    assert!(size_of::<Turn>() <= 5 * size_of::<usize>());
    assert!(size_of::<Completion<evidence::On>>() <= usize::BITS as usize);
    assert!(size_of::<Turn<evidence::On>>() <= usize::BITS as usize);
};
