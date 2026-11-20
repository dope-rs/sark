use o3::buffer::{hold, hold::evidence};
use sark_core::life::{self, audit, exchange, sequence};

use crate::wire::output;

pub trait Mode: evidence::Mode {
    type Link: Default;

    const LINKED: Self::Link;

    fn charge(linked: Self::Link, owner: hold::Charge) -> hold::Charge;
}

impl Mode for evidence::Off {
    type Link = ();
    const LINKED: () = ();

    fn charge((): (), _: hold::Charge) -> hold::Charge {
        hold::Charge::default()
    }
}

impl Mode for evidence::On {
    type Link = bool;
    const LINKED: bool = true;

    fn charge(linked: bool, owner: hold::Charge) -> hold::Charge {
        if linked {
            owner
        } else {
            hold::Charge::default()
        }
    }
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
        linked: R::Link,
    },
    Completed {
        ready: Readiness,
    },
    Rejected {
        ready: Readiness,
    },
}

impl<R: Mode> Turn<R> {
    pub fn published(ready: Readiness, held: Option<output::Origin>) -> Self {
        let (owner, linked) = match held {
            Some(output::Origin::Response(owner)) => (owner, R::Link::default()),
            Some(output::Origin::Linked(owner)) => (owner, R::LINKED),
            None => return Self::Rejected { ready },
        };
        Self::Response {
            ready,
            owner,
            linked,
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
                        R::charge(linked, charge),
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
    assert!(size_of::<Turn>() <= 5 * size_of::<usize>());
    assert!(size_of::<Turn<evidence::On>>() <= 5 * size_of::<usize>());
};
