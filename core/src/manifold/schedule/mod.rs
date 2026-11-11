//! Allocation-free scheduling over carrier-owned stream records.

pub mod data;
pub mod typed;

mod drive;
mod occupied;
mod policy;
mod records;
mod runtime;
mod vacant;

use std::marker;

use o3::{
    base::cell::region,
    depot::{array::Index, map::direct},
};
pub use occupied::Occupied;
pub use records::Records;
pub use runtime::Runtime;
pub use vacant::Vacant;

use crate::manifold;
use drive::TurnOutcome;

const LANES: usize = 7;
const CONTROL_READY: Index<LANES> = Index::at::<0>();
const DATA_READY: Index<LANES> = Index::at::<1>();
const CONNECTION_WAIT: Index<LANES> = Index::at::<2>();
const STREAM_WAIT: Index<LANES> = Index::at::<3>();
const CONTROL_EGRESS_WAIT: Index<LANES> = Index::at::<4>();
const CONTROL_CONNECTION_WAIT: Index<LANES> = Index::at::<5>();
const CONTROL_STREAM_WAIT: Index<LANES> = Index::at::<6>();
const ALL: [Index<LANES>; LANES] = [
    CONTROL_READY,
    DATA_READY,
    CONNECTION_WAIT,
    STREAM_WAIT,
    CONTROL_EGRESS_WAIT,
    CONTROL_CONNECTION_WAIT,
    CONTROL_STREAM_WAIT,
];

/// Physical resource that owns a blocked stream's next wakeup.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Wait {
    Connection,
    Stream,
    Egress,
}

/// Static classification of one carrier-native blocked result.
pub trait Block: Copy + Eq {
    fn wait(self) -> Wait;
}

/// A carrier wakeup delivered to its resident scheduling domain.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Wake<K> {
    Connection,
    Stream(K),
}

/// Whether owned work still prevents retirement.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Quiescence {
    Busy,
    Quiet,
}

const _: () = {
    assert!(core::mem::size_of::<Quiescence>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Quiescence>() == core::mem::align_of::<bool>());
};

/// Whether this turn released carrier egress capacity.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Egress {
    Unchanged,
    Released,
}

const _: () = {
    assert!(core::mem::size_of::<Egress>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Egress>() == core::mem::align_of::<bool>());
};

/// Result of one bounded egress turn.
#[derive(Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Turn<B> {
    Complete,
    Progress,
    Blocked(B),
    Idle,
}

/// The statically selected kind of a stream quantum.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WorkClass {
    Control,
    Data,
}

impl WorkClass {
    const fn ready_lane(self) -> Index<LANES> {
        match self {
            Self::Control => CONTROL_READY,
            Self::Data => DATA_READY,
        }
    }

    const fn blocked_lane(self, blocked: Option<Wait>) -> Index<LANES> {
        match (blocked, self) {
            (None, class) => class.ready_lane(),
            (Some(Wait::Egress), Self::Control) => CONTROL_EGRESS_WAIT,
            (Some(Wait::Egress), Self::Data) => DATA_READY,
            (Some(Wait::Connection), Self::Control) => CONTROL_CONNECTION_WAIT,
            (Some(Wait::Connection), Self::Data) => CONNECTION_WAIT,
            (Some(Wait::Stream), Self::Control) => CONTROL_STREAM_WAIT,
            (Some(Wait::Stream), Self::Data) => STREAM_WAIT,
        }
    }
}

/// One control quantum and its complete scheduling continuation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ControlTurn<B> {
    step: Step<B>,
    activity: WorkClass,
}

impl<B> ControlTurn<B> {
    pub const fn progress(next: WorkClass) -> Self {
        Self {
            step: Step::Progress(next),
            activity: WorkClass::Control,
        }
    }

    pub const fn blocked(block: B) -> Self {
        Self {
            step: Step::Blocked(block),
            activity: WorkClass::Control,
        }
    }

    pub const fn complete() -> Self {
        Self {
            step: Step::Complete,
            activity: WorkClass::Control,
        }
    }

    pub const fn idle() -> Self {
        Self {
            step: Step::Idle,
            activity: WorkClass::Control,
        }
    }

    fn into_parts(self) -> (Step<B>, WorkClass) {
        (self.step, self.activity)
    }
}

impl<B: Copy> ControlTurn<B> {
    pub const fn new(egress: Turn<B>, next: WorkClass) -> Self {
        Self::from_turn(egress, next, WorkClass::Control)
    }

    pub const fn fused(egress: Turn<B>, next: WorkClass) -> Self {
        Self::from_turn(egress, next, WorkClass::Data)
    }

    const fn from_turn(egress: Turn<B>, next: WorkClass, activity: WorkClass) -> Self {
        Self {
            step: DataTurn::new(egress, next),
            activity,
        }
    }
}

/// One data quantum and its complete scheduling continuation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DataTurn<B> {
    Complete,
    Progress(WorkClass),
    Blocked(B),
    Idle,
}

impl<B: Copy> DataTurn<B> {
    pub const fn new(egress: Turn<B>, next: WorkClass) -> Self {
        match egress {
            Turn::Complete => Self::Complete,
            Turn::Progress => Self::Progress(next),
            Turn::Blocked(block) => Self::Blocked(block),
            Turn::Idle => Self::Idle,
        }
    }
}

/// The same scheduling steps as [`DataTurn`], named for the control quantum.
type Step<B> = DataTurn<B>;

/// Exact storage destination after a terminal event.
pub enum Settlement<T> {
    Keep(T),
    Remove,
}

/// The terminal event delivered to a record owner.
pub enum Terminal<E> {
    Complete,
    Idle,
    Failed(E),
}

/// Whether a successful record transaction also makes it runnable.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum Activation<B> {
    #[default]
    Keep,
    Activate {
        blocked: Option<B>,
        class: WorkClass,
    },
}

impl<B> Activation<B> {
    pub const fn data(blocked: Option<B>) -> Self {
        Self::Activate {
            blocked,
            class: WorkClass::Data,
        }
    }

    pub const fn control(blocked: Option<B>) -> Self {
        Self::Activate {
            blocked,
            class: WorkClass::Control,
        }
    }
}

/// Failure to update one exact resident stream.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error<E> {
    Missing,
    Rejected(E),
}

/// Terminal policy for one payload-specific resident state.
pub trait Settle<T> {
    type Key: manifold::raw::Key;
    type Error;

    fn settle(
        &mut self,
        stream: Self::Key,
        value: T,
        terminal: Terminal<Self::Error>,
    ) -> Settlement<T>;
}

#[repr(transparent)]
pub struct Decision<'id, K> {
    outcome: TurnOutcome<K>,
    _scope: marker::PhantomData<region::Identity<'id>>,
}

impl<'id, K> Decision<'id, K> {
    fn bind(outcome: TurnOutcome<K>, _scope: region::Identity<'id>) -> Self {
        Self {
            outcome,
            _scope: marker::PhantomData,
        }
    }

    const fn running(stream: K, activity: WorkClass) -> TurnOutcome<K> {
        TurnOutcome::new(stream, activity, drive::Effect::None)
    }

    const fn blocked(stream: K, activity: WorkClass) -> TurnOutcome<K> {
        TurnOutcome::new(stream, activity, drive::Effect::BlockData)
    }
}

/// Payload-specific state machine driven inside one resident domain.
pub trait Driver<T>: Settle<T> {
    type Block: Block;
    type Permit;

    fn permit(&mut self) -> Option<Self::Permit>;

    fn control<'id>(
        &mut self,
        permit: Self::Permit,
        entry: Occupied<'_, Self::Key, T, Self::Block, region::Identity<'id>>,
    ) -> Decision<'id, Self::Key>;

    fn data(
        &mut self,
        permit: Self::Permit,
        stream: Self::Key,
        value: &mut T,
    ) -> Result<DataTurn<Self::Block>, Self::Error>;
}

/// Exact amount of scheduler work performed by one drive.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Report {
    pub turns: usize,
    pub completed: usize,
}

/// Admission commits the resident owner or returns its exact rejection.
pub type InsertResult<K, T> = Result<(), Insert<K, T>>;

/// Exact rejection from one bounded resident-domain insertion.
#[must_use = "a rejected resident owner must be handled"]
#[derive(Debug)]
pub enum Insert<K, T> {
    Full {
        stream: K,
        value: T,
        used: usize,
        limit: usize,
    },
    Occupied {
        stream: K,
        stored: K,
        value: T,
    },
    Excluded {
        stream: K,
        value: T,
        index: usize,
        capacity: usize,
    },
}

impl<K: manifold::raw::Key, T> Insert<K, T> {
    pub(super) fn from_direct(error: direct::Error<K>, value: T) -> Self {
        match error {
            direct::Error::Outside {
                key,
                index,
                capacity,
            } => Self::Excluded {
                stream: key,
                value,
                index,
                capacity,
            },
            direct::Error::Occupied { key, stored } => Self::Occupied {
                stream: key,
                stored,
                value,
            },
        }
    }
}
