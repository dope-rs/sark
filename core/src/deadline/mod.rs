//! Exact, protocol-neutral exchange deadline scheduling.

use core::cmp;
use std::mem;

use o3::{
    base::time::Positive,
    depot::{
        capacity,
        heap::{self, resident},
    },
};

use crate::manifold;

pub mod config;
mod exact;
pub mod timeline;
pub use exact::Exact;
/// The exchange phase whose progress authority expires.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Phase {
    Head,
    BodyIdle,
    Total,
    Handler,
    ResponseAdmission,
}

impl Phase {
    const fn rank(self) -> u8 {
        match self {
            Self::Total => 0,
            _ => self as u8 + 1,
        }
    }

    const fn lane(self) -> usize {
        match self {
            Self::Total => 0,
            Self::Head | Self::BodyIdle => 1,
            Self::Handler | Self::ResponseAdmission => 2,
        }
    }
}

/// A positive, non-expiring exchange activation.
/// Wakes carry no [`Phase`]: runnable is not expired authority.
#[repr(u8)]
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub enum WakeReason {
    DeferredOutput,
}

/// The semantic action selected when an exchange-local instant becomes due.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Action<W = WakeReason> {
    Expire(Phase),
    Wake(W),
}

impl<W: Ord> Ord for Action<W> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match (self, other) {
            (Self::Expire(left), Self::Expire(right)) => left.rank().cmp(&right.rank()),
            (Self::Expire(_), Self::Wake(_)) => cmp::Ordering::Less,
            (Self::Wake(_), Self::Expire(_)) => cmp::Ordering::Greater,
            (Self::Wake(left), Self::Wake(right)) => left.cmp(right),
        }
    }
}

impl<W: Ord> PartialOrd for Action<W> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// One effective phase deadline for an exact exchange identity.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Scheduled<P, W = WakeReason> {
    pub at: P,
    pub action: Action<W>,
}

impl<P, W> Scheduled<P, W> {
    pub const fn expire(at: P, phase: Phase) -> Self {
        Self {
            at,
            action: Action::Expire(phase),
        }
    }

    pub const fn wake(at: P, reason: W) -> Self {
        Self {
            at,
            action: Action::Wake(reason),
        }
    }
}

impl<P: Ord, W: Ord> Ord for Scheduled<P, W> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.at
            .cmp(&other.at)
            .then_with(|| self.action.cmp(&other.action))
    }
}

impl<P: Ord, W: Ord> PartialOrd for Scheduled<P, W> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Effect of one committed book mutation on its externally armed minimum.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Mutation {
    Unchanged,
    Contents,
    Earliest,
}

impl Mutation {
    pub const fn is_earliest(self) -> bool {
        matches!(self, Self::Earliest)
    }

    fn between<P: PartialEq>(before: Option<P>, after: Option<P>) -> Self {
        if before == after {
            Self::Contents
        } else {
            Self::Earliest
        }
    }
}

impl From<heap::Change> for Mutation {
    fn from(change: heap::Change) -> Self {
        match change {
            heap::Change::Contents => Self::Contents,
            heap::Change::Minimum => Self::Earliest,
        }
    }
}

/// A schedule request that cannot name a live slot in this fixed domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error<K> {
    OutOfRange { key: K, capacity: usize },
    Conflict { requested: K, current: K },
}

/// One due entry removed from the exact book.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Expired<K, P, W = WakeReason> {
    pub key: K,
    pub scheduled: Scheduled<P, W>,
}

/// Result of visiting at most one due entry in a deadline book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "account for renewal work and preserve the current minimum"]
pub enum Poll<T> {
    Idle,
    /// An idle deadline was renewed. Account for this work before polling again,
    /// and preserve or rearm the book's current minimum.
    Renewed,
    Expired(T),
}

/// Fixed-capacity deadline storage.
pub trait Book<K: manifold::raw::Key, P: Copy + Ord, W: Copy + Ord = WakeReason> {
    fn schedule(&mut self, key: K, item: Scheduled<P, W>) -> Result<(), Error<K>>;
    fn cancel(&mut self, key: K) -> Mutation;
    fn cancel_one(&mut self, key: K, action: Action<W>) -> Mutation;
    fn peek(&self) -> Option<(K, Scheduled<P, W>)>;
    fn poll_due(&mut self, now: P) -> Poll<Expired<K, P, W>>;
    /// Drains lazy renewals until an expiry or an idle book is reached.
    /// This may visit every resident entry; event-loop turns use `poll_due` to
    /// bound renewal work before yielding to other connections.
    fn pop_due(&mut self, now: P) -> Option<Expired<K, P, W>> {
        loop {
            match self.poll_due(now) {
                Poll::Idle => return None,
                Poll::Renewed => {}
                Poll::Expired(item) => return Some(item),
            }
        }
    }
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One-allocation total, ingress, work, and wake agenda. Separate ingress and
/// work lanes allow duplex body receipt and application work to overlap.
pub struct Agenda<K: manifold::raw::Key, P: Copy + Ord, W: Copy + Ord = WakeReason> {
    heap: resident::Min<K, Scheduled<P, W>, Lane<P, W>>,
}

#[derive(Clone, Copy)]
struct Lane<P, W> {
    phases: timeline::Timeline<P>,
    wake: Option<Wake<P, W>>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct Wake<P, W = WakeReason> {
    at: P,
    reason: W,
}

const _: () = {
    assert!(mem::size_of::<Wake<u64>>() <= mem::size_of::<Scheduled<u64>>());
    assert!(mem::align_of::<Wake<u64>>() == mem::align_of::<Scheduled<u64>>());
};

enum Plan<'a, K: manifold::raw::Key, P: Copy + Ord, W: Copy + Ord> {
    Vacant(resident::Vacant<'a, K, Scheduled<P, W>, Lane<P, W>>),
    Occupied(resident::Occupied<'a, K, Scheduled<P, W>, Lane<P, W>>),
}

impl<P: Copy + Ord, W: Copy + Ord> Lane<P, W> {
    const EMPTY: Self = Self {
        phases: timeline::Timeline::EMPTY,
        wake: None,
    };

    fn get(&self, action: Action<W>) -> Option<Scheduled<P, W>> {
        self.resident(action).filter(|item| item.action == action)
    }

    fn resident(&self, action: Action<W>) -> Option<Scheduled<P, W>> {
        match action {
            Action::Expire(phase) => self
                .phases
                .resident(phase)
                .map(|item| Scheduled::expire(item.at, item.phase)),
            Action::Wake(_) => self.wake.map(|item| Scheduled::wake(item.at, item.reason)),
        }
    }

    /// Installs `item` as this lane's resident for its action and returns the lane's
    /// resulting minimum. The installed item is already included in that minimum.
    fn set(&mut self, item: Scheduled<P, W>) -> Scheduled<P, W> {
        match item.action {
            Action::Expire(phase) => {
                self.phases.set(item.at, phase);
            }
            Action::Wake(reason) => {
                self.wake = Some(Wake {
                    at: item.at,
                    reason,
                });
            }
        };
        self.next().unwrap_or(item)
    }

    fn retire(&mut self, action: Action<W>) {
        match action {
            Action::Expire(phase) => self.phases.clear_phase(phase),
            Action::Wake(reason) => {
                if self.wake.is_some_and(|item| item.reason == reason) {
                    self.wake = None;
                }
            }
        }
    }

    fn next(&self) -> Option<Scheduled<P, W>> {
        self.phases
            .next()
            .map(|item| Scheduled::expire(item.at, item.phase))
            .into_iter()
            .chain(self.wake.map(|item| Scheduled::wake(item.at, item.reason)))
            .min()
    }
}

impl<K: manifold::raw::Key, P: Copy + Ord, W: Copy + Ord> Agenda<K, P, W> {
    fn try_entry(&mut self, key: K) -> Result<Plan<'_, K, P, W>, Error<K>> {
        match self.heap.entry(key) {
            Err(error) => Err(Error::OutOfRange {
                key: error.key(),
                capacity: error.capacity(),
            }),
            Ok(resident::Entry::Vacant(entry)) => Ok(Plan::Vacant(entry)),
            Ok(resident::Entry::Occupied(entry)) => Ok(Plan::Occupied(entry)),
            Ok(resident::Entry::Conflict(entry)) => Err(Error::Conflict {
                requested: entry.requested(),
                current: entry.current(),
            }),
        }
    }

    pub fn try_new(capacity: capacity::Count) -> Result<Self, heap::Error> {
        Ok(Self {
            heap: resident::Min::try_repeat(capacity, Lane::EMPTY)?,
        })
    }

    pub fn transition<N, F>(
        &mut self,
        key: K,
        config: config::Config,
        change: timeline::Transition,
        now: N,
        add: F,
    ) -> Result<(), Error<K>>
    where
        N: Copy,
        F: FnMut(N, Positive) -> P,
    {
        if change == timeline::Transition::BodyProgress {
            let Plan::Occupied(mut entry) = self.try_entry(key)? else {
                return Ok(());
            };
            entry.resident_mut().phases.progress(config, now, add);
            return Ok(());
        }
        let plan = self.try_entry(key)?;
        if change == timeline::Transition::Retired {
            drop(add);
            if let Plan::Occupied(entry) = plan {
                entry.retire();
            }
            return Ok(());
        }
        match plan {
            Plan::Vacant(mut entry) => {
                let mut lane = Lane::EMPTY;
                if lane.phases.apply(config, change, now, add) == timeline::Change::Unchanged {
                    return Ok(());
                }
                let Some(next) = lane.next() else {
                    return Ok(());
                };
                *entry.resident_mut() = lane;
                entry.insert(next);
            }
            Plan::Occupied(mut entry) => {
                if entry.resident_mut().phases.apply(config, change, now, add)
                    == timeline::Change::Unchanged
                {
                    return Ok(());
                }
                match entry.resident().next() {
                    Some(next) if entry.priority() != &next => entry.set(next),
                    Some(_) => {}
                    None => entry.retire(),
                }
            }
        }
        Ok(())
    }
}

impl<K: manifold::raw::Key, P: Copy + Ord, W: Copy + Ord> Book<K, P, W> for Agenda<K, P, W> {
    fn schedule(&mut self, key: K, item: Scheduled<P, W>) -> Result<(), Error<K>> {
        let plan = self.try_entry(key)?;
        match plan {
            Plan::Vacant(mut entry) => {
                let mut lane = Lane::EMPTY;
                let minimum = lane.set(item);
                *entry.resident_mut() = lane;
                entry.insert(minimum);
            }
            Plan::Occupied(mut entry) => {
                if entry.resident().resident(item.action) == Some(item) {
                    return Ok(());
                }
                let minimum = entry.resident_mut().set(item);
                entry.set(minimum);
            }
        }
        Ok(())
    }

    fn cancel(&mut self, key: K) -> Mutation {
        let heap::Probe::Occupied(entry) = self.heap.probe(key) else {
            return Mutation::Unchanged;
        };
        entry.retire_min().into()
    }

    fn cancel_one(&mut self, key: K, action: Action<W>) -> Mutation {
        let heap::Probe::Occupied(mut entry) = self.heap.probe(key) else {
            return Mutation::Unchanged;
        };
        if entry.resident().get(action).is_none() {
            return Mutation::Unchanged;
        }
        let next = {
            let lane = entry.resident_mut();
            lane.retire(action);
            lane.next()
        };
        match next {
            Some(next) => entry.set_min(next).into(),
            None => entry.retire_min().into(),
        }
    }

    fn peek(&self) -> Option<(K, Scheduled<P, W>)> {
        self.heap.peek().map(|(key, item)| (key, *item))
    }

    fn poll_due(&mut self, now: P) -> Poll<Expired<K, P, W>> {
        let Some(mut entry) = self.heap.first_entry_if(|priority| !(priority.at > now)) else {
            return Poll::Idle;
        };
        let key = entry.key();
        let item = *entry.priority();
        let (next, renewed) = {
            let lane = entry.resident_mut();
            let renewed = item.action == Action::Expire(Phase::BodyIdle)
                && lane.phases.refresh(item.at) == timeline::Change::Changed;
            if !renewed {
                lane.retire(item.action);
            }
            (lane.next(), renewed)
        };
        match next {
            Some(next) => entry.set(next),
            None => entry.retire(),
        }
        if renewed {
            Poll::Renewed
        } else {
            Poll::Expired(Expired {
                key,
                scheduled: item,
            })
        }
    }

    fn len(&self) -> usize {
        self.heap.len()
    }
}
