use std::cmp;

use o3::base::time::Positive;

use crate::deadline::{self, config};

const PHASE_LANES: usize = 3;

/// One scheduled phase expiry.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Expiry<P> {
    pub at: P,
    pub phase: deadline::Phase,
}

impl<P: Ord> Ord for Expiry<P> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.at
            .cmp(&other.at)
            .then_with(|| self.phase.rank().cmp(&other.phase.rank()))
    }
}

impl<P: Ord> PartialOrd for Expiry<P> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// One protocol-neutral exchange lifetime transition.
#[repr(u8)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Transition {
    /// Starts the exchange without extending a total admitted by its raw head.
    Open,
    AwaitingHead,
    HeadAdmitted,
    InputAdmitted,
    BodyProgress,
    InputFinished,
    HandlerStarted,
    ResponseReady,
    ResponseCommitted,
    Retired,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Change {
    Unchanged,
    Changed,
}

impl Change {
    const fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unchanged, Self::Unchanged) => Self::Unchanged,
            (Self::Unchanged, Self::Changed)
            | (Self::Changed, Self::Unchanged)
            | (Self::Changed, Self::Changed) => Self::Changed,
        }
    }
}

/// Linear total, ingress, and work lanes for one exchange.
/// Body progress stays lazy until an enclosing schedule visits the old root.
#[derive(Clone, Copy)]
pub struct Timeline<P> {
    scheduled: [Option<Expiry<P>>; PHASE_LANES],
    body_progress: Option<P>,
}

impl<P: Copy + Ord> Timeline<P> {
    pub const EMPTY: Self = Self {
        scheduled: [None; PHASE_LANES],
        body_progress: None,
    };

    pub fn transition<N, F>(
        &mut self,
        config: config::Config,
        step: Transition,
        now: N,
        mut add: F,
    ) -> deadline::Mutation
    where
        N: Copy,
        F: FnMut(N, Positive) -> P,
    {
        let before = self.next();
        let changed = self.apply(config, step, now, &mut add);
        if changed == Change::Unchanged {
            deadline::Mutation::Unchanged
        } else {
            deadline::Mutation::between(before, self.next())
        }
    }

    /// Applies phase mutations and releases its callback before returning.
    /// Callers retaining callback ownership pass a mutable borrow.
    pub(super) fn apply<N, F>(
        &mut self,
        config: config::Config,
        step: Transition,
        now: N,
        mut add: F,
    ) -> Change
    where
        N: Copy,
        F: FnMut(N, Positive) -> P,
    {
        match step {
            Transition::Open => {
                if self.get(deadline::Phase::Total).is_some() {
                    return Change::Unchanged;
                }
                let total = Self::instant(config, deadline::Phase::Total, now, &mut add);
                self.insert_at(total, deadline::Phase::Total)
            }
            Transition::AwaitingHead => {
                let total = Self::instant(config, deadline::Phase::Total, now, &mut add);
                let head = Self::instant(config, deadline::Phase::Head, now, &mut add);
                self.insert_at(total, deadline::Phase::Total)
                    .merge(self.insert_at(head, deadline::Phase::Head))
            }
            Transition::HeadAdmitted => {
                let body = Self::instant(config, deadline::Phase::BodyIdle, now, &mut add);
                self.remove(deadline::Phase::Head)
                    .merge(self.insert_at(body, deadline::Phase::BodyIdle))
            }
            Transition::InputAdmitted => self.remove(deadline::Phase::Head),
            Transition::BodyProgress => self.progress(config, now, add),
            Transition::InputFinished => self.remove(deadline::Phase::BodyIdle),
            Transition::HandlerStarted => {
                let handler = Self::instant(config, deadline::Phase::Handler, now, &mut add);
                self.insert_at(handler, deadline::Phase::Handler)
            }
            Transition::ResponseReady => {
                let admit =
                    Self::instant(config, deadline::Phase::ResponseAdmission, now, &mut add);
                self.remove(deadline::Phase::Handler)
                    .merge(self.insert_at(admit, deadline::Phase::ResponseAdmission))
            }
            Transition::ResponseCommitted => self
                .remove(deadline::Phase::Handler)
                .merge(self.remove(deadline::Phase::ResponseAdmission)),
            Transition::Retired => self.clear(),
        }
    }

    pub fn next(&self) -> Option<Expiry<P>> {
        self.scheduled.iter().flatten().copied().min()
    }

    pub fn pop_due(&mut self, now: P) -> Option<Expiry<P>> {
        loop {
            let item = self.next().filter(|item| item.at <= now)?;
            if self.refresh(item.at) == Change::Changed {
                continue;
            }
            self.clear_phase(item.phase);
            return Some(item);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.scheduled.iter().all(Option::is_none)
    }

    pub(super) fn get(&self, phase: deadline::Phase) -> Option<Expiry<P>> {
        self.resident(phase).filter(|item| item.phase == phase)
    }

    pub(super) fn resident(&self, phase: deadline::Phase) -> Option<Expiry<P>> {
        self.scheduled[phase.lane()]
    }

    pub(super) fn insert(&mut self, at: P, phase: deadline::Phase) -> Change {
        let lane = phase.lane();
        let item = Expiry { at, phase };
        if self.scheduled[lane] == Some(item) {
            return Change::Unchanged;
        }
        self.set(at, phase);
        Change::Changed
    }

    pub(super) fn set(&mut self, at: P, phase: deadline::Phase) {
        let lane = phase.lane();
        if lane == deadline::Phase::BodyIdle.lane() {
            self.body_progress = None;
        }
        self.scheduled[lane] = Some(Expiry { at, phase });
    }

    fn remove(&mut self, phase: deadline::Phase) -> Change {
        let lane = phase.lane();
        let Some(item) = self.scheduled[lane] else {
            return Change::Unchanged;
        };
        if item.phase != phase {
            return Change::Unchanged;
        }
        self.clear_phase(phase);
        Change::Changed
    }

    pub(super) fn clear_phase(&mut self, phase: deadline::Phase) {
        self.scheduled[phase.lane()] = None;
        if phase == deadline::Phase::BodyIdle {
            self.body_progress = None;
        }
    }

    /// Checks the active idle lane once before evaluating and retaining progress.
    pub(super) fn progress<N, F>(&mut self, config: config::Config, now: N, mut add: F) -> Change
    where
        N: Copy,
        F: FnMut(N, Positive) -> P,
    {
        if self.get(deadline::Phase::BodyIdle).is_none() {
            return Change::Unchanged;
        }
        let Some(at) = Self::instant(config, deadline::Phase::BodyIdle, now, &mut add) else {
            return Change::Unchanged;
        };
        if self.body_progress.is_some_and(|idle| idle >= at) {
            return Change::Unchanged;
        }
        self.body_progress = Some(at);
        Change::Changed
    }

    pub(super) fn refresh(&mut self, current: P) -> Change {
        let Some(at) = self.body_progress.take() else {
            return Change::Unchanged;
        };
        if at <= current {
            return Change::Unchanged;
        }
        self.scheduled[deadline::Phase::BodyIdle.lane()] = Some(Expiry {
            at,
            phase: deadline::Phase::BodyIdle,
        });
        Change::Changed
    }

    pub(super) fn clear(&mut self) -> Change {
        if self.is_empty() && self.body_progress.is_none() {
            return Change::Unchanged;
        }
        *self = Self::EMPTY;
        Change::Changed
    }

    fn insert_at(&mut self, at: Option<P>, phase: deadline::Phase) -> Change {
        match at {
            Some(at) => self.insert(at, phase),
            None => Change::Unchanged,
        }
    }

    fn instant<N, F>(
        config: config::Config,
        phase: deadline::Phase,
        now: N,
        add: &mut F,
    ) -> Option<P>
    where
        N: Copy,
        F: FnMut(N, Positive) -> P,
    {
        match config.window(phase) {
            config::Window::Disabled => None,
            config::Window::Finite(window) => Some(add(now, window)),
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Transition>() == 1);
    assert!(std::mem::align_of::<Transition>() == 1);
    assert!(!std::mem::needs_drop::<Transition>());
    assert!(std::mem::size_of::<Expiry<u64>>() <= std::mem::size_of::<deadline::Scheduled<u64>>());
    assert!(
        std::mem::align_of::<Expiry<u64>>() == std::mem::align_of::<deadline::Scheduled<u64>>()
    );
};
