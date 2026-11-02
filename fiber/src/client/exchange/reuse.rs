use std::{mem, time};

use dope::core::driver::schedule::clock;
use dope_fiber::reply::order::Depth;
use o3::base::time::{Positive, Zero};
use sark_http::h1::framing;

const KEEP_ALIVE_MARGIN: time::Duration = time::Duration::from_secs(1);

/// The locally-authoritative upper bound for an idle connection lease.
/// Peer persistence metadata may only shorten this budget.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Policy {
    maximum: Budget,
}

impl Policy {
    pub(super) const fn new(maximum: time::Duration) -> Self {
        Self {
            maximum: Budget::new(maximum),
        }
    }

    pub(super) fn available<'d>(self, now: impl FnOnce() -> clock::Moment<'d>) -> Admission<'d> {
        match self.maximum {
            Budget::Retired => Admission::Fresh,
            Budget::Reusable(budget) => Admission::Idle(Lease::new(now(), budget)),
        }
    }

    const fn is_disabled(self) -> bool {
        matches!(self.maximum, Budget::Retired)
    }

    fn admit(self, control: framing::Control) -> Budget {
        let Budget::Reusable(maximum) = self.maximum else {
            return Budget::Retired;
        };
        if !control.is_reusable() {
            return Budget::Retired;
        }
        let peer = control
            .timeout()
            .map(time::Duration::from_secs)
            .map(|timeout| timeout.saturating_sub(KEEP_ALIVE_MARGIN));
        match peer {
            Some(timeout) => Budget::new(maximum.get().min(timeout)),
            None => Budget::Reusable(maximum),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Lease<'d> {
    since: clock::Moment<'d>,
    budget: Positive,
}

impl<'d> Lease<'d> {
    const fn new(since: clock::Moment<'d>, budget: Positive) -> Self {
        Self { since, budget }
    }

    fn is_live(self, now: clock::Moment<'d>) -> bool {
        now.clamp_since(self.since) < self.budget.get()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Admission<'d> {
    Reserved,
    Fresh,
    Active,
    Idle(Lease<'d>),
    Sealed,
}

pub(super) enum Claim {
    Ready,
    Expired,
    Unavailable,
}

impl<'d> Admission<'d> {
    pub(super) const fn can_queue(self) -> bool {
        matches!(self, Self::Fresh | Self::Active | Self::Idle(_))
    }

    pub(super) const fn idle_window(self) -> Option<Positive> {
        match self {
            Self::Idle(lease) => Some(lease.budget),
            Self::Reserved | Self::Fresh | Self::Active | Self::Sealed => None,
        }
    }

    pub(super) fn claim(self, now: impl FnOnce() -> clock::Moment<'d>) -> Claim {
        match self {
            Self::Fresh | Self::Active => Claim::Ready,
            Self::Idle(lease) if lease.is_live(now()) => Claim::Ready,
            Self::Idle(_) => Claim::Expired,
            Self::Reserved | Self::Sealed => Claim::Unavailable,
        }
    }

    pub(super) const fn submitted(self, policy: Policy) -> Self {
        match self {
            Self::Sealed => Self::Sealed,
            Self::Fresh | Self::Reserved if policy.is_disabled() => Self::Sealed,
            Self::Fresh | Self::Reserved | Self::Active | Self::Idle(_) => Self::Active,
        }
    }

    pub(super) fn released(self, policy: Policy, now: impl FnOnce() -> clock::Moment<'d>) -> Self {
        match self {
            Self::Reserved => policy.available(now),
            state => state,
        }
    }

    pub(super) fn completed(
        self,
        budget: Budget,
        depth: Depth,
        now: impl FnOnce() -> clock::Moment<'d>,
    ) -> Self {
        if matches!(self, Self::Sealed) {
            return Self::Sealed;
        }
        let Budget::Reusable(budget) = budget else {
            return Self::Sealed;
        };
        if depth != Depth::ZERO {
            Self::Active
        } else {
            Self::Idle(Lease::new(now(), budget))
        }
    }
}

/// A response's complete, locally-bounded reuse decision.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Budget {
    #[default]
    Retired,
    Reusable(Positive),
}

impl Budget {
    pub(super) const fn new(duration: time::Duration) -> Self {
        match Positive::try_new(duration) {
            Ok(duration) => Self::Reusable(duration),
            Err(Zero) => Self::Retired,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum Decision {
    Reuse,
    Retire,
}

#[derive(Default)]
#[repr(transparent)]
pub struct Reuse {
    pending: Budget,
}

impl Reuse {
    /// Records the current final response's bounded reuse decision.
    pub(super) fn observe(&mut self, policy: Policy, control: framing::Control) -> Decision {
        let budget = policy.admit(control);
        self.pending = budget;
        match budget {
            Budget::Reusable(_) => Decision::Reuse,
            Budget::Retired => Decision::Retire,
        }
    }

    pub(super) fn reject(&mut self) {
        self.pending = Budget::Retired;
    }

    pub(super) fn finish(&mut self) -> Budget {
        mem::take(&mut self.pending)
    }
}

const _: () = assert!(core::mem::size_of::<Reuse>() == core::mem::size_of::<time::Duration>());
const _: () = assert!(core::mem::size_of::<Decision>() == core::mem::size_of::<bool>());
