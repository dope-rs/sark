//! Exchange deadlines over HTTP/1's listener-owned application root.

use core::mem;

use dope::{
    core::driver::{route::Identity, schedule::clock},
    manifold::network::listener::{conn, handler},
    net::{link::egress::data, wire},
};
use o3::alloc;
use sark_core::{
    deadline,
    life::{exchange, sequence},
};

mod domain;
pub mod policies;
pub mod view;

#[derive(Clone, Copy, Debug)]
pub enum BindError {
    Allocation(alloc::Error),
}

/// Minimal listener-root mutation selected after one application turn.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Root<P> {
    Unchanged,
    Ensure,
    Arm(P),
    Cancel,
}

/// Exact input evidence accumulated across one listener callback.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum Head {
    #[default]
    Unchanged,
    Pending,
    /// The head is admitted and request body input is still pending.
    Body,
    Ready,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum Body {
    #[default]
    None,
    Progress,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub struct Progress {
    head: Head,
    body: Body,
}

impl Progress {
    pub const fn new(head: Head, body: Body) -> Self {
        Self { head, body }
    }

    pub fn merge(&mut self, next: Self) {
        if !matches!(next.head, Head::Unchanged) {
            self.head = next.head;
        }
        if matches!(next.body, Body::Progress) {
            self.body = Body::Progress;
        }
    }

    pub const fn head(self) -> Head {
        self.head
    }

    pub const fn body(self) -> Body {
        self.body
    }
}

const _: () = {
    assert!(mem::size_of::<Progress>() == 2);
    assert!(mem::align_of::<Progress>() == 1);
};

impl<'d> Root<clock::Deadline<'d>> {
    pub fn resolve(self, now: clock::Moment<'d>) -> Option<handler::Deadline<'d>> {
        match self {
            Self::Ensure => Some(handler::Deadline::Ensure(now)),
            Self::Arm(deadline) => Some(handler::Deadline::Arm(deadline)),
            Self::Cancel => Some(handler::Deadline::Clear),
            Self::Unchanged => None,
        }
    }

    pub fn try_apply<ID: Identity, W, C, B>(
        self,
        conn: &mut conn::Ctx<'_, '_, '_, '_, '_, 'd, ID, W, C, B>,
        now: clock::Moment<'d>,
    ) -> Result<(), conn::deadline::Error>
    where
        W: wire::Spec,
        B: data::raw::Payload<'d>,
    {
        match self {
            Self::Unchanged => Ok(()),
            Self::Ensure => conn.deadline().try_ensure(now),
            Self::Arm(deadline) => conn.deadline().try_arm(deadline),
            Self::Cancel => {
                conn.deadline().cancel();
                Ok(())
            }
        }
    }
}

/// Exact owner whose deadline expired.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Expiry<C> {
    Connection,
    Exchange {
        exchange: exchange::Id<C, u64>,
        phase: deadline::Phase,
    },
}

/// Result of consuming one listener application timeout.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Due<C, P> {
    pub expiry: Option<Expiry<C>>,
    pub root: Root<P>,
}

/// Runtime deadline control carried by every bound H1 lifecycle mode.
pub trait Mode<'d, ID: Identity>: sequence::Mode<conn::Id<'d, ID>> {
    fn begin(&mut self, now: clock::Moment<'d>) -> clock::Moment<'d>;
    fn finish(
        &mut self,
        conn: conn::Id<'d, ID>,
        now: clock::Moment<'d>,
        head: Head,
        body: Body,
    ) -> Root<clock::Deadline<'d>>;
    fn timeout(
        &mut self,
        conn: conn::Id<'d, ID>,
        now: clock::Moment<'d>,
    ) -> Due<conn::Id<'d, ID>, clock::Deadline<'d>>;
}
