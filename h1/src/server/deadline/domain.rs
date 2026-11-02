//! Connection-generation keyed storage for finite H1 exchange deadlines.

use std::mem;

use dope::{
    core::driver::{
        route::{Identity, KeyTag},
        schedule::{clock, ready::task::owner},
    },
    manifold::network::listener::conn,
};
use o3::alloc;
use sark_core::{
    deadline::{config, timeline},
    life::{self, exchange},
};

use crate::server::deadline;

#[derive(Clone, Copy)]
enum Activity<'d, ID: Identity> {
    Idle(conn::Id<'d, ID>),
    Awaiting(conn::Id<'d, ID>),
    Admitted(exchange::Id<conn::Id<'d, ID>, u64>),
}

impl<'d, ID: Identity> Activity<'d, ID> {
    const fn connection(self) -> conn::Id<'d, ID> {
        match self {
            Self::Idle(conn) | Self::Awaiting(conn) => conn,
            Self::Admitted(exchange) => exchange.connection(),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Root {
    Clean,
    Dirty,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Scan {
    Dirty,
    All,
}

#[derive(Clone, Copy)]
struct Bound<'d, ID: Identity> {
    activity: Activity<'d, ID>,
    root: Root,
    timeline: timeline::Timeline<clock::Deadline<'d>>,
}

impl<'d, ID: Identity> Bound<'d, ID> {
    const fn new(conn: conn::Id<'d, ID>) -> Self {
        Self {
            activity: Activity::Idle(conn),
            root: Root::Clean,
            timeline: timeline::Timeline::EMPTY,
        }
    }

    /// Whether this binding still belongs to the exact session that owns `conn`.
    fn belongs(&self, conn: conn::Id<'d, ID>) -> bool {
        self.activity.connection() == conn
    }

    fn transition(
        &mut self,
        config: config::Config,
        change: timeline::Transition,
        now: clock::Moment<'d>,
    ) {
        let mutation = self
            .timeline
            .transition(config, change, now, clock::Moment::clamp_add);
        if mutation.is_earliest() {
            self.root = Root::Dirty;
        }
    }
}

/// One startup allocation holding every H1 connection's active exchange timeline.
pub(super) struct Domain<'d, ID: Identity> {
    config: config::Config,
    now: Option<clock::Moment<'d>>,
    slots: Vec<Option<Bound<'d, ID>>>,
}

impl<'d, ID: Identity> Domain<'d, ID> {
    pub(super) fn try_new(
        config: config::Config,
        capacity: owner::Capacity,
    ) -> Result<Self, alloc::Error> {
        let capacity = capacity.count().get() as usize;
        let mut slots: Vec<Option<Bound<'d, ID>>> = alloc::VecExt::try_new(capacity)?;
        slots.resize(capacity, None);
        Ok(Self {
            config,
            now: None,
            slots,
        })
    }

    /// Looks up the slot for `conn`. Route indices are bounded by the shared
    /// index encoding, not by this domain's resident capacity, so a missing
    /// slot is a connection this domain does not track, never a fault.
    fn slot_mut(&mut self, conn: conn::Id<'d, ID>) -> Option<&mut Option<Bound<'d, ID>>> {
        self.slots.get_mut(conn.index().into_usize())
    }

    /// Binds this slot to `conn`, replacing any timeline left behind by the
    /// session that previously owned the same recycled route index.
    fn bind(&mut self, conn: conn::Id<'d, ID>) {
        let Some(slot) = self.slot_mut(conn) else {
            return;
        };
        *slot = Some(Bound::new(conn));
    }

    /// Applies one timeline phase change stamped with a clock sample the
    /// caller already holds. Every later layer reuses that sample instead of
    /// re-reading the clock or re-validating the connection.
    fn apply(
        &mut self,
        now: clock::Moment<'d>,
        exchange: exchange::Id<conn::Id<'d, ID>, u64>,
        change: timeline::Transition,
    ) {
        let conn = exchange.connection();
        let config = self.config;
        let Some(bound) = self.resolve_mut(conn) else {
            return;
        };
        match change {
            timeline::Transition::Open => match bound.activity {
                Activity::Admitted(current) if current == exchange => return,
                Activity::Awaiting(_) => {
                    bound.activity = Activity::Admitted(exchange);
                    return;
                }
                Activity::Idle(_) => bound.activity = Activity::Admitted(exchange),
                Activity::Admitted(_) => {
                    bound.transition(config, timeline::Transition::Retired, now);
                    bound.activity = Activity::Admitted(exchange);
                }
            },
            timeline::Transition::AwaitingHead => match bound.activity {
                Activity::Awaiting(_) => return,
                Activity::Admitted(current) if current == exchange => return,
                Activity::Admitted(_) => {
                    bound.transition(config, timeline::Transition::Retired, now);
                    bound.activity = Activity::Awaiting(conn);
                }
                Activity::Idle(_) => bound.activity = Activity::Awaiting(conn),
            },
            timeline::Transition::HeadAdmitted
            | timeline::Transition::InputAdmitted
            | timeline::Transition::BodyProgress
            | timeline::Transition::InputFinished
            | timeline::Transition::HandlerStarted
            | timeline::Transition::ResponseReady
            | timeline::Transition::ResponseCommitted
            | timeline::Transition::Retired => match bound.activity {
                Activity::Admitted(current) if current == exchange => {}
                Activity::Idle(_) | Activity::Awaiting(_) | Activity::Admitted(_) => return,
            },
        }
        bound.transition(config, change, now);
        if change == timeline::Transition::Retired {
            bound.activity = Activity::Idle(conn);
        }
    }

    /// Resolves the timeline owner for `conn`. A vacant slot or a recycled
    /// index that belongs to a different session simply has nothing to track.
    fn resolve_mut(&mut self, conn: conn::Id<'d, ID>) -> Option<&mut Bound<'d, ID>> {
        match self.slot_mut(conn)? {
            None => None,
            Some(bound) if bound.belongs(conn) => Some(bound),
            Some(_) => None,
        }
    }

    fn root(bound: &mut Bound<'d, ID>, scan: Scan) -> deadline::Root<clock::Deadline<'d>> {
        if scan == Scan::Dirty && bound.root == Root::Clean {
            return deadline::Root::Unchanged;
        }
        bound.root = Root::Clean;
        match bound.timeline.next() {
            Some(item) => deadline::Root::Arm(item.at),
            None => deadline::Root::Cancel,
        }
    }

    pub(super) fn observe(&mut self, event: life::Event<conn::Id<'d, ID>, u64>) {
        match event {
            life::Event::SessionOpened(conn) => self.bind(conn),
            life::Event::ExchangeOpened(exchange) => {
                self.stamped(exchange, timeline::Transition::Open);
            }
            life::Event::HeadAdmitted(exchange) => {
                self.stamped(exchange, timeline::Transition::HeadAdmitted);
            }
            life::Event::InputFinished(exchange) => {
                self.stamped(exchange, timeline::Transition::InputFinished);
            }
            life::Event::TaskAdmitted(exchange) => {
                self.stamped(exchange, timeline::Transition::HandlerStarted);
            }
            life::Event::TaskPollReady(exchange) => {
                self.stamped(exchange, timeline::Transition::ResponseReady);
            }
            life::Event::ResponseCommitted { exchange, .. } => {
                self.stamped(exchange, timeline::Transition::ResponseCommitted);
            }
            life::Event::ExchangeRetired(exchange) => {
                self.stamped(exchange, timeline::Transition::Retired);
            }
            life::Event::SessionClosed(conn) => self.close(conn),
            life::Event::SessionEstablished(_)
            | life::Event::PreAdmissionReserved { .. }
            | life::Event::PreAdmissionReleased { .. }
            | life::Event::RequestOwnerAcquired { .. }
            | life::Event::RequestOwnerScopeClosed(_)
            | life::Event::BuildReady(_)
            | life::Event::TaskPollPending(_)
            | life::Event::TaskCancelled(_)
            | life::Event::ResponseBodyCopied { .. }
            | life::Event::ResponseOwnership(_)
            | life::Event::Terminal { .. }
            | life::Event::ExchangeDeadlineExpired { .. }
            | life::Event::SessionClosing(_) => {}
        }
    }

    /// Stamps one lifecycle phase change with the clock sample published by
    /// the enclosing turn. A callback that has not begun owns no armed root,
    /// so it has no deadline work to stamp.
    fn stamped(
        &mut self,
        exchange: exchange::Id<conn::Id<'d, ID>, u64>,
        change: timeline::Transition,
    ) {
        let Some(now) = self.now else {
            return;
        };
        self.apply(now, exchange, change);
    }
}

const _: () = {
    assert!(
        mem::size_of::<Activity<'static, KeyTag<0>>>()
            == mem::size_of::<exchange::Id<conn::Id<'static, KeyTag<0>>, u64>>()
                + mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Option<Bound<'static, KeyTag<0>>>>()
            <= mem::size_of::<timeline::Timeline<clock::Deadline<'static>>>()
                + 6 * mem::size_of::<usize>()
    );
    assert!(mem::align_of::<Option<Bound<'static, KeyTag<0>>>>() == mem::align_of::<usize>());
};

impl<'d, ID: Identity> Domain<'d, ID> {
    pub(super) fn begin_turn(&mut self, now: clock::Moment<'d>) {
        self.now = Some(now);
    }

    pub(super) fn admit_head(&mut self, exchange: exchange::Id<conn::Id<'d, ID>, u64>) {
        self.stamped(exchange, timeline::Transition::HeadAdmitted);
    }

    pub(super) fn turn(
        &mut self,
        conn: conn::Id<'d, ID>,
        now: clock::Moment<'d>,
        head: deadline::Head,
        body: deadline::Body,
    ) -> deadline::Root<clock::Deadline<'d>> {
        self.now = Some(now);
        let config = self.config;
        let Some(bound) = self.resolve_mut(conn) else {
            return deadline::Root::Unchanged;
        };
        if matches!(head, deadline::Head::Pending) && matches!(bound.activity, Activity::Idle(_)) {
            bound.activity = Activity::Awaiting(conn);
            bound.transition(config, timeline::Transition::AwaitingHead, now);
        }
        if matches!(body, deadline::Body::Progress) && !matches!(bound.activity, Activity::Idle(_))
        {
            bound.transition(config, timeline::Transition::BodyProgress, now);
        }
        Self::root(bound, Scan::Dirty)
    }

    pub(super) fn timeout(
        &mut self,
        conn: conn::Id<'d, ID>,
        now: clock::Moment<'d>,
    ) -> deadline::Due<conn::Id<'d, ID>, clock::Deadline<'d>> {
        self.now = Some(now);
        let Some(bound) = self.resolve_mut(conn) else {
            return deadline::Due {
                expiry: None,
                root: deadline::Root::Unchanged,
            };
        };
        let due = bound.timeline.pop_due(now.into());
        let expiry = due.map(|item| match bound.activity {
            Activity::Admitted(exchange) => deadline::Expiry::Exchange {
                exchange,
                phase: item.phase,
            },
            Activity::Idle(_) | Activity::Awaiting(_) => deadline::Expiry::Connection,
        });
        let root = match expiry {
            Some(_) => deadline::Root::Cancel,
            None => Self::root(bound, Scan::All),
        };
        deadline::Due { expiry, root }
    }

    pub(super) fn close(&mut self, conn: conn::Id<'d, ID>) {
        let Some(slot) = self.slot_mut(conn) else {
            return;
        };
        if matches!(slot, Some(bound) if bound.belongs(conn)) {
            *slot = None;
        }
    }
}
