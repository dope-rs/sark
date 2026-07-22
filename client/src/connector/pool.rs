use core::cell::Cell;
use core::pin::Pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

use cartel_core::{Arena, Limits};
use dope::driver::token::Token;
use o3::cell::RawCell;
use o3::collections::SlotQueue;
use o3::mem::ByteBudget;
use sark_core::http::Response;

use super::error::Error;

pub(super) type Outcome = Result<Response, Error>;

const KEEPALIVE_MARGIN: Duration = Duration::from_secs(1);

struct Connection<'d> {
    id: Cell<Option<Token>>,
    arena: Arena<'d, Outcome>,
    last_activity: Cell<Option<Instant>>,
    keepalive: Cell<Option<Duration>>,
    queued: Cell<bool>,
}

pub(super) struct ConnectionPool<'d> {
    entries: Box<[Connection<'d>]>,
    ready: RawCell<SlotQueue<Token>>,
    live: Cell<usize>,
    _budget: Pin<Rc<ByteBudget>>,
}

impl<'d> ConnectionPool<'d> {
    pub(super) fn new(capacity: usize, max_inflight: usize, limit: usize) -> Self {
        let budget = Rc::pin(ByteBudget::new(limit));
        let limits = Limits::new(1, limit, 1);
        Self {
            entries: (0..capacity)
                .map(|_| Connection {
                    id: Cell::new(None),
                    arena: Arena::with_shared_budget(max_inflight, limits, budget.clone()),
                    last_activity: Cell::new(None),
                    keepalive: Cell::new(None),
                    queued: Cell::new(false),
                })
                .collect(),
            ready: RawCell::new(SlotQueue::with_capacity(capacity)),
            live: Cell::new(0),
            _budget: budget,
        }
    }

    fn entry(&self, id: Token) -> Option<&Connection<'d>> {
        let entry = self.entries.get(id.slot().raw() as usize)?;
        (entry.id.get() == Some(id)).then_some(entry)
    }

    fn push_ready(&self, entry: &Connection<'d>, id: Token) {
        if entry.queued.replace(true) {
            return;
        }
        unsafe {
            self.ready.with_mut(|ready| {
                ready
                    .vacant_entry(id.slot().raw() as usize)
                    .expect("ready queue entry must be vacant")
                    .push_back(id);
            })
        };
    }

    fn pop_ready(&self) -> Option<Token> {
        unsafe { self.ready.with_mut(SlotQueue::pop_front) }
    }

    fn remove_ready(&self, id: Token) {
        unsafe {
            self.ready
                .with_mut(|ready| ready.remove(id.slot().raw() as usize))
        };
    }

    pub(super) fn has_connection(&self) -> bool {
        self.live.get() != 0
    }

    pub(super) fn connection_count(&self) -> usize {
        self.live.get()
    }

    pub(super) fn note_connect(&self, id: Token, now: Instant) {
        let entry = &self.entries[id.slot().raw() as usize];
        if entry.id.replace(Some(id)).is_none() {
            self.live.set(self.live.get() + 1);
        }
        entry.last_activity.set(Some(now));
        entry.keepalive.set(None);
        self.push_ready(entry, id);
    }

    pub(super) fn push_response(
        &self,
        id: Token,
        outcome: Outcome,
        bytes: usize,
        keepalive: Option<Duration>,
        now: Instant,
    ) {
        let Some(entry) = self.entry(id) else {
            return;
        };
        entry.last_activity.set(Some(now));
        if keepalive.is_some() {
            entry.keepalive.set(keepalive);
        }
        entry.arena.try_push(outcome, bytes, 1);
        entry.arena.complete();
        if entry.arena.can_register() {
            self.push_ready(entry, id);
        }
    }

    pub(super) fn close(&self, id: Token) {
        let Some(entry) = self.entry(id) else {
            return;
        };
        entry.arena.fail_all(|| Err(Error::Closed));
        entry.id.set(None);
        self.live.set(self.live.get() - 1);
        entry.last_activity.set(None);
        entry.keepalive.set(None);
        entry.queued.set(false);
        self.remove_ready(id);
    }

    pub(super) fn acquire(
        &self,
        now: Instant,
        idle_timeout: Duration,
        mut recycle: impl FnMut(Token),
    ) -> Option<Token> {
        while let Some(id) = self.pop_ready() {
            let Some(entry) = self.entry(id) else {
                continue;
            };
            entry.queued.set(false);
            let limit = entry
                .keepalive
                .get()
                .map(|keepalive| keepalive.saturating_sub(KEEPALIVE_MARGIN))
                .unwrap_or(idle_timeout);
            let stale = entry
                .last_activity
                .get()
                .is_some_and(|last| now.saturating_duration_since(last) >= limit);
            if stale {
                self.close(id);
                recycle(id);
                continue;
            }
            if entry.arena.can_register() {
                return Some(id);
            }
        }
        None
    }

    pub(super) fn arena(&'d self, id: Token) -> Option<&'d Arena<'d, Outcome>> {
        Some(&self.entry(id)?.arena)
    }

    pub(super) fn submitted(&self, id: Token, now: Instant) {
        if let Some(entry) = self.entry(id) {
            entry.last_activity.set(Some(now));
            if entry.arena.can_register() {
                self.push_ready(entry, id);
            }
        }
    }

    pub(super) fn make_available(&self, id: Token) {
        if let Some(entry) = self.entry(id)
            && entry.arena.can_register()
        {
            self.push_ready(entry, id);
        }
    }
}
