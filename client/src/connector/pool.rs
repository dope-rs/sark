use core::cell::Cell;
use std::time::{Duration, Instant};

use cartel_core::{Arena, ArenaConfig, ArenaLane, Limits};
use dope::driver::token::Token;
use o3::cell::{RegionCell, RegionToken};
use o3::collections::SlotQueue;

use super::codec::{MAX_INFORMATIONAL_RESPONSES, STREAM_CHUNK_SIZE};
use super::error::Error;
use super::response::ResponseEvent;

pub(super) type Outcome = Result<ResponseEvent, Error>;

const KEEPALIVE_MARGIN: Duration = Duration::from_secs(1);

pub(super) struct ResponsePush {
    pub outcome: Outcome,
    pub bytes: usize,
    pub complete: bool,
    pub keepalive: Option<Duration>,
    pub now: Instant,
}

struct Connection {
    id: Cell<Option<Token>>,
    last_activity: Cell<Option<Instant>>,
    keepalive: Cell<Option<Duration>>,
    queued: Cell<bool>,
}

pub(super) struct ConnectionPool<'d> {
    entries: Box<[Connection]>,
    arena: Arena<'d, Outcome>,
    ready: RegionCell<'d, SlotQueue<Token>>,
    live: Cell<usize>,
}

impl<'d> ConnectionPool<'d> {
    pub(super) fn new(
        capacity: usize,
        max_inflight: usize,
        limit: usize,
        max_response_body: usize,
    ) -> Self {
        let entries = capacity
            .checked_mul(max_inflight)
            .expect("HTTP response entry capacity overflow");
        let per_entry_items = max_response_body
            .div_ceil(STREAM_CHUNK_SIZE)
            .saturating_mul(2)
            .saturating_add(MAX_INFORMATIONAL_RESPONSES + 3);
        let items = entries
            .checked_mul(3)
            .and_then(|base| {
                limit
                    .div_ceil(STREAM_CHUNK_SIZE)
                    .checked_mul(2)
                    .and_then(|body| base.checked_add(body))
            })
            .expect("HTTP response item capacity overflow");
        let limits = Limits::new(per_entry_items, limit, per_entry_items);
        Self {
            entries: (0..capacity)
                .map(|_| Connection {
                    id: Cell::new(None),
                    last_activity: Cell::new(None),
                    keepalive: Cell::new(None),
                    queued: Cell::new(false),
                })
                .collect(),
            arena: Arena::new(ArenaConfig::new(
                capacity, entries, items, limit, items, limits,
            )),
            ready: RegionCell::new(SlotQueue::with_capacity(capacity)),
            live: Cell::new(0),
        }
    }

    fn entry(&self, id: Token) -> Option<&Connection> {
        let entry = self.entries.get(id.slot().raw() as usize)?;
        (entry.id.get() == Some(id)).then_some(entry)
    }

    fn push_ready(&self, token: &mut RegionToken<'d>, entry: &Connection, id: Token) {
        if entry.queued.replace(true) {
            return;
        }
        self.ready
            .borrow_mut(token)
            .vacant_entry(id.slot().raw() as usize)
            .expect("ready queue entry must be vacant")
            .push_back(id);
    }

    fn pop_ready(&self, token: &mut RegionToken<'d>) -> Option<Token> {
        self.ready.borrow_mut(token).pop_front()
    }

    fn remove_ready(&self, token: &mut RegionToken<'d>, id: Token) {
        self.ready
            .borrow_mut(token)
            .remove(id.slot().raw() as usize);
    }

    pub(super) fn has_connection(&self) -> bool {
        self.live.get() != 0
    }

    pub(super) fn connection_count(&self) -> usize {
        self.live.get()
    }

    pub(super) fn note_connect(&self, token: &mut RegionToken<'d>, id: Token, now: Instant) {
        let entry = &self.entries[id.slot().raw() as usize];
        if entry.id.replace(Some(id)).is_none() {
            self.live.set(self.live.get() + 1);
            self.arena.activate(token, id.slot().raw() as usize);
        }
        entry.last_activity.set(Some(now));
        entry.keepalive.set(None);
        self.push_ready(token, entry, id);
    }

    pub(super) fn push_response(
        &self,
        token: &mut RegionToken<'d>,
        id: Token,
        push: ResponsePush,
    ) -> bool {
        let ResponsePush {
            outcome,
            bytes,
            complete,
            keepalive,
            now,
        } = push;
        let Some(entry) = self.entry(id) else {
            return false;
        };
        entry.last_activity.set(Some(now));
        if keepalive.is_some() {
            entry.keepalive.set(keepalive);
        }
        let lane = id.slot().raw() as usize;
        let pushed = self.arena.try_push(token, lane, outcome, bytes, 1);
        if complete {
            self.arena.complete(token, lane);
        }
        if complete && self.arena.can_register(token, lane) {
            self.push_ready(token, entry, id);
        }
        pushed
    }

    pub(super) fn complete_response(
        &self,
        token: &mut RegionToken<'d>,
        id: Token,
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
        let lane = id.slot().raw() as usize;
        self.arena.complete(token, lane);
        if self.arena.can_register(token, lane) {
            self.push_ready(token, entry, id);
        }
    }

    pub(super) fn close(&self, token: &mut RegionToken<'d>, id: Token) {
        let Some(entry) = self.entry(id) else {
            return;
        };
        let lane = id.slot().raw() as usize;
        self.arena.fail_all(token, lane, || Err(Error::Closed));
        self.arena.deactivate(token, lane);
        entry.id.set(None);
        self.live.set(self.live.get() - 1);
        entry.last_activity.set(None);
        entry.keepalive.set(None);
        entry.queued.set(false);
        self.remove_ready(token, id);
    }

    pub(super) fn acquire(
        &self,
        token: &mut RegionToken<'d>,
        now: Instant,
        idle_timeout: Duration,
        mut recycle: impl FnMut(Token),
    ) -> Option<Token> {
        while let Some(id) = self.pop_ready(token) {
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
                self.close(token, id);
                recycle(id);
                continue;
            }
            if self.arena.can_register(token, id.slot().raw() as usize) {
                return Some(id);
            }
        }
        None
    }

    pub(super) fn arena(&'d self, id: Token) -> Option<ArenaLane<'d, Outcome>> {
        self.entry(id)?;
        Some(self.arena.lane(id.slot().raw() as usize))
    }

    pub(super) fn submitted(&self, token: &mut RegionToken<'d>, id: Token, now: Instant) {
        if let Some(entry) = self.entry(id) {
            entry.last_activity.set(Some(now));
            if self.arena.can_register(token, id.slot().raw() as usize) {
                self.push_ready(token, entry, id);
            }
        }
    }

    pub(super) fn make_available(&self, token: &mut RegionToken<'d>, id: Token) {
        if let Some(entry) = self.entry(id)
            && self.arena.can_register(token, id.slot().raw() as usize)
        {
            self.push_ready(token, entry, id);
        }
    }
}
