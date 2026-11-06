//! Static deadline policies and connection-local timer state.
use core::{convert::Infallible, mem};

use dope::core::driver::schedule::clock;
use o3::{
    buffer::resident,
    depot::{capacity, heap},
};
use sark_core::deadline::{self, config, timeline};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{
        machine,
        machine::header::{self, Stage},
    },
    stream,
};

/// Zero-state policy preserving the existing server path exactly.
#[derive(Clone, Copy, Default)]
pub struct Disabled;

/// Static policy seam implemented by disabled and finite deadline modes.
#[doc(hidden)]
pub trait Policy<'d> {
    const ENABLED: bool;
    type State;

    fn state(&self, capacity: capacity::Count) -> Result<Self::State, heap::Error>;

    fn controller<'a>(&self, state: &'a mut Self::State) -> Option<&'a mut Controller<'d>>;
}

impl<'d> Policy<'d> for Disabled {
    const ENABLED: bool = false;
    type State = ();

    fn state(&self, _: capacity::Count) -> Result<Self::State, heap::Error> {
        Ok(())
    }

    fn controller<'a>(&self, _: &'a mut Self::State) -> Option<&'a mut Controller<'d>> {
        None
    }
}

impl<'d> Policy<'d> for config::Config {
    const ENABLED: bool = true;
    type State = Controller<'d>;

    fn state(&self, capacity: capacity::Count) -> Result<Self::State, heap::Error> {
        Ok(Controller {
            config: *self,
            book: deadline::Agenda::try_new(capacity)?,
            header: None,
        })
    }

    fn controller<'a>(&self, state: &'a mut Self::State) -> Option<&'a mut Controller<'d>> {
        Some(state)
    }
}

/// Exact action selected by one connection-local deadline turn.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Due {
    Exchange {
        key: stream::Key,
        phase: deadline::Phase,
    },
    Critical,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Change {
    Unchanged,
    Changed,
}

const _: () = assert!(mem::size_of::<Change>() == mem::size_of::<bool>());

#[derive(Clone, Copy)]
struct Header<'d> {
    observed: header::Header,
    at: clock::Deadline<'d>,
}

/// One-allocation deadline authority owned by an admitted H2 connection.
#[doc(hidden)]
pub struct Controller<'d> {
    config: config::Config,
    book: deadline::Agenda<stream::Key, clock::Deadline<'d>, Infallible>,
    header: Option<Header<'d>>,
}

impl<'d> Controller<'d> {
    pub(in crate::server) fn observe_head<F>(
        &mut self,
        conn: &machine::Connection<crate::ServerRole, F, resident::Shared<'d>>,
        now: clock::Moment<'d>,
    ) -> Result<Change, deadline::Error<stream::Key>>
    where
        F: rules::Policy,
    {
        let before = self.deadline();
        let observed = header::Header::read(conn);
        if self
            .header
            .is_some_and(|header| Some(header.observed) == observed)
        {
            return Ok(Change::Unchanged);
        }
        if let Some(previous) = self.header.take()
            && let Some(key) = previous.observed.exchange
            && !conn.status().owns_exchange(key)
        {
            deadline::Book::cancel(&mut self.book, key);
        }
        let Some(observed) = observed else {
            return Ok(if before == self.deadline() {
                Change::Unchanged
            } else {
                Change::Changed
            });
        };
        let total_at = if let Some(key) = observed.exchange
            && observed.stage == Stage::Initial
        {
            let at = instant(self.config, deadline::Phase::Total, now);
            if let Some(at) = at {
                schedule_at(&mut self.book, key, deadline::Phase::Total, at)?;
            }
            at
        } else {
            None
        };
        let head_at = instant(self.config, deadline::Phase::Head, now);
        if observed.stage == Stage::Other
            && head_at.is_some()
            && let Some(key) = observed.exchange
        {
            deadline::Book::cancel_one(
                &mut self.book,
                key,
                deadline::Action::Expire(deadline::Phase::BodyIdle),
            );
        }
        self.header = head_at.or(total_at).map(|at| Header { observed, at });
        Ok(if before == self.deadline() {
            Change::Unchanged
        } else {
            Change::Changed
        })
    }

    pub(in crate::server) fn transition(
        &mut self,
        key: stream::Key,
        change: timeline::Transition,
        now: clock::Moment<'d>,
    ) -> Result<Change, deadline::Error<stream::Key>> {
        let before = self.deadline();
        let change = if change == timeline::Transition::HeadAdmitted
            && self.header.is_some_and(|header| {
                header.observed.exchange == Some(key) && header.observed.stage == Stage::Other
            }) {
            timeline::Transition::InputAdmitted
        } else {
            change
        };
        self.book
            .transition(key, self.config, change, now, clock::Moment::clamp_add)?;
        Ok(if before == self.deadline() {
            Change::Unchanged
        } else {
            Change::Changed
        })
    }

    pub fn deadline(&self) -> Option<clock::Deadline<'d>> {
        let exchange = deadline::Book::peek(&self.book).map(|entry| entry.1.at);
        let header = self.header.map(|header| header.at);
        match (exchange, header) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(at), None) | (None, Some(at)) => Some(at),
            (None, None) => None,
        }
    }

    pub(crate) fn poll_due(&mut self, now: clock::Moment<'d>) -> deadline::Poll<Due> {
        let exchange = deadline::Book::peek(&self.book);
        if let Some(header) = self.header
            && header.at <= now
            && exchange.is_none_or(|entry| header.at < entry.1.at)
        {
            self.header = None;
            return deadline::Poll::Expired(Due::Critical);
        }
        let expired = match deadline::Book::poll_due(&mut self.book, now.into()) {
            deadline::Poll::Idle => return deadline::Poll::Idle,
            deadline::Poll::Renewed => return deadline::Poll::Renewed,
            deadline::Poll::Expired(expired) => expired,
        };
        deadline::Poll::Expired(match expired.scheduled.action {
            deadline::Action::Expire(phase) => match self.header {
                Some(header) if header.observed.exchange == Some(expired.key) => Due::Critical,
                Some(_) | None => Due::Exchange {
                    key: expired.key,
                    phase,
                },
            },
            deadline::Action::Wake(never) => match never {},
        })
    }

    pub(in crate::server) fn cancel(&mut self, key: stream::Key) -> Change {
        if deadline::Book::cancel(&mut self.book, key).is_earliest() {
            Change::Changed
        } else {
            Change::Unchanged
        }
    }
}

fn instant<'d>(
    config: config::Config,
    phase: deadline::Phase,
    now: clock::Moment<'d>,
) -> Option<clock::Deadline<'d>> {
    match config.window(phase) {
        config::Window::Disabled => None,
        config::Window::Finite(window) => Some(now.clamp_add(window)),
    }
}

fn schedule_at<'d>(
    book: &mut deadline::Agenda<stream::Key, clock::Deadline<'d>, Infallible>,
    key: stream::Key,
    phase: deadline::Phase,
    at: clock::Deadline<'d>,
) -> Result<(), deadline::Error<stream::Key>> {
    let item = deadline::Scheduled::expire(at, phase);
    deadline::Book::schedule(book, key, item)
}

const _: () = assert!(mem::size_of::<Disabled>() == 0);
