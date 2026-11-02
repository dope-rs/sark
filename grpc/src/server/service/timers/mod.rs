//! Deferred-output timer policies and capabilities.

use std::time;

use dope::core::driver::schedule::clock;
use o3::depot::heap::keyed;

use crate::server::runtime::state::active;

mod book;
mod wake;

#[doc(hidden)]
pub use book::Book;
use sark_h2::stream::Key;

/// Zero-state policy for handlers without deferred output.
#[derive(Clone, Copy, Default)]
pub struct Off;

/// Exact connection-local scheduling for handlers with deferred output.
#[derive(Clone, Copy, Default)]
pub struct On;

#[doc(hidden)]
pub struct Enabled<'a, 'r, 'd> {
    entry: active::Entry<'r>,
    now: clock::Moment<'d>,
    deadlines: &'a mut Book<'r, 'd>,
}

/// Compile-time deferred-output storage policy.
#[doc(hidden)]
pub trait Policy: Copy {
    type State<'r, 'd>;
    type Access<'a, 'r, 'd>
    where
        'r: 'a,
        'd: 'a;
}

impl Policy for Off {
    type State<'r, 'd> = ();
    type Access<'a, 'r, 'd>
        = ()
    where
        'r: 'a,
        'd: 'a;
}

impl Policy for On {
    type State<'r, 'd> = Book<'r, 'd>;
    type Access<'a, 'r, 'd>
        = Enabled<'a, 'r, 'd>
    where
        'r: 'a,
        'd: 'a;
}

/// Output capability tied to one stream, driver lifetime, and timer policy.
pub struct Output<'a, 'r: 'a, 'd: 'a, P: Policy = Off> {
    access: P::Access<'a, 'r, 'd>,
}

impl<'a, 'r: 'a, 'd: 'a> Output<'a, 'r, 'd, Off> {
    pub(in crate::server) const fn new() -> Self {
        Self { access: () }
    }
}

impl<'a, 'r: 'a, 'd: 'a> Output<'a, 'r, 'd, On> {
    pub(in crate::server) fn new(
        entry: active::Entry<'r>,
        turn: clock::Moment<'d>,
        state: &'a mut Book<'r, 'd>,
    ) -> Self {
        Self {
            access: Enabled {
                entry,
                now: turn,
                deadlines: state,
            },
        }
    }

    pub fn defer_until(&mut self, deadline: clock::Deadline<'d>) -> Result<(), Error> {
        self.access.deadlines.defer(self.access.entry, deadline)
    }

    pub fn defer_for(&mut self, duration: time::Duration) -> Result<(), Error> {
        let deadline = self.access.now.try_add(duration).map_err(Error::Clock)?;
        self.defer_until(deadline)
    }
}

/// A deferred output could not be represented or stored in its fixed domain.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Error {
    Clock(clock::Range),
    Bounds(keyed::Error<Key>),
    Conflict(keyed::Conflict<Key>),
}

const _: () = assert!(core::mem::size_of::<Off>() == 0);
const _: () = assert!(core::mem::size_of::<On>() == 0);
const _: () = assert!(core::mem::size_of::<Output<'static, 'static, 'static, Off>>() == 0);
const _: () = assert!(
    core::mem::size_of::<Output<'static, 'static, 'static, On>>()
        == 6 * core::mem::size_of::<usize>()
);
const _: () = assert!(core::mem::size_of::<Key>() == core::mem::size_of::<u64>());
const _: () = assert!(core::mem::size_of::<Option<Key>>() == core::mem::size_of::<u64>());
