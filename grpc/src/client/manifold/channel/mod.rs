use std::{cell, num::NonZero, ops};

use dope::{
    core::driver::{
        self,
        route::{Identity, KeyTag},
        schedule::{app, ready::wake},
    },
    manifold::{
        client::{conn, port, session::ingress},
        service::bindings::endpoint,
    },
    net::link::egress::data::{compose, pair},
    runtime::client,
};
use o3::{
    base::cell::region,
    buffer::{pool, resident},
    depot::queue::resident::Fixed,
    mem::quota,
};
use sark_h2::stream;
use wake::{fanout, queue, table};

use crate::client::{
    manifold::{conn::State, ops::cancels, settings},
    receive::{self, policies::Notify},
};

mod binding;
mod handle;
mod unary;

#[doc(hidden)]
pub use binding::Binding;
pub use handle::Handle;
pub use unary::Unary;

pub(in crate::client::manifold) struct Shared<'d, ID: Identity> {
    pub(in crate::client::manifold) change_waiters: fanout::Set<'d>,
    pub(in crate::client::manifold) sender_waiters: queue::Queue<'d, Fixed<1>>,
    pub(in crate::client::manifold) completion_waiters: table::Table<'d>,
    sender_wake: cell::Cell<SenderWake>,
    state: region::Value<region::Identity<'d, 1>, State<'d, ID>>,
    pub(in crate::client::manifold) cancels: cancels::Queue<'d, ID>,
}

/// The exact state/table pair borrowed from one channel allocation.
pub(in crate::client::manifold) struct Access<'a, 'life, 'd, ID: Identity> {
    shared: &'life Shared<'d, ID>,
    state: &'a mut State<'d, ID>,
}

impl<'a, 'life: 'a, 'd, ID: Identity> Access<'a, 'life, 'd, ID> {
    pub(in crate::client::manifold) fn shared(&self) -> &'life Shared<'d, ID> {
        self.shared
    }

    pub(in crate::client::manifold) fn with<'call, R>(
        &'call mut self,
        run: impl for<'scope> FnOnce(&mut State<'d, ID>, &mut Notify<'scope, 'life, 'd>) -> R,
    ) -> R {
        table::Table::with_source(self, |state, scope, sink| {
            run(state, &mut Notify::new(receive::Scope::new(scope), sink))
        })
    }

    fn drain(&mut self) -> cancels::Empty<'life, 'd, ID> {
        let shared = self.shared;
        self.with(|state, done| {
            loop {
                let (conn, call) = match shared.cancels.pop() {
                    Ok(call) => call,
                    Err(idle) => return idle,
                };
                if let Some(session) = state.get_mut(conn) {
                    call.cancel(session, done);
                    shared.wake_space(state);
                    state.signal(conn);
                }
            }
        })
    }

    pub(in crate::client::manifold) fn register<T>(
        &mut self,
        conn: conn::Id<'d, ID>,
        key: cancels::Key,
        stream: stream::Key,
        value: T,
    ) -> cancels::Registered<T> {
        let idle = self.drain();
        idle.register(conn, key, stream, value)
    }
}

impl<'d, ID: Identity> ops::Deref for Access<'_, '_, 'd, ID> {
    type Target = State<'d, ID>;

    fn deref(&self) -> &Self::Target {
        self.state
    }
}

impl<ID: Identity> ops::DerefMut for Access<'_, '_, '_, ID> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state
    }
}

const _: () = assert!(
    core::mem::size_of::<Access<'static, 'static, 'static, KeyTag<0>>>()
        == 2 * core::mem::size_of::<usize>()
);

#[derive(Clone, Copy, PartialEq, Eq)]
enum SenderWake {
    None,
    Capacity(usize),
    All,
}

impl<'d, ID: Identity> Shared<'d, ID> {
    pub(in crate::client::manifold) fn wake(&self) {
        self.change_waiters.publish();
    }

    pub(in crate::client::manifold) fn complete_all(&self) {
        if self.completion_waiters.has_waiters() && !self.completion_waiters.is_draining() {
            self.completion_waiters.begin_drain();
        }
    }

    pub(in crate::client::manifold) fn wake_space(&self, state: &State<'d, ID>) {
        if !self.sender_waiters.has_waiters() {
            return;
        }
        let slots = state.owner_cap();
        if slots == 0 {
            return;
        }
        self.sender_wake.set(match self.sender_wake.get() {
            SenderWake::None => SenderWake::Capacity(slots),
            SenderWake::Capacity(pending) => SenderWake::Capacity(pending.max(slots)),
            SenderWake::All => SenderWake::All,
        });
    }

    pub(in crate::client::manifold) fn wake_senders(&self) {
        if self.sender_waiters.has_waiters() {
            self.sender_wake.set(SenderWake::All);
        }
    }

    pub(in crate::client::manifold) fn flush_wake(&self, work: app::Work<'_, 'd>) {
        loop {
            match self.change_waiters.drain_one(work) {
                wake::Drain::Woke => {}
                wake::Drain::Empty => break,
                wake::Drain::Exhausted => return,
            }
        }
        let wake = || {
            work.run(|| match self.sender_waiters.wake_one::<0>() {
                wake::Once::Empty => None,
                wake::Once::Woke => Some(()),
            })
        };
        match self.sender_wake.get() {
            SenderWake::None => {}
            SenderWake::All => {
                loop {
                    match wake() {
                        quota::Admission::Item(()) => {}
                        quota::Admission::Empty => break,
                        quota::Admission::Exhausted => return,
                    }
                }
                self.sender_wake.set(SenderWake::None);
            }
            SenderWake::Capacity(mut count) => {
                while count != 0 {
                    match wake() {
                        quota::Admission::Item(()) => count -= 1,
                        quota::Admission::Empty => break,
                        quota::Admission::Exhausted => {
                            self.sender_wake.set(SenderWake::Capacity(count));
                            return;
                        }
                    }
                }
                self.sender_wake.set(SenderWake::None);
            }
        }
        loop {
            match self.completion_waiters.drain_one(work) {
                wake::Drain::Woke => {}
                wake::Drain::Empty => break,
                wake::Drain::Exhausted => return,
            }
        }
    }

    pub(in crate::client::manifold) fn get_mut<'a>(
        &'a self,
        token: &'a mut region::Token<region::Identity<'d, 1>>,
    ) -> &'a mut State<'d, ID> {
        self.source(token).state
    }

    pub(in crate::client::manifold) fn source<'a, 'life: 'a>(
        &'life self,
        token: &'a mut region::Token<region::Identity<'d, 1>>,
    ) -> Access<'a, 'life, 'd, ID> {
        let mut source = Access {
            shared: self,
            state: token.borrow_mut(&self.state),
        };
        source.drain();
        source
    }

    pub(in crate::client::manifold) fn try_new(
        capacity: NonZero<usize>,
        state: State<'d, ID>,
        driver: driver::Reference<'d>,
        token: &region::Token<region::Identity<'d, 1>>,
    ) -> Result<Self, settings::Error> {
        let limits = state.limits();
        let claims = limits.slots();
        let calls = limits.call_count().get();
        let offset = limits.unary_count().get();
        Ok(Self {
            change_waiters: fanout::Set::try_new(driver, capacity.get())
                .map_err(settings::Error::Change)?,
            sender_waiters: queue::Queue::try_new(driver, capacity.get(), Fixed::<1>)
                .map_err(settings::Error::Sender)?,
            completion_waiters: table::Table::try_new(driver, claims)
                .map_err(settings::Error::Completion)?,
            sender_wake: cell::Cell::new(SenderWake::None),
            state: token.bind(state),
            cancels: cancels::Queue::try_new(claims, calls, offset)?,
        })
    }
}

pub struct Port<'e, 'd, ID: Identity = KeyTag<0>> {
    pub(in crate::client::manifold) ingress: ingress::Plan<ingress::Local>,
    pub(in crate::client::manifold) io:
        port::Port<'e, 'd, compose::Either<pool::Cursor, pair::Source<resident::Shared<'d>>>, ID>,
    pub(in crate::client::manifold) shared: Shared<'d, ID>,
}

impl<'life, 'e: 'life, 'd: 'life, ID: Identity + 'life>
    endpoint::Provider<'life, 'd, ID, { u32::MAX as usize }> for Port<'e, 'd, ID>
{
    type Client<'app, P: ?Sized + 'app>
        = Handle<'app, 'life, 'e, 'd, P, ID>
    where
        'life: 'app,
        'd: 'app;

    fn provide<'app, P: ?Sized + 'app>(
        &'life self,
        bound: client::Bound<'app, 'd, P>,
    ) -> Self::Client<'app, P>
    where
        'life: 'app,
        'd: 'app,
    {
        Handle::new(self, bound)
    }
}

// SAFETY: Shared is the only constructor of Access and borrows its own State.
// Its completion table is built from that State's immutable protocol limits;
// every reconnect creates receive claims from the same retained plan. Access
// keeps the original state/table pair borrowed throughout each scoped operation.
unsafe impl<'a, 'state: 'a, 'life: 'state, 'd: 'life, ID: Identity>
    table::raw::Source<'a, 'life, 'd> for &'a mut Access<'state, 'life, 'd, ID>
{
    type State = State<'d, ID>;

    fn parts(self) -> (&'a mut Self::State, &'life table::Table<'d>) {
        (self.state, &self.shared.completion_waiters)
    }
}
