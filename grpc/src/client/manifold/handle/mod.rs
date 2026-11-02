use std::{mem, task};

use dope::{
    core::driver::{
        route::{Identity, KeyTag},
        schedule::ready::wake::{self, queue, table},
    },
    manifold::client::conn,
    runtime::client,
};
use o3::{base::cell::region, buffer::resident, depot::queue::resident::Fixed};

use crate::client::{
    manifold::{channel, error, ops::cancels},
    receive,
};

pub(super) struct Scope<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> {
    pub(super) port: &'life channel::Port<'e, 'd, ID>,
    pub(super) connection: conn::Id<'d, ID>,
    pub(in crate::client::manifold) bound: client::Bound<'app, 'd, P>,
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Copy
    for Scope<'app, 'life, 'e, 'd, P, ID>
{
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Clone
    for Scope<'app, 'life, 'e, 'd, P, ID>
{
    fn clone(&self) -> Self {
        *self
    }
}

const _: () = {
    assert!(
        mem::size_of::<Scope<'static, 'static, 'static, 'static, str, KeyTag<0>>>()
            == mem::size_of::<usize>() + mem::size_of::<conn::Id<'static, KeyTag<0>>>()
    );
    assert!(
        mem::size_of::<Option<Scope<'static, 'static, 'static, 'static, str, KeyTag<0>>>>()
            == mem::size_of::<Scope<'static, 'static, 'static, 'static, str, KeyTag<0>>>()
    );
};

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Scope<'app, 'life, 'e, 'd, P, ID> {
    pub(super) const fn new(
        port: &'life channel::Port<'e, 'd, ID>,
        conn: conn::Id<'d, ID>,
        bound: client::Bound<'app, 'd, P>,
    ) -> Self {
        Self {
            port,
            connection: conn,
            bound,
        }
    }

    pub(super) fn cancel(self, call: cancels::Registered<cancels::Call<'d>>) {
        self.port.shared.cancels.push(self.connection, call);
    }
}

mod bidi;
mod delivery;
mod recv;
pub mod request;
mod response;
mod send;
mod unary;
mod write;

pub use bidi::Bidi;
pub use delivery::Delivery;
use recv::Recv;
pub use response::Response;
pub use send::Send;
pub use unary::Unary;
use write::Write;

trait Completion<'life, 'd: 'life> {
    type Output: receive::Event<Storage = resident::Shared<'d>>;

    fn poll(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        lease: &mut table::Registration<'life, 'd>,
        wake: &wake::Waker<'d>,
    ) -> Result<Option<Self::Output>, error::Error>;
}

/// Registration stores the wake target without changing the admission result.
pub(super) fn poll_sender<'target, 'life, 'e, 'd, T, F, ID: Identity>(
    port: &'life channel::Port<'e, 'd, ID>,
    wake: &wake::Waker<'d>,
    lease: &mut queue::Registration<'target, 'd, Fixed<1>>,
    admit: F,
) -> task::Poll<Result<T, error::Error>>
where
    F: FnOnce() -> Result<Option<T>, error::Error>,
    'life: 'target,
    'd: 'target,
{
    match admit() {
        Ok(Some(value)) => task::Poll::Ready(Ok(value)),
        Err(error) => task::Poll::Ready(Err(error)),
        Ok(None) => {
            port.shared
                .sender_waiters
                .try_register::<0>(lease, wake)
                .map_err(error::Error::Sender)?;
            task::Poll::Pending
        }
    }
}
