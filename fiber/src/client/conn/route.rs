use dope::{
    core::{driver, driver::route::Identity},
    manifold::client::{app, attempt, attempt::queue::mailbox, conn},
    net::wire::{self, terminal},
};
use dope_fiber::reply::layout;
use o3::base::cell::region;

use crate::client::{
    channel::Port,
    config::report,
    conn::Kind,
    dynamic::channel,
    exchange::{
        net::{self, PoolTag},
        protocol::Protocol,
    },
};

pub(in crate::client) enum Connect<'d, ID: Identity> {
    Open(net::Open),
    Mailbox(mailbox::Error<'d, ID>, channel::Outcome<'d, ID>),
}

pub(in crate::client) trait Route<'f, 'e, 'b, 'c, 'd, ID: Identity>:
    Kind<
        'd,
        ID,
        Error: 'f,
        Tag: PoolTag<'d, ID>,
        Position = layout::Position<'b, 'd, ID>,
        Failure: report::Policy<
            <Self as Kind<'d, ID>>::Error,
            State<'f, 'd> = <Self as Kind<'d, ID>>::State,
            Detail<'f>: 'b,
        >,
    >
{
    fn can_wake(&self, region: &region::Token<region::Identity<'d>>) -> bool;

    fn protocol(&self) -> &Protocol<'f, 'e, 'b, 'c, 'd, ID, Self::Tag, Self::Error, Self::Failure>;

    fn reject(&self, attempt: &attempt::Completion<'_, 'd, ID>, error: net::Open)
    -> app::Admission;

    fn try_connect(
        &self,
        attempt: attempt::Completion<'_, 'd, ID>,
        conn: conn::Id<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<(), Connect<'d, ID>>;
}

impl<'f, 'e, 'b, 'c, 'w: 'f, 'd: 'f, ID: Identity, W: wire::Spec + 'w, F: 'w>
    Route<'f, 'e, 'b, 'c, 'd, ID> for Port<'f, 'e, 'b, 'c, 'w, 'd, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
    F::Detail<'f>: 'b,
{
    fn can_wake(&self, region: &region::Token<region::Identity<'d>>) -> bool {
        let net = &self.protocol.transport;
        net.pool.directory.ready.has_ready(region) && net.waiters.has_active()
    }

    fn protocol(&self) -> &Protocol<'f, 'e, 'b, 'c, 'd, ID, (), terminal::Error<'w, W>, F> {
        &self.protocol
    }

    fn reject(&self, _: &attempt::Completion<'_, 'd, ID>, error: net::Open) -> app::Admission {
        app::Admission::Rejected(error.close())
    }

    fn try_connect(
        &self,
        _: attempt::Completion<'_, 'd, ID>,
        conn: conn::Id<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<(), Connect<'d, ID>> {
        self.protocol
            .transport
            .note_connect(driver, conn)
            .map_err(Connect::Open)
    }
}

impl<'o, 'q, 'e, 'b, 'c, 'd, ID: Identity> Route<'d, 'e, 'b, 'c, 'd, ID>
    for channel::Port<'o, 'q, 'e, 'b, 'c, 'd, ID>
{
    fn can_wake(&self, _: &region::Token<region::Identity<'d>>) -> bool {
        let source = self.pending.source();
        (source.has_room() || source.is_exhausted()) && self.protocol.transport.waiters.has_active()
    }

    fn protocol(&self) -> &Protocol<'d, 'e, 'b, 'c, 'd, ID, Self::Tag> {
        &self.protocol
    }

    fn reject(
        &self,
        attempt: &attempt::Completion<'_, 'd, ID>,
        error: net::Open,
    ) -> app::Admission {
        self.fail(attempt, channel::Failure::Activation(error));
        app::Admission::Rejected(error.close())
    }

    fn try_connect(
        &self,
        attempt: attempt::Completion<'_, 'd, ID>,
        conn: conn::Id<'d, ID>,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<(), Connect<'d, ID>> {
        let reserve = match self.protocol.transport.try_reserve(driver, conn) {
            Ok(reserve) => reserve,
            Err(error) => {
                self.fail(&attempt, channel::Failure::Activation(error));
                return Err(Connect::Open(error));
            }
        };
        let (origin, waiter) = self
            .pending
            .try_settle(&attempt, channel::Outcome::Connected(conn))
            .map_err(|(cause, outcome)| Connect::Mailbox(cause, outcome))?;
        reserve(origin);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
        Ok(())
    }
}
