use dope::{
    core::driver::{
        route::Identity,
        schedule::{Progress, app::Work},
    },
    manifold::client::{app, conn},
};
use dope_fiber::reply;
use o3::base::cell::region;

use crate::client::conn::route::Route;

pub(in crate::client::conn) trait Schedule<'d, ID: Identity> {
    fn park(&self, work: Work<'_, 'd>, region: &mut region::Token<region::Identity<'d>>);
    fn progress(&self, region: &region::Token<region::Identity<'d>>) -> Progress<'d>;
    fn inbound(&self, conn: conn::Id<'d, ID>) -> app::Inbound;
    fn shutdown(&self);
}

impl<'f, 'e, 'b: 'e, 'c: 'b, 'd: 'c + 'e + 'f, ID: Identity, P> Schedule<'d, ID> for P
where
    P: Route<'f, 'e, 'b, 'c, 'd, ID>,
{
    fn park(&self, work: Work<'_, 'd>, region: &mut region::Token<region::Identity<'d>>) {
        let net = &self.protocol().transport;
        if let Some(tag) = net.pool.maintain(work, region) {
            net.waiters.wake(tag);
        }
        if net.pool.responses.is_exhausted(region) {
            net.waiters.begin_drain();
        }
        net.waiters.drain(work);
        if net.pool.directory.len() != 0 && net.waiters.has_io() && work.permit().is_some() {
            net.waiters.wake_io();
        }
        if self.can_wake(region) && work.permit().is_some() {
            net.waiters.wake_next();
        }
    }

    fn progress(&self, region: &region::Token<region::Identity<'d>>) -> Progress<'d> {
        let net = &self.protocol().transport;
        if net.pool.responses.is_exhausted(region) {
            net.waiters.begin_drain();
        }
        match net.pool.responses.work() {
            reply::Work::Pending => Progress::Runnable,
            reply::Work::Idle
                if net.waiters.has_drain()
                    || (net.pool.directory.len() != 0 && net.waiters.has_io())
                    || self.can_wake(region) =>
            {
                Progress::Runnable
            }
            reply::Work::Idle => Progress::Quiescent,
        }
    }

    fn inbound(&self, conn: conn::Id<'d, ID>) -> app::Inbound {
        self.protocol()
            .transport
            .pool
            .directory
            .idle_window(conn.index().into_usize())
            .map_or(app::Inbound::Quiescent, app::Inbound::Awaiting)
    }

    fn shutdown(&self) {
        self.protocol().transport.waiters.begin_drain();
    }
}
