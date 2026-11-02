use std::task;

use dope::{
    core::driver::{
        route::{Identity, KeyTag},
        schedule::ready::wake,
    },
    manifold::client::{IOV_CAP, conn},
    net::link::{egress, pool::change::opening},
};
use o3::base::cell::{local, region};

use crate::client::{self, channel, outbound};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Claimed;

/// The connector's exclusive capacity waiter, retained until this driver drops.
pub struct Driver<'app, 'life: 'app, 'e: 'life, 'd: 'e, B: ?Sized, ID: Identity = KeyTag<0>> {
    pub(super) handle: client::Handle<'app, 'life, 'e, 'd, B, ID>,
}

impl<B: ?Sized, ID: Identity> Driver<'_, '_, '_, '_, B, ID> {
    /// Cancels only this driver's outstanding capacity registration.
    pub fn clear_waiter(&mut self) {
        self.handle.port.shared.clear_waiter();
    }
}

impl<'d, B: ?Sized, ID: Identity> Driver<'_, '_, '_, 'd, B, ID> {
    /// Waits for an open connection using this driver's exclusive wake slot.
    /// An active connection preserves any outstanding capacity registration.
    pub fn poll_active(
        &mut self,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), opening::Exhaustion>> {
        match self.handle.port.active() {
            Ok(true) => task::Poll::Ready(Ok(())),
            Ok(false) => {
                let shared = &self.handle.port.shared;
                shared.send_plan.set(None);
                shared.send_waiter.set(wake);
                task::Poll::Pending
            }
            Err(cause) => {
                self.clear_waiter();
                task::Poll::Ready(Err(cause))
            }
        }
    }

    /// Enqueues text, retaining this driver's waiter while capacity is exhausted.
    pub fn poll_text(
        &mut self,
        local: local::Access<'_, region::Identity<'d>>,
        data: &str,
        wake: wake::Waker<'d>,
    ) -> task::Poll<Result<(), client::Error>> {
        let port = self.handle.port;
        let plan = match port.try_plan(0x1, data.as_bytes()) {
            Ok(plan) => plan,
            Err(error) => {
                self.clear_waiter();
                return task::Poll::Ready(Err(error));
            }
        };
        port.shared.set_waiter(plan.batch(), wake);
        let result = local.with_region(|region| port.try_commit(region, plan));
        match result {
            Err(client::Error::Admission(_)) => task::Poll::Pending,
            result => {
                self.clear_waiter();
                task::Poll::Ready(result)
            }
        }
    }
}

impl<B: ?Sized, ID: Identity> Drop for Driver<'_, '_, '_, '_, B, ID> {
    fn drop(&mut self) {
        self.clear_waiter();
        self.handle.port.shared.claimed.set(false);
    }
}

const _: () =
    assert!(size_of::<Driver<'static, 'static, 'static, 'static, ()>>() == size_of::<usize>());

pub struct Sender<'a, 'p, 'e, 'd, ID: Identity = KeyTag<0>> {
    sink: egress::Queue<'a, 'p, 'd, IOV_CAP>,
    port: &'a channel::Port<'e, 'd, ID>,
    region: &'a mut region::Token<region::Identity<'d>>,
    conn: conn::Id<'d, ID>,
}

impl<'a, 'p, 'e, 'd, ID: Identity> Sender<'a, 'p, 'e, 'd, ID> {
    pub(super) fn new(
        conn: conn::Id<'d, ID>,
        sink: egress::Queue<'a, 'p, 'd, IOV_CAP>,
        port: &'a channel::Port<'e, 'd, ID>,
        region: &'a mut region::Token<region::Identity<'d>>,
    ) -> Self {
        Self {
            sink,
            port,
            region,
            conn,
        }
    }

    pub fn binary(&mut self, data: &[u8]) -> Result<(), client::Error> {
        let plan = self.port.try_plan(0x2, data)?;
        self.send(plan)
    }

    pub fn close(&mut self, close: crate::inbound::Close<'_>) -> Result<(), client::Error> {
        self.send(outbound::Plan::close(close)?)
    }

    pub(super) fn reply(&mut self, reply: crate::inbound::Reply<'_>) -> Result<(), client::Error> {
        self.send(outbound::Plan::reply(reply)?)
    }

    fn send(&mut self, plan: outbound::Plan<'_>) -> Result<(), client::Error> {
        self.port
            .try_final(self.conn, &self.sink, self.region, plan)
    }
}
