//! Driver-bound WebSocket channel and shared wake state.

use std::{cell, marker::PhantomData, time};

use dope::{
    core::{
        driver,
        driver::{
            route::{Identity, KeyTag},
            schedule::{app::Work, ready::wake},
            settings::errors,
        },
    },
    manifold::{
        app::dispatch::identity,
        client::{conn, port, session::ingress},
        network::client::topology::plan,
        service::{bindings::endpoint, health},
    },
    net::link::{egress, egress::metadata::LaneCount, pool::change::opening},
    runtime::client,
};
use dope_fiber::{wait, wait::Capacity};
use egress::metadata::batch;
use o3::buffer::storage;
use wake::store;

use crate::client::{Config, outbound, protocol};

mod app;
mod demand;
mod factory;
mod layout;

pub use app::App;
pub use demand::Demand;
pub use factory::Factory;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanError {
    Backoff(health::Error),
    Layout(errors::Claim),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Connection {
    Open,
    Closing,
}

pub(super) struct SharedState<'d, ID: Identity> {
    pub(super) conn_id: identity::Binding<conn::Id<'d, ID>, Connection>,
    pub(super) exhausted: cell::Cell<Option<opening::Exhaustion>>,
    pub(super) active_waiters: wait::Queue<'d>,
    pub(super) send_waiter: store::Slot<'d>,
    pub(super) send_plan: cell::Cell<Option<batch::Plan>>,
    pub(super) claimed: cell::Cell<bool>,
    wake_pending: cell::Cell<bool>,
    pub(super) rng: outbound::Sequence,
}

impl<'d, ID: Identity> SharedState<'d, ID> {
    pub(super) fn new(cap: Capacity, driver: driver::Reference<'d>) -> Self {
        use std::cell::Cell;

        let active = wait::Queue::new(driver, cap);
        Self {
            conn_id: identity::Binding::new(),
            exhausted: Cell::new(None),
            active_waiters: active,
            send_waiter: store::Slot::empty(),
            send_plan: Cell::new(None),
            claimed: Cell::new(false),
            wake_pending: Cell::new(false),
            rng: outbound::Sequence::default(),
        }
    }

    pub(super) fn wake(&self) {
        self.wake_pending.set(true);
    }

    pub(super) fn exhaust(&self, cause: opening::Exhaustion) {
        if self.exhausted.get().is_none() {
            self.exhausted.set(Some(cause));
        }
        self.wake_waiter();
        self.wake();
    }

    pub(super) fn unavailable(&self) -> crate::client::Error {
        self.exhausted.get().map_or(
            crate::client::Error::NotConnected,
            crate::client::Error::Exhausted,
        )
    }

    pub(super) fn is_closing(&self, conn: conn::Id<'d, ID>) -> bool {
        matches!(self.conn_id.current(), Some((current, Connection::Closing)) if current == conn)
    }

    pub(super) fn close(&self, conn: conn::Id<'d, ID>) {
        if self
            .conn_id
            .replace_if(conn, Connection::Open, Connection::Closing)
            .is_some()
        {
            self.wake_waiter();
            self.wake();
        }
    }

    pub(super) fn set_waiter(&self, plan: batch::Plan, wake: wake::Waker<'d>) {
        self.send_plan.set(Some(plan));
        self.send_waiter.set(wake);
    }

    pub(super) fn clear_waiter(&self) {
        self.send_waiter.clear();
        self.send_plan.set(None);
    }

    pub(super) fn wake_waiter(&self) {
        self.send_plan.set(None);
        self.send_waiter.wake_once();
    }

    pub(super) fn flush_wake(&self, work: Work<'_, 'd>) {
        use dope_fiber::wait::WakeStatus;
        if self.wake_pending.get() && self.active_waiters.wake(work) == WakeStatus::Complete {
            self.wake_pending.set(false);
        }
    }
}

pub struct Port<'e, 'd, ID: Identity = KeyTag<0>> {
    pub(super) codec: protocol::Codec,
    pub(super) ingress: ingress::Plan<ingress::Local>,
    pub(super) egress: egress::Config,
    pub(super) shared: SharedState<'d, ID>,
    pub(super) io: port::Port<'e, 'd, storage::Shared, ID>,
}

impl<'life, 'e: 'life, 'd: 'e, ID: Identity + 'life>
    endpoint::Provider<'life, 'd, ID, { u32::MAX as usize }> for Port<'e, 'd, ID>
{
    type Client<'app, B: ?Sized + 'app>
        = crate::client::Handle<'app, 'life, 'e, 'd, B, ID>
    where
        'life: 'app,
        'd: 'app;

    fn provide<'app, B: ?Sized + 'app>(
        &'life self,
        bound: client::Bound<'app, 'd, B>,
    ) -> Self::Client<'app, B>
    where
        'life: 'app,
        'd: 'app,
    {
        crate::client::Handle {
            port: self,
            _bound: bound,
        }
    }
}

impl<'e, 'd, ID: Identity> Port<'e, 'd, ID> {
    pub fn demand<I>(&self) -> Demand<'_, 'e, 'd, I, ID> {
        Demand {
            port: self,
            endpoint: PhantomData,
        }
    }

    /// Reports the selected connection before any permanent opening failure.
    pub fn active(&self) -> Result<bool, opening::Exhaustion> {
        if matches!(self.shared.conn_id.current(), Some((_, Connection::Open))) {
            Ok(true)
        } else {
            self.shared.exhausted.get().map_or(Ok(false), Err)
        }
    }

    pub fn capacity(&self) -> LaneCount {
        self.io.capacity()
    }

    /// Reconstructs the startup-validated connector plan for custom
    /// generative installers whose discovery is created inside the driver.
    /// Use `watch` with `demand()` and `sync::Preserve` to keep one selected connection.
    pub fn plan(
        &self,
        base: time::Duration,
        max: time::Duration,
    ) -> Result<plan::Plan<ID, { u32::MAX as usize }>, PlanError> {
        let spec = Factory::<ID>::spec(&self.codec.config, self.egress, base, max)
            .map_err(PlanError::Backoff)?;
        let lanes = self.capacity().count().get();
        plan::Plan::new(lanes, spec).map_err(PlanError::Layout)
    }
}

impl Port<'_, '_, KeyTag<0>> {
    pub fn factory(
        config: Config,
        lanes: usize,
        waits: usize,
    ) -> Result<Factory, crate::client::Error> {
        Factory::new(config, lanes, waits)
    }
}
