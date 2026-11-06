use std::marker;

use dope::{
    core::driver::{self, route::Identity},
    manifold::{
        self,
        app::{env, receive},
        network::listener,
        timing::interval,
    },
    net::{
        Listen,
        link::{
            egress::data,
            pool::{change, recv::input},
        },
    },
};

use super::{Result, Root, root::Bound};
use crate::server::{date, deadline::policies, listener::bind};

/// Owns prepared server storage before a dependent application is constructed.
pub struct Prepared<'n, 'p, 's, 'w, 'd: 'w, ListenerId, DateId, E, R, B, I, D>
where
    ListenerId: Identity,
    DateId: Identity,
    E: env::Spec,
    E::Transport: Listen,
    B: data::raw::Payload<'d> + 'd,
{
    pub(super) listener: listener::Prepared<'n, 'p, 's, 'w, 'd, ListenerId, E, R, B>,
    pub(super) clock: &'w date::Clock,
    pub(super) date: interval::Interval<'d, DateId>,
    pub(super) deadline: D,
    pub(super) input: marker::PhantomData<fn() -> I>,
}

impl<'n, 'p, 's, 'w, 'd: 'w, ListenerId, DateId, E, R, B, I, D>
    Prepared<'n, 'p, 's, 'w, 'd, ListenerId, DateId, E, R, B, I, D>
where
    ListenerId: Identity + 'd,
    DateId: Identity,
    E: env::Spec,
    E::Transport: Listen,
    E::Wire: input::Execution + 'd,
    R: change::Prepare<'w, 'd, ListenerId, E::Wire>,
    B: data::raw::Payload<'d> + 'd,
    D: policies::Policy<'d, ListenerId>,
    I: input::Mode<E::Wire> + receive::Resources,
{
    pub fn build<F>(
        self,
        app: F,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<Bound<'n, 'p, 's, 'w, 'd, ListenerId, DateId, F, D, E>, F::Error>
    where
        F: bind::Bind<'w, 'd, ListenerId, Wire = E::Wire, Input = I, Egress = B>,
    {
        let binding = self.listener.binding(driver.driver_ref());
        let app = app
            .try_bind(binding, self.deadline, self.clock)
            .map_err(manifold::BuildError::Factory)?;
        let listener = self
            .listener
            .build(app, driver)
            .map_err(manifold::BuildError::Create)?;
        Ok(Root {
            listener,
            date: date::Root::new(self.clock, self.date),
        })
    }
}
