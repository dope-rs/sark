use o3::buffer::{resident, storage};
use sark_core::manifold::server::{self, app::Capacity};
use sark_h2::stream::Key;

use crate::server::{Bank, Lease, runtime::state, service::timers};

pub struct Store<'a, 'd, T, P: timers::Policy, S>
where
    S: server::Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
{
    pub(in crate::server::runtime) state: state::Session<'a, 'd, T, P, S>,
    pub(in crate::server::runtime) active: Lease<'a>,
}

impl<'a, 'd, T, P: timers::Policy, S> Store<'a, 'd, T, P, S>
where
    S: server::Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
{
    pub(in crate::server::runtime) fn try_new(
        bank: &'a Bank,
        cap: Capacity,
    ) -> Result<Self, state::Error>
    where
        P: state::Timing<'d>,
    {
        let active = bank.try_take(cap)?;
        let times = P::try_new(cap.streams()).map_err(state::Error::Deadlines)?;
        Ok(Self {
            state: state::Session::try_new(&bank.limits, cap, times)
                .map_err(state::Error::Storage)?,
            active,
        })
    }
}
