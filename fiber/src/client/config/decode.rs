use dope::{core::driver::storage, manifold::app::creation};
use reply::decode::Policy;
use sark_client::{reply, reply::decode::Decoder};

const DEFAULT_BODY_LIMIT: usize = 16 * 1024 * 1024;
const DEFAULT_RESIDENT_CAPACITY: usize = 16 * 1024 * 1024;

pub(in crate::client) struct Decode {
    pub(in crate::client) policy: Policy,
    pub(in crate::client) body_limit: usize,
    pub(in crate::client) resident_capacity: usize,
}

impl Decode {
    pub(in crate::client) fn try_build<'d>(
        self,
        context: &storage::Context<'_, 'd>,
    ) -> Result<Decoder<'d>, creation::Error> {
        let budget = context
            .budget(storage::Resident::bytes(self.resident_capacity))
            .map_err(creation::Error::Resident)?;
        Ok(Decoder::new(
            self.policy,
            self.body_limit,
            budget,
            context.region(),
        ))
    }
}

impl Default for Decode {
    fn default() -> Self {
        Self {
            policy: Policy::Strict,
            body_limit: DEFAULT_BODY_LIMIT,
            resident_capacity: DEFAULT_RESIDENT_CAPACITY,
        }
    }
}
