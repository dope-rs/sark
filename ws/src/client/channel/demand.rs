use std::marker::PhantomData;

use dope::{
    core::driver::route::{Identity, KeyTag},
    manifold::service::discover,
};
use o3::base::cell::region;

use crate::client::channel::Port;

/// Admits future dials while this port has no selected connection.
/// Install with `sync::Preserve` to retain already submitted and live connections.
pub struct Demand<'a, 'e, 'd, I, ID: Identity = KeyTag<0>> {
    pub(super) port: &'a Port<'e, 'd, ID>,
    pub(super) endpoint: PhantomData<fn() -> I>,
}

impl<'d, I, ID: Identity> discover::Demand<'d> for Demand<'_, '_, 'd, I, ID> {
    type Endpoint = I;

    fn is_needed(&self, _: &region::Token<region::Identity<'d, 1>>) -> bool {
        let shared = &self.port.shared;
        shared.conn_id.current().is_none() && shared.exhausted.get().is_none()
    }

    fn is_eligible(&self, _: &I, token: &region::Token<region::Identity<'d, 1>>) -> bool {
        self.is_needed(token)
    }
}
