use crate::{client::config, protocol::request::Scheme};

#[derive(Clone, Copy)]
pub(in crate::client) struct Plan<P: Copy> {
    pub(in crate::client) scheme: Scheme,
    pub(in crate::client) limits: config::Limits,
    pub(in crate::client) carrier: P,
}

impl<P: Copy> Plan<P> {
    pub(in crate::client) const fn new(limits: config::Limits, carrier: P) -> Self {
        Self {
            scheme: Scheme::Http,
            limits,
            carrier,
        }
    }
}
