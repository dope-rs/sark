//! Validated H3 startup plans and their exact failures.

use dope_quic::runtime::endpoint;
use sark_core::life::session;

use crate::manifold::server::{ConfigError, Handler, deadline};

pub mod embedded;
mod plan;
pub(super) mod setup;

pub use endpoint::BindFailure as BindError;
pub use plan::Plan;

type Local<'d, R, O, Q> = Handler<'d, R, O, session::Disabled, deadline::Disabled, Q>;

#[derive(Debug)]
pub enum PrepareError {
    Application(ConfigError),
    Endpoint(endpoint::PlanError),
}
