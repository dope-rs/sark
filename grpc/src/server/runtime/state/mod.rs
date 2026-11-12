mod error;
pub(super) mod storage;
mod store;
mod timing;

/// Connection readiness authorities shared with dispatch policies.
pub mod active;
pub(super) mod lanes;

pub use error::Error;
pub use store::Store;
pub(in crate::server::runtime) use store::try_deadline;
pub(in crate::server) use timing::Timing;

pub struct State<'r, S> {
    pub(in crate::server::runtime) storage: S,
    pub(in crate::server::runtime) active: crate::server::Lease<'r>,
}
