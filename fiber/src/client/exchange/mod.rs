//! Shared HTTP request exchange state machine.

mod call;
pub(in crate::client) mod dispatch;
pub(in crate::client) mod net;
pub(in crate::client) mod protocol;
pub(in crate::client) mod request;
pub(in crate::client) mod response;
mod retry;
pub(in crate::client) mod reuse;

pub(in crate::client) use call::Call;
pub(in crate::client) use retry::Retry;
pub(in crate::client) use reuse::Reuse;

pub(in crate::client) type Result<T, E, M> = std::result::Result<T, sark_client::Failure<E, M>>;
