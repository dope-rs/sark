/// Direct request execution.
pub mod direct;
/// Driver-local task execution.
pub(in crate::server) mod executor;
/// Server dispatch contracts.
pub mod policy;
/// Request preparation.
pub mod prepare;
/// Connection service execution.
pub mod run;
/// Scoped task scheduling contracts.
pub mod schedule;
/// Request state transitions.
pub mod state;
/// Streaming task execution.
pub mod stream;
/// Unary task execution.
pub mod unary;

pub(in crate::server) type Connection<'d, E> = sark_h2::conn::machine::Connection<
    sark_h2::ServerRole,
    crate::protocol::section::Plan,
    o3::buffer::resident::Shared<'d>,
    sark_http::field::metadata::section::rules::RejectPush,
    E,
>;
