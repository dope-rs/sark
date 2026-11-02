mod call;
pub use call::Call;

/// Direct request execution.
pub mod direct;
/// Server dispatch contracts.
pub mod policy;
/// Request preparation.
pub mod prepare;
/// Connection service execution.
pub mod run;
/// Request state transitions.
pub mod state;
/// Streaming task execution.
pub mod stream;
/// Unary task execution.
pub mod unary;
