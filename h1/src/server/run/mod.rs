mod failure;
mod plan;
pub mod prepare;
mod prepared;
#[doc(hidden)]
mod root;
mod watch;

pub use dope::manifold::network::listener::{Wait, Watch};
pub use failure::{Cause, ExecutionError, Failure, RunCause, RunError};
pub use plan::Plan;
pub use prepared::Prepared;
#[doc(hidden)]
pub use root::Root;
#[doc(hidden)]
pub use watch::Pending;
pub use watch::run;

pub type Output<T> = std::result::Result<T, ExecutionError>;

pub type Result<T, E> = std::result::Result<T, dope::manifold::BuildError<E>>;
