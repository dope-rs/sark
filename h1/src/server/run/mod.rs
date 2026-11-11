use dope::runtime::topology;

mod plan;
pub mod prepare;
mod prepared;
#[doc(hidden)]
mod root;
mod watch;

pub use dope::manifold::network::listener::{ExecutionError, Wait, Watch};
pub use plan::Plan;
pub use prepared::Prepared;
#[doc(hidden)]
pub use root::Root;
#[doc(hidden)]
pub use watch::Pending;
pub use watch::{lending, run, session};

pub type Failure<'report, I, T> = topology::Failure<'report, I, T, ExecutionError>;
pub type Cause<'a, I> = topology::error::Failure<'a, I, ExecutionError>;
pub type RunError<'owner, S, I, O, T, Q = ()> =
    topology::RunError<'owner, S, I, O, T, Q, ExecutionError>;
pub type RunCause<'a, S, I> = topology::error::Run<'a, S, I, ExecutionError>;

pub type Output<T> = std::result::Result<T, ExecutionError>;

pub type Result<T, E> = std::result::Result<T, dope::manifold::BuildError<E>>;
