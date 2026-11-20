use dope::runtime::topology;

mod plan;
pub mod prepare;
mod prepared;
#[doc(hidden)]
mod root;

pub use dope::manifold::network::listener::{ExecutionError, Wait, Watch};
pub use plan::Plan;
pub use prepared::Prepared;
#[doc(hidden)]
pub use root::Root;

pub type Failure<'report, I, T> = topology::Failure<'report, I, T, ExecutionError>;
pub type Cause<'a, I> = topology::error::Failure<'a, I, ExecutionError>;
pub type RunError<'owner, S, I, O, T, Q = ()> =
    topology::RunError<'owner, S, I, O, T, Q, ExecutionError>;
pub type RunCause<'a, S, I> = topology::error::Run<'a, S, I, ExecutionError>;

pub type Result<T, E> = std::result::Result<T, dope::manifold::BuildError<E>>;

/// Resolves a dependent server only after its provider has also quiesced.
pub fn lending<'report, I, T>(
    watch: &Watch<'_>,
    result: topology::lending::Result<'report, I, std::result::Result<T, ExecutionError>>,
) -> std::result::Result<T, Failure<'report, I, T>> {
    watch.finish(match result {
        Ok(output) => Ok(output),
        Err(topology::lending::Error::Build(cause)) => return Err(Failure::Install(cause)),
        Err(topology::lending::Error::Run(error)) => Err(error),
    })
}
