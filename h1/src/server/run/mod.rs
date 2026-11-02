mod plan;
pub mod prepare;
mod prepared;
#[doc(hidden)]
mod root;

pub use plan::Plan;
pub use prepared::Prepared;
#[doc(hidden)]
pub use root::Root;

pub type Result<T, E> = std::result::Result<T, dope::manifold::BuildError<E>>;
