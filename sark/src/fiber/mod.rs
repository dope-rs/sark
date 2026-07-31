pub use dope_fiber::abi::Fiber;
pub use dope_fiber::owner::SplitTask;
pub use dope_fiber::slab::{ErasedTaskId, FixedSlab, FixedSlabVacantEntry, TaskId};

#[doc(hidden)]
pub trait FixedSlabFiber<'d, Output>: Fiber<'d, Output = Output> {}

impl<'d, F, Output> FixedSlabFiber<'d, Output> for F where F: Fiber<'d, Output = Output> {}
