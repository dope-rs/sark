pub use dope_fiber::{
    ErasedTaskId, Fiber, FixedSlab, FixedSlabVacantEntry, OwnerFiber, SplitTask, TaskId,
    try_from_split_task,
};

#[doc(hidden)]
pub trait FixedSlabFiber<'d, Output>: Fiber<'d, Output = Output> {}

impl<'d, F, Output> FixedSlabFiber<'d, Output> for F where F: Fiber<'d, Output = Output> {}
