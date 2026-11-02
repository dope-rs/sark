pub use dope_fiber::task::storage::set::insert::Error;

/// Immediate disposition of one admitted task.
pub enum Start<Pending, Output> {
    Ready(Output),
    Pending(Pending),
}
