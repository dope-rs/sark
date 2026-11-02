use dope_core::driver::schedule::ready::task::{self, owner};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Full,
    Owner(owner::Error),
    Admission(task::Error),
    Topology(task::Error),
}

/// Immediate disposition of one admitted task.
pub enum Start<Pending, Output> {
    Ready(Output),
    Pending(Pending),
}
