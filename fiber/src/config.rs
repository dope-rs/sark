use core::mem;

use dope::manifold::app::receive::limits;

/// Global task admission capacity for a generated application.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Config<R = ()> {
    task_capacity: usize,
    retention: R,
}

impl Config {
    /// Creates the global admission quota without changing physical route quotas.
    pub const fn new(capacity: usize) -> Self {
        Self {
            task_capacity: capacity,
            retention: (),
        }
    }

    #[doc(hidden)]
    pub const fn capacity(self) -> usize {
        self.task_capacity
    }

    /// Supplies the resident authority required by H1 duplex input.
    pub const fn with_budget(self, budget: limits::Budget) -> Config<limits::Budget> {
        Config {
            task_capacity: self.task_capacity,
            retention: budget,
        }
    }
}

impl Config<limits::Budget> {
    pub const fn into_parts(self) -> (Config, limits::Budget) {
        (Config::new(self.task_capacity), self.retention)
    }
}

const _: () = {
    assert!(mem::size_of::<Config>() == mem::size_of::<usize>());
    assert!(mem::align_of::<Config>() == mem::align_of::<usize>());
    assert!(!mem::needs_drop::<Config>());
    assert!(mem::size_of::<Config<limits::Budget>>() == mem::size_of::<(usize, limits::Budget)>());
    assert!(!mem::needs_drop::<Config<limits::Budget>>());
};
