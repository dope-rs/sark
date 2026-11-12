use std::ops::ControlFlow;

use sark_h2::server::app::Capacity;

pub(in crate::server) trait Storage<T>: Sized {
    fn try_new<E>(
        capacity: Capacity,
        make: impl FnOnce(Capacity) -> Result<T, E>,
    ) -> Result<Self, E>;

    fn get(&self) -> Option<&T>;

    fn entry(&mut self) -> ControlFlow<(&mut Self, Capacity), &mut T>;

    fn insert(&mut self, value: T) -> &mut T;
}

impl<T> Storage<T> for T {
    fn try_new<E>(
        capacity: Capacity,
        make: impl FnOnce(Capacity) -> Result<T, E>,
    ) -> Result<Self, E> {
        make(capacity)
    }

    fn get(&self) -> Option<&T> {
        Some(self)
    }

    fn entry(&mut self) -> ControlFlow<(&mut Self, Capacity), &mut T> {
        ControlFlow::Continue(self)
    }

    fn insert(&mut self, value: T) -> &mut T {
        *self = value;
        self
    }
}

impl<T> Storage<T> for ControlFlow<Capacity, T> {
    fn try_new<E>(capacity: Capacity, _: impl FnOnce(Capacity) -> Result<T, E>) -> Result<Self, E> {
        Ok(Self::Break(capacity))
    }

    fn get(&self) -> Option<&T> {
        match self {
            Self::Continue(value) => Some(value),
            Self::Break(_) => None,
        }
    }

    fn entry(&mut self) -> ControlFlow<(&mut Self, Capacity), &mut T> {
        match self {
            Self::Continue(value) => ControlFlow::Continue(value),
            Self::Break(capacity) => {
                let capacity = *capacity;
                ControlFlow::Break((self, capacity))
            }
        }
    }

    fn insert(&mut self, value: T) -> &mut T {
        *self = Self::Continue(value);
        match self {
            Self::Continue(value) => value,
            Self::Break(_) => unreachable!("state"),
        }
    }
}
