//! Protocol-independent session lifetime policy.

use std::time::Duration;

use o3::{
    base::time,
    buffer::{self, storage},
};

/// Retained byte ownership accepted by a protocol-independent session.
pub trait Storage: buffer::raw::Segment + From<storage::Shared> {}

impl<T: buffer::raw::Segment + From<storage::Shared>> Storage for T {}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Config {
    age: time::Positive,
    drain: time::Positive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    MaxAge { source: time::Zero },
    DrainTimeout { source: time::Zero },
}

impl Config {
    pub const fn new(age: time::Positive, drain: time::Positive) -> Self {
        Self { age, drain }
    }

    pub fn try_new(age: Duration, drain: Duration) -> Result<Self, ConfigError> {
        let age = match time::Positive::try_new(age) {
            Ok(age) => age,
            Err(source) => return Err(ConfigError::MaxAge { source }),
        };
        let drain = match time::Positive::try_new(drain) {
            Ok(drain) => drain,
            Err(source) => return Err(ConfigError::DrainTimeout { source }),
        };
        Ok(Self::new(age, drain))
    }

    pub const fn age(self) -> time::Positive {
        self.age
    }

    pub const fn drain(self) -> time::Positive {
        self.drain
    }
}

#[derive(Clone, Copy, Default)]
pub struct Disabled;

const _: () = {
    assert!(core::mem::size_of::<Disabled>() == 0);
};
