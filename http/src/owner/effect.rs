use o3::buffer::hold;

use crate::owner::Change;

/// One indivisible response-owner mutation. Handoff transfers the
/// owner from protocol staging to transport.
#[must_use = "owner effects must be committed to their lifecycle sink"]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Effect<K> {
    key: K,
    charge: hold::Charge,
    change: Change,
}

impl<K> Effect<K> {
    pub const fn new(key: K, charge: hold::Charge, next: Change) -> Self {
        Self {
            key,
            charge,
            change: next,
        }
    }

    pub const fn key(&self) -> &K {
        &self.key
    }

    pub const fn charge(&self) -> hold::Charge {
        self.charge
    }

    pub const fn change(&self) -> Change {
        self.change
    }

    pub fn map_key<T>(self, map: impl FnOnce(K) -> T) -> Effect<T> {
        Effect {
            key: map(self.key),
            charge: self.charge,
            change: self.change,
        }
    }
}
