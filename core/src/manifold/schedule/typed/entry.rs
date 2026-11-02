use o3::depot::queue::indexed::lanes::typed;
use typed::Value;

use crate::manifold::{
    self,
    schedule::{Block, LANES, policy::Policy, typed::Occupied},
};

/// The exact resident phase selected by one key admission.
pub enum Entry<'a, K: manifold::raw::Key, C, D, I, B: Block> {
    Control(Occupied<'a, K, C, D, I, B, typed::Control>),
    Data(Occupied<'a, K, C, D, I, B, typed::Data>),
    Idle(Occupied<'a, K, C, D, I, B, typed::Idle>),
}

impl<'a, K: manifold::raw::Key, C, D, I, B: Block> Entry<'a, K, C, D, I, B> {
    pub(super) fn new(entry: typed::Entry<'a, K, C, D, I, LANES>, policy: &'a mut Policy) -> Self {
        match entry {
            typed::Entry::Control(slot) => Self::Control(Occupied::new(slot, policy)),
            typed::Entry::Data(slot) => Self::Data(Occupied::new(slot, policy)),
            typed::Entry::Idle(slot) => Self::Idle(Occupied::new(slot, policy)),
        }
    }

    pub fn remove(self) -> Value<C, D, I> {
        match self {
            Self::Control(entry) => Value::Control(entry.remove()),
            Self::Data(entry) => Value::Data(entry.remove()),
            Self::Idle(entry) => Value::Idle(entry.remove()),
        }
    }
}
