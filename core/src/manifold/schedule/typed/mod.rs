//! Resident payload phases that retain their exact scheduling authority.

use std::convert::Infallible;

use o3::depot::queue::indexed::lanes::typed::Value;

use crate::manifold::{self, schedule};

mod entry;
mod occupied;
mod place;
mod runtime;
mod source;
mod vacant;

pub use entry::Entry;
pub use occupied::Occupied;
pub use runtime::Runtime;
pub use vacant::Vacant;

/// One successor and the resource that owns its next wakeup.
pub enum Next<C, D, I, B> {
    Control(C, Option<B>),
    Data(D, Option<B>),
    Idle(I),
}

/// One owned control transition, including any data quantum it performed.
pub enum Control<C, D, I, B, E = Infallible> {
    Control { value: C, turn: schedule::Turn<B> },
    Data { value: D, turn: schedule::Turn<B> },
    Fused { value: D, turn: schedule::Turn<B> },
    Idle(I),
    Failed { value: Value<C, D, I>, error: E },
    Complete,
    Remove,
}

/// Terminal ownership policy shared by all driver admission modes.
pub trait Settle<C, D, I> {
    type Key: manifold::raw::Key;
    type Error;

    fn settle(
        &mut self,
        stream: Self::Key,
        value: Value<C, D, I>,
        terminal: schedule::Terminal<Self::Error>,
    ) -> schedule::Settlement<Value<C, D, I>>;
}

/// Control consumes its owner; data borrows only its admitted payload phase.
pub trait Driver<C, D, I>: Settle<C, D, I> {
    type Block: schedule::Block;
    type Permit;

    fn permit(&mut self) -> Option<Self::Permit>;

    fn control(
        &mut self,
        permit: Self::Permit,
        stream: Self::Key,
        value: C,
    ) -> Control<C, D, I, Self::Block, Self::Error>;

    fn data(
        &mut self,
        permit: Self::Permit,
        stream: Self::Key,
        value: &mut D,
    ) -> Result<schedule::Turn<Self::Block>, Self::Error>;
}
