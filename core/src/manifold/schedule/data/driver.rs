use crate::manifold::{
    self,
    schedule::{Block, Terminal, Turn, data::Settlement},
};

/// Data-only state machine driven inside one resident domain.
pub trait Driver<T> {
    type Key: manifold::raw::Key;
    type Block: Block;
    type Error;

    fn can_continue(&self) -> bool;

    fn turn(&mut self, stream: Self::Key, value: &mut T) -> Result<Turn<Self::Block>, Self::Error>;

    fn settle(
        &mut self,
        stream: Self::Key,
        value: &mut T,
        terminal: Terminal<Self::Error>,
    ) -> Settlement;

    fn remove(&mut self, value: T);
}
