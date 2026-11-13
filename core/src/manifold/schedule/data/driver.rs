use crate::manifold::{
    self,
    schedule::{Settlement, Terminal, Turn},
};

/// Data-only state machine driven inside one resident domain.
pub trait Driver<C, T> {
    type Key: manifold::raw::Key;
    type Block: Copy + Eq;
    type Error;
    type Failure;

    fn can_continue(&self) -> bool {
        true
    }

    fn turn(
        &mut self,
        context: &mut C,
        stream: Self::Key,
        value: &mut T,
    ) -> Result<Turn<Self::Block>, Self::Error>;

    fn settle(
        &mut self,
        context: &mut C,
        stream: Self::Key,
        value: &mut T,
        terminal: Terminal<Self::Error>,
    ) -> (Settlement<()>, Result<(), Self::Failure>);

    fn remove(&mut self, value: T);
}
