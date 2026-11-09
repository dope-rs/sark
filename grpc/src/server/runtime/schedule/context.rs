use dope::core::driver::{
    self,
    schedule::{app, ready},
};

/// Driver resources borrowed for the first task turn.
pub struct Context<'a, 'turn, 'borrow, 'd> {
    pub parent: ready::Target<'d>,
    pub work: app::Work<'turn, 'd>,
    pub driver: &'a mut driver::Context<'borrow, 'd>,
}
