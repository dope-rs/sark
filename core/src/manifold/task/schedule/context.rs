use dope_core::driver::{
    retained,
    schedule::{app, ready},
};

/// Driver resources borrowed for the first task turn.
pub struct Context<'a, 'turn, 'borrow, 'owner, 'd: 'owner> {
    pub parent: ready::Target<'d>,
    pub work: app::Work<'turn, 'd>,
    pub driver: &'a mut retained::Context<'borrow, 'owner, 'd>,
}
