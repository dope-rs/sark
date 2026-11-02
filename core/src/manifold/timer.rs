//! Driver timer access required by generated application fibers.

use dope_core::driver::schedule::clock::timer;

pub trait Source<'d> {
    fn timer(&self) -> timer::Timer<'d>;
}
