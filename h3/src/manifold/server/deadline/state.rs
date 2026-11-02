use dope::core::driver::schedule::clock;
use sark_core::deadline::{Poll, timeline};

use crate::manifold::requests::{
    self,
    book::timers::{Due, Error},
};

pub trait State<'d> {
    fn transition(
        state: &mut Self,
        id: requests::Id,
        change: timeline::Transition,
        now: clock::Moment<'d>,
    ) -> Result<(), Error>;

    fn deadline(state: &Self) -> Option<clock::Deadline<'d>>;

    fn poll_due(state: &mut Self, now: clock::Moment<'d>) -> Poll<Due>;
}
