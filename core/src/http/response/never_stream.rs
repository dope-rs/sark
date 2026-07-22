use std::pin::Pin;
use std::task::Poll;

use dope_fiber::{Context, Fiber};
use o3::buffer::Shared;

pub enum NeverStream {}

impl<'d> Fiber<'d> for NeverStream {
    type Output = Option<Shared>;

    fn poll(self: Pin<&mut Self>, _context: Pin<&mut Context<'_, 'd>>) -> Poll<Self::Output> {
        match *self.get_mut() {}
    }
}
