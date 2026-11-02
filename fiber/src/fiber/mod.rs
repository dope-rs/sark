use std::task;

use dope_fiber::{abi, context};
use sark_http::payload::response::dispatch::{self, chunk};

/// A stream that yields iterator chunks without allocation.
#[repr(transparent)]
pub struct IterStream<I> {
    iter: I,
}

impl<'d, I> abi::Stream<'d> for IterStream<I>
where
    I: Iterator<Item = chunk::Chunk<'d>> + Unpin,
{
    type Item = chunk::Chunk<'d>;

    fn poll_next(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Option<Self::Item>> {
        task::Poll::Ready(call.into_fiber().get_mut().iter.next())
    }
}

pub trait IntoStream<'a>: IntoIterator<Item = chunk::Chunk<'a>> + Sized
where
    Self::IntoIter: Unpin,
{
    fn into_stream<'req>(self) -> dispatch::Stream<'req, IterStream<Self::IntoIter>> {
        dispatch::Stream::new(IterStream {
            iter: self.into_iter(),
        })
    }
}

impl<'a, I> IntoStream<'a> for I
where
    I: IntoIterator<Item = chunk::Chunk<'a>>,
    I::IntoIter: Unpin,
{
}
