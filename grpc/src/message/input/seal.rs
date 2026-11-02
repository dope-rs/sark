use o3::buffer;

use crate::message;

pub trait Seal<C: buffer::raw::Segment> {}

impl<C: buffer::raw::Segment> Seal<C> for message::Payload<C> {}
impl<C: buffer::raw::Segment, P> Seal<C> for message::DataChunk<C, P> {}
