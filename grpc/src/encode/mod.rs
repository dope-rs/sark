use core::marker;

use o3::buffer::{self, pool, storage};

use crate::{Reason, Status};

mod field;
mod varint;

pub use field::Field;

/// A consuming destination for the actual measured bytes of one message.
pub trait Sink: Sized {
    type Output;

    fn try_write<const N: usize>(
        self,
        source: Option<buffer::raw::Slices<'_, N>>,
    ) -> Result<Self::Output, Status>;

    fn try_slices<const N: usize>(self, sources: [&[u8]; N]) -> Result<Self::Output, Status> {
        let source = buffer::raw::Slices::try_new(sources)
            .map_err(|error| Status::capacity(Reason::MessageLarge, error))?;
        self.try_write(source)
    }
}

/// Protobuf encoding that supplies actual bytes instead of reporting a length.
pub trait Message {
    type Plan<'a>: Plan
    where
        Self: 'a;

    fn plan(&self) -> Self::Plan<'_>;
}

/// One encoding recipe that retains the exact source until its single write.
pub trait Plan {
    fn write<S: Sink>(self, sink: S) -> Result<S::Output, Status>;
}

impl Message for () {
    type Plan<'a> = ();

    fn plan(&self) -> Self::Plan<'_> {
        *self
    }
}

impl Plan for () {
    fn write<S: Sink>(self, sink: S) -> Result<S::Output, Status> {
        sink.try_write(None::<buffer::raw::Slices<'_, 0>>)
    }
}

impl Sink for &mut storage::Shared {
    type Output = ();

    fn try_write<const N: usize>(
        self,
        source: Option<buffer::raw::Slices<'_, N>>,
    ) -> Result<(), Status> {
        *self = match source {
            Some(source) => source
                .try_owned()
                .map_err(storage::AllocError::Exhausted)?
                .into_shared(),
            None => storage::Shared::new(),
        };
        Ok(())
    }
}

impl Sink for pool::Span {
    type Output = storage::Shared;

    fn try_write<const N: usize>(
        mut self,
        source: Option<buffer::raw::Slices<'_, N>>,
    ) -> Result<Self::Output, Status> {
        self.truncate(0);
        let frozen = match source {
            Some(source) => pool::Copy::try_new(source, self)
                .map_err(|error| Status::capacity(Reason::RetainedCapacity, error))?
                .commit(),
            None => self.freeze(),
        };
        Ok(frozen.into_shared())
    }
}

#[derive(Default)]
pub struct Prost<Encode, Decode = Encode>(marker::PhantomData<fn() -> (Encode, Decode)>);

impl<Encode, Decode> Prost<Encode, Decode> {
    pub const fn new() -> Self {
        Self(marker::PhantomData)
    }
}

impl<Encode, Decode> crate::Codec for Prost<Encode, Decode>
where
    Encode: Message,
    Decode: prost::Message + Default,
{
    type Encode = Encode;
    type Decode<'a, C>
        = Decode
    where
        C: buffer::raw::Segment + 'a;
    type Owned<C: buffer::raw::Segment> = Decode;

    fn encode<S: Sink>(&mut self, item: &Encode, sink: S) -> Result<S::Output, crate::Status> {
        item.plan().write(sink)
    }

    fn decode<'a, C: buffer::raw::Segment + 'a>(
        &mut self,
        input: crate::message::View<'a, C>,
    ) -> Result<Decode, crate::Status> {
        Ok(Decode::decode(input.cursor())?)
    }

    fn decode_frame<C: buffer::raw::Segment>(
        &mut self,
        frame: crate::message::Frame<C>,
    ) -> Result<Decode, crate::Status> {
        Ok(Decode::decode(frame.cursor())?)
    }
}
