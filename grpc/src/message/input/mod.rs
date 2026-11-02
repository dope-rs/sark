use o3::buffer::{self, segments, write::consume};

use crate::message;

mod seal;
pub(super) use seal::Seal;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Consume(consume::Error),
}

/// Monomorphic byte ownership consumed by the gRPC framing state machine.
/// Only retained payload and transport-chunk owners implement this contract:
/// successful copies and transfers consume exactly the requested byte prefix.
pub trait Source<C: buffer::raw::Segment>: Seal<C> {
    fn len(&self) -> usize;

    fn try_copy(&mut self, output: &mut [u8]) -> Result<(), Error>;

    fn try_move(
        &mut self,
        len: usize,
        target: &mut message::Payload<C>,
    ) -> Result<(), segments::Error>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn copy<C: buffer::raw::Segment>(
    input: &mut segments::Chain<C>,
    mut output: &mut [u8],
) -> Result<(), Error> {
    let prefix = segments::Drain::try_new(input, output.len()).map_err(Error::Consume)?;
    let input: &segments::Chain<C> = prefix.as_ref();
    if output.is_empty() {
        prefix.commit();
        return Ok(());
    }
    for segment in input.segments() {
        if output.is_empty() {
            break;
        }
        let bytes = segment.as_ref();
        let len = output.len().min(bytes.len());
        let (target, rest) = output.split_at_mut(len);
        target.copy_from_slice(&bytes[..len]);
        output = rest;
    }
    prefix.commit();
    Ok(())
}

impl<C: buffer::raw::Segment> Source<C> for message::Payload<C> {
    fn len(&self) -> usize {
        self.segments.len()
    }

    fn try_copy(&mut self, output: &mut [u8]) -> Result<(), Error> {
        copy(&mut self.segments, output)
    }

    fn try_move(
        &mut self,
        len: usize,
        target: &mut message::Payload<C>,
    ) -> Result<(), segments::Error> {
        self.segments.try_move_to(&mut target.segments, len)
    }
}

impl<C: buffer::raw::Segment, P> Source<C> for message::DataChunk<C, P> {
    fn len(&self) -> usize {
        self.data.len()
    }

    fn try_copy(&mut self, output: &mut [u8]) -> Result<(), Error> {
        copy(&mut self.data.segments, output)?;
        self.release();
        Ok(())
    }

    fn try_move(
        &mut self,
        len: usize,
        target: &mut message::Payload<C>,
    ) -> Result<(), segments::Error> {
        let moved = self.data.segments.try_move_to(&mut target.segments, len);
        self.release();
        moved
    }
}
