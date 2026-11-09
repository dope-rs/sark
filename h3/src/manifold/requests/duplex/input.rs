use o3::{
    buffer::{resident, segments},
    mem,
};
use sark_core::duplex::{self, input, machine};
use sark_http::payload::owned;

use crate::{Payload, manifold::requests};

pub(in crate::manifold) struct Buf<'d> {
    input: segments::Chain<resident::Shared<'d>>,
}

impl<'d> Buf<'d> {
    pub(in crate::manifold::requests) const fn new(
        input: segments::Chain<resident::Shared<'d>>,
    ) -> Self {
        Self { input }
    }

    pub(in crate::manifold::requests) fn discard(self) -> usize {
        o3::buffer::raw::Chunks::len(&self.input)
    }
}

impl<'d> input::Buf<'d> for Buf<'d> {
    type Input = Payload<'d>;
    type Error = requests::Error;
    type Receipt = usize;

    fn state(&self) -> input::Buffer {
        match self.input.is_empty() {
            true => input::Buffer::Empty,
            false => input::Buffer::Queued,
        }
    }

    fn push(&mut self, input: Self::Input) -> Result<(), mem::Rejected<Self::Input, Self::Error>> {
        match input.into_body().into_shape() {
            segments::Shape::Empty => Ok(()),
            segments::Shape::One(owner) => match self.input.try_insert(owner) {
                Ok(_) => Ok(()),
                Err(fail) => {
                    let (owner, error) = fail.into_parts();
                    Err(mem::Rejected::new(
                        Payload::from_body(owned::Segments::from(owner)),
                        requests::Error::Segments(error),
                    ))
                }
            },
            segments::Shape::Many(owners) => match self.input.try_extend(owners) {
                Ok(()) => Ok(()),
                Err(fail) => {
                    let (owners, error) = fail.into_parts();
                    Err(mem::Rejected::new(
                        Payload::from_body(owned::Segments::from(segments::Shape::Many(owners))),
                        requests::Error::Segments(error),
                    ))
                }
            },
        }
    }

    fn take(&mut self) -> input::Take<'d, Self::Receipt> {
        let Some(owner) = self.input.front_entry().map(segments::Front::take) else {
            return input::Take::Empty;
        };
        let bytes = owner.as_ref().len();
        input::Take::Item {
            input: owned::Segments::from(owner),
            receipt: bytes,
        }
    }
}

pub(in crate::manifold) type Input<'d, D> = input::State<'d, D, Buf<'d>>;

const _: () = {
    assert!(
        core::mem::size_of::<Input<'static, duplex::Never>>()
            <= core::mem::size_of::<segments::Chain<resident::Shared<'static>>>()
                + core::mem::size_of::<machine::Machine<'static, duplex::Never>>()
                + 16
    );
};
