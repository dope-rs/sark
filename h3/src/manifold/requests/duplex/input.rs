use o3::{
    buffer::{resident, segments},
    mem,
};
use sark_core::{
    body,
    duplex::{self, admit, input, machine},
    manifold::stream::Boundary,
};
use sark_http::payload::owned;

use crate::{Payload, manifold::requests};

struct Buf<'d> {
    input: segments::Chain<resident::Shared<'d>>,
}

impl<'d> Buf<'d> {
    const fn new(input: segments::Chain<resident::Shared<'d>>) -> Self {
        Self { input }
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

#[repr(transparent)]
pub(in crate::manifold) struct Input<'d, D>(input::State<'d, D, Buf<'d>>)
where
    D: duplex::Session<'d>;

impl<'d, D> Input<'d, D>
where
    D: duplex::Session<'d>,
{
    pub(in crate::manifold) fn open(
        session: D,
        input: segments::Chain<resident::Shared<'d>>,
    ) -> Self {
        Self(input::State::open(session, Buf::new(input)))
    }

    pub(in crate::manifold) fn push(
        &mut self,
        data: Payload<'d>,
        boundary: Boundary,
    ) -> Result<(), mem::Rejected<Payload<'d>, requests::Error>> {
        self.0.push(data, boundary).map_err(|error| match error {
            input::PushError::Finished(input) => {
                mem::Rejected::new(input, requests::Error::Body(body::Error::InputFinished))
            }
            input::PushError::Buffer(fail) => fail,
            input::PushError::Finish { input, error } => {
                mem::Rejected::new(input, requests::Error::Duplex(error))
            }
        })
    }

    pub(in crate::manifold) fn finish(&mut self) -> Result<(), machine::FinishError> {
        self.0.finish()
    }

    pub(in crate::manifold) fn work(&self) -> input::Work {
        self.0.work()
    }

    pub(in crate::manifold) fn discard(self) -> usize {
        o3::buffer::raw::Chunks::len(&self.0.into_buffer().input)
    }

    pub(in crate::manifold) fn transact<R, E>(
        &mut self,
        admit: impl for<'turn> FnOnce(
            admit::Emission<'turn, 'd, D>,
        ) -> admit::Admission<'turn, 'd, D, R, E>,
    ) -> (machine::Transaction<R, E>, input::Feed<usize>) {
        self.0.transact(admit).into_parts()
    }
}

const _: () = {
    assert!(
        core::mem::size_of::<Input<'static, duplex::Never>>()
            <= core::mem::size_of::<segments::Chain<resident::Shared<'static>>>()
                + core::mem::size_of::<machine::Machine<'static, duplex::Never>>()
                + 16
    );
};
