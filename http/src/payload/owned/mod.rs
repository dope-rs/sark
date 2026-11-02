use std::{num::NonZero, ops::ControlFlow};

use o3::{
    buffer::{resident, storage},
    mem,
};

pub mod flatten;
pub mod iter;

mod error;
mod segment;
mod segments;

pub use error::Error;
use flatten::State;
pub use segment::Segment;
pub use segments::Segments;

/// Retained input or its single admitted, cooperatively copied output.
#[must_use = "step or release the retained body"]
pub struct Flatten<'d>(State<'d>);

impl<'d> Flatten<'d> {
    pub const fn new(source: Segments<'d>) -> Self {
        Self(State::Input(source))
    }

    pub fn try_push(
        self,
        segment: Segment<'d>,
    ) -> Result<Self, mem::Rejected<(Self, Segment<'d>), flatten::Error>> {
        match self.0 {
            State::Input(source) => match source.try_push(segment) {
                Ok(source) => Ok(Self::new(source)),
                Err(rejected) => {
                    let ((source, segment), error) = rejected.into_parts();
                    Err(mem::Rejected::new(
                        (Self::new(source), segment),
                        flatten::Error::Assembly(error),
                    ))
                }
            },
            state @ State::Copying(_) => Err(mem::Rejected::new(
                (Self(state), segment),
                flatten::Error::Finished,
            )),
        }
    }

    /// Empty and single-owner bodies complete without allocating or copying.
    /// Multi-owner bodies reserve once, then copy at most `budget` bytes per call.
    pub fn try_step(
        self,
        budget: NonZero<usize>,
    ) -> Result<ControlFlow<resident::Shared<'d>, Self>, mem::Rejected<Self, storage::AllocError>>
    {
        use o3::buffer::segments;

        let copy = match self.0 {
            State::Input(source) => match source.storage {
                segments::Shape::Empty => return Ok(ControlFlow::Break(resident::Shared::new())),
                segments::Shape::One(owner) => return Ok(ControlFlow::Break(owner.into_inner())),
                segments::Shape::Many(source) => match segments::Flatten::try_new(source) {
                    Ok(copy) => copy,
                    Err(rejected) => {
                        let (source, error) = rejected.into_parts();
                        return Err(mem::Rejected::new(
                            Self::new(Segments::from(segments::Shape::Many(source))),
                            error,
                        ));
                    }
                },
            },
            State::Copying(copy) => copy,
        };
        Ok(match copy.step(budget) {
            ControlFlow::Break(output) => ControlFlow::Break(resident::Shared::from(output)),
            ControlFlow::Continue(copy) => ControlFlow::Continue(Self(State::Copying(copy))),
        })
    }
}
