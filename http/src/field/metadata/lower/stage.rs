use crate::field::metadata::lower::{State, scratch, tagged};

pub(super) enum Stage {
    Ready,
    Scratch(scratch::Scratch),
    Tagged(tagged::Tagged),
}

impl Stage {
    pub(super) fn state(&self) -> State {
        match self {
            Self::Ready => State::Ready,
            Self::Scratch(_) => State::Scratch,
            Self::Tagged(field) => field.state(),
        }
    }

    pub(super) fn try_ready(&self) -> Result<(), State> {
        match self {
            Self::Ready => Ok(()),
            phase => Err(phase.state()),
        }
    }
}
