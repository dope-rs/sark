use dope_core::driver::schedule::ready;

#[must_use = "admission exhaustion must re-arm the exact connection owner"]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Progress(State);

#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
enum State {
    #[default]
    Idle,
    Drained,
    Exhausted,
    Both,
}

impl Progress {
    pub const fn has_drained(self) -> bool {
        matches!(self.0, State::Drained | State::Both)
    }

    pub const fn is_exhausted(self) -> bool {
        matches!(self.0, State::Exhausted | State::Both)
    }

    pub fn error<E>(result: Result<Self, E>, wake: ready::Target<'_>) -> Option<E> {
        match result {
            Ok(progress) => {
                if progress.is_exhausted() {
                    wake.wake();
                }
                None
            }
            Err(error) => Some(error),
        }
    }

    /// Records work deferred by bounded admission.
    pub fn exhaust(&mut self) {
        self.0 = match self.0 {
            State::Idle | State::Exhausted => State::Exhausted,
            State::Drained | State::Both => State::Both,
        };
    }

    /// Records work already admitted by its executing owner.
    pub fn record(&mut self) {
        self.0 = match self.0 {
            State::Idle | State::Drained => State::Drained,
            State::Exhausted | State::Both => State::Both,
        };
    }

    pub fn merge(&mut self, other: Self) {
        match other.0 {
            State::Idle => {}
            State::Drained => self.record(),
            State::Exhausted => self.exhaust(),
            State::Both => self.0 = State::Both,
        }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Progress>() == 1);
    assert!(core::mem::align_of::<Progress>() == 1);
};
