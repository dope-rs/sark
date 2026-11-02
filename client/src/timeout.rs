use dope_fiber::task::time::budget;

/// Exact call phase and budget bound that expired.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(C)]
#[derive(Debug)]
pub struct Error {
    phase: Phase,
    elapsed: budget::Elapsed,
}

/// Exact call phase whose deadline expired.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Phase {
    Dispatch,
    Head,
    Body,
    Decode,
    Backoff,
}

impl Error {
    pub const fn phase(self) -> Phase {
        self.phase
    }

    pub const fn elapsed(self) -> budget::Elapsed {
        self.elapsed
    }
}

impl Phase {
    pub const fn expire(self, elapsed: budget::Elapsed) -> Error {
        Error {
            phase: self,
            elapsed,
        }
    }
}

const _: () = {
    assert!(size_of::<Phase>() == 1);
    assert!(size_of::<Error>() == 2);
    assert!(size_of::<Error>() <= size_of::<budget::Error>());
    assert!(align_of::<Error>() <= align_of::<budget::Error>());
};
