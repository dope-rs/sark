use std::time;

use o3::base::time::{Positive, Zero};

use crate::deadline;

/// Validated non-empty windows shared by every protocol adapter.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Config {
    windows: [Window; Self::COUNT],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    ZeroWindow { source: Zero },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Window {
    Disabled,
    Finite(Positive),
}

impl Config {
    const COUNT: usize = 5;

    pub const fn new(phase: deadline::Phase, window: Positive) -> Self {
        let mut windows = [Window::Disabled; Self::COUNT];
        windows[phase as usize] = Window::Finite(window);
        Self { windows }
    }

    pub fn try_new(phase: deadline::Phase, window: time::Duration) -> Result<Self, Error> {
        let window = Self::validate(window)?;
        Ok(Self::new(phase, window))
    }

    pub const fn with(mut self, phase: deadline::Phase, window: Positive) -> Self {
        self.windows[phase as usize] = Window::Finite(window);
        self
    }

    pub fn try_with(self, phase: deadline::Phase, window: time::Duration) -> Result<Self, Error> {
        let window = Self::validate(window)?;
        Ok(self.with(phase, window))
    }

    pub const fn window(self, phase: deadline::Phase) -> Window {
        self.windows[phase as usize]
    }

    fn validate(window: time::Duration) -> Result<Positive, Error> {
        match Positive::try_new(window) {
            Ok(window) => Ok(window),
            Err(source) => Err(Error::ZeroWindow { source }),
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Window>() == std::mem::size_of::<Positive>());
    assert!(std::mem::align_of::<Window>() == std::mem::align_of::<Positive>());
    assert!(std::mem::size_of::<Config>() == 5 * std::mem::size_of::<Positive>());
    assert!(!std::mem::needs_drop::<Config>());
};
