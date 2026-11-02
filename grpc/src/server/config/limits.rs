use std::num::NonZero;

use o3::depot::capacity;
use sark_core::manifold::server::app;

use crate::server::config::{
    Input, Output,
    error::{Error, Field},
};

const DEFAULT_CALLS: NonZero<u32> = NonZero::<u32>::MIN.saturating_add(255);
const DEFAULT_MESSAGE_BYTES: capacity::Count = capacity::Count::new(4 * 1024 * 1024);

/// Valid carrier-independent limits for one gRPC server connection.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub(in crate::server) calls: NonZero<u32>,
    pub(in crate::server) message_bytes: capacity::Count,
    input: Input,
    output: Output,
}

impl Limits {
    pub fn try_new(
        calls: usize,
        bytes: usize,
        input: Input,
        output: Output,
    ) -> Result<Self, Error> {
        let calls = Self::try_count(calls)?;
        let bytes = Self::try_size(bytes)?;
        Ok(Self {
            calls,
            message_bytes: bytes,
            input,
            output,
        })
    }

    pub fn try_calls(self, calls: usize) -> Result<Self, Error> {
        Ok(Self {
            calls: Self::try_count(calls)?,
            ..self
        })
    }

    pub fn try_bytes(self, bytes: usize) -> Result<Self, Error> {
        Ok(Self {
            message_bytes: Self::try_size(bytes)?,
            ..self
        })
    }

    fn try_count(calls: usize) -> Result<NonZero<u32>, Error> {
        let calls = NonZero::<usize>::new(calls).ok_or(Error::ZeroCapacity(Field::Calls))?;
        capacity::Count::try_nonzero(calls).map_err(|error| Error::MaximumCapacity {
            field: Field::Calls,
            maximum: u32::MAX as usize,
            error,
        })
    }

    fn try_size(bytes: usize) -> Result<capacity::Count, Error> {
        capacity::Count::try_from(bytes).map_err(|error| Error::MaximumCapacity {
            field: Field::MessageBytes,
            maximum: u32::MAX as usize,
            error,
        })
    }

    #[must_use]
    pub const fn with_input(mut self, input: Input) -> Self {
        self.input = input;
        self
    }

    #[must_use]
    pub const fn with_output(mut self, output: Output) -> Self {
        self.output = output;
        self
    }

    pub const fn bytes(&self) -> usize {
        self.message_bytes.get()
    }

    pub const fn input(&self) -> Input {
        self.input
    }

    pub const fn output(&self) -> Output {
        self.output
    }

    pub(in crate::server) fn layout(self) -> app::Layout {
        app::Layout::new(self.calls.get() as usize, self.input.wire())
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            calls: DEFAULT_CALLS,
            message_bytes: DEFAULT_MESSAGE_BYTES,
            input: Input::DEFAULT,
            output: Output::DEFAULT,
        }
    }
}

const _: () = {
    assert!(!core::mem::needs_drop::<Limits>());
    assert!(core::mem::size_of::<Limits>() == 8 * core::mem::size_of::<usize>());
};
