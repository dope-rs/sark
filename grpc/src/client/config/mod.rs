use std::num::NonZero;

use o3::{buffer::pool, depot::capacity};

mod plan;
use plan::Plan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    ZeroCapacity(Field),
    MaximumCapacity {
        field: Field,
        maximum: usize,
        error: capacity::Error,
    },
    Overflow {
        field: Field,
        operation: Operation,
        left: usize,
        right: usize,
    },
    Pool(pool::LayoutError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    Calls,
    MessageBytes,
    CompletedResponses,
    ResponseEvents,
    InputMessages,
    InputSegments,
    OutputMessages,
    UnaryClaims,
    CompletionSlots,
    InputBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Add,
    Multiply,
}

const DEFAULT_CALLS: NonZero<u32> = NonZero::<u32>::MIN.saturating_add(255);
const DEFAULT_COMPLETED: NonZero<u32> = NonZero::<u32>::MIN.saturating_add(255);
const DEFAULT_EVENTS: NonZero<u32> = NonZero::<u32>::MIN.saturating_add(8191);
const DEFAULT_INPUT_MESSAGES: NonZero<u32> = NonZero::<u32>::MIN.saturating_add(8191);
const DEFAULT_INPUT_SEGMENTS: NonZero<u32> = NonZero::<u32>::MIN.saturating_add(16_383);
const DEFAULT_INPUT_BYTES: usize = 16 << 20;
const DEFAULT_OUTPUT_MESSAGES: NonZero<u32> = NonZero::<u32>::MIN.saturating_add(3);
const DEFAULT_MESSAGE_BYTES: capacity::Count = capacity::Count::new(4 << 20);

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Input {
    completed: NonZero<u32>,
    events: NonZero<u32>,
    messages: NonZero<u32>,
    segments: NonZero<u32>,
    bytes: usize,
}

impl Input {
    const DEFAULT: Self = Self {
        completed: DEFAULT_COMPLETED,
        events: DEFAULT_EVENTS,
        messages: DEFAULT_INPUT_MESSAGES,
        segments: DEFAULT_INPUT_SEGMENTS,
        bytes: DEFAULT_INPUT_BYTES,
    };

    pub fn try_new(
        done: usize,
        events: usize,
        messages: usize,
        segments: usize,
        bytes: usize,
    ) -> Result<Self, Error> {
        let segments = Self::count(Field::InputSegments, segments)?;
        Ok(Self {
            completed: Self::count(Field::CompletedResponses, done)?,
            events: Self::count(Field::ResponseEvents, events)?,
            messages: Self::count(Field::InputMessages, messages)?,
            segments,
            bytes,
        })
    }

    fn count(field: Field, value: usize) -> Result<NonZero<u32>, Error> {
        let value = NonZero::new(value).ok_or(Error::ZeroCapacity(field))?;
        capacity::Count::try_nonzero(value).map_err(|error| Error::MaximumCapacity {
            field,
            maximum: u32::MAX as usize,
            error,
        })
    }

    pub const fn completed(&self) -> usize {
        self.completed.get() as usize
    }

    pub const fn events(&self) -> usize {
        self.events.get() as usize
    }

    pub const fn messages(&self) -> usize {
        self.messages.get() as usize
    }

    pub const fn segments(&self) -> usize {
        self.segments.get() as usize
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    pub(in crate::client) const fn done_count(&self) -> capacity::Count {
        capacity::Count::new(self.completed.get())
    }

    pub(in crate::client) const fn event_count(&self) -> capacity::Count {
        capacity::Count::new(self.events.get())
    }

    pub(in crate::client) const fn owner_count(&self) -> capacity::Count {
        capacity::Count::new(self.messages.get())
    }
}

impl Default for Input {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Output {
    messages: NonZero<u32>,
}

impl Output {
    const DEFAULT: Self = Self {
        messages: DEFAULT_OUTPUT_MESSAGES,
    };

    pub fn try_new(messages: usize) -> Result<Self, Error> {
        Ok(Self {
            messages: Input::count(Field::OutputMessages, messages)?,
        })
    }

    pub const fn messages(&self) -> usize {
        self.messages.get() as usize
    }

    pub(in crate::client) const fn owner_count(&self) -> capacity::Count {
        capacity::Count::new(self.messages.get())
    }
}

impl Default for Output {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    calls: NonZero<u32>,
    bytes: capacity::Count,
    input: Input,
    output: Output,
    plan: Plan,
}

impl Limits {
    const DEFAULT: Self = {
        let layout = pool::Layout::constant::<
            { DEFAULT_OUTPUT_MESSAGES.get() as usize },
            { DEFAULT_MESSAGE_BYTES.get() },
        >();
        Self {
            calls: DEFAULT_CALLS,
            bytes: DEFAULT_MESSAGE_BYTES,
            input: Input::DEFAULT,
            output: Output::DEFAULT,
            plan: Plan {
                layout,
                unary: capacity::Count::new(DEFAULT_CALLS.get() + DEFAULT_COMPLETED.get()),
                slots: DEFAULT_CALLS.get() as usize * 2 + DEFAULT_COMPLETED.get() as usize,
                input_max: DEFAULT_INPUT_BYTES + DEFAULT_MESSAGE_BYTES.get(),
                request_max: layout.bytes(),
            },
        }
    };

    pub fn try_new(
        calls: usize,
        bytes: usize,
        input: Input,
        output: Output,
    ) -> Result<Self, Error> {
        Self::try_parts(
            Input::count(Field::Calls, calls)?,
            try_count(Field::MessageBytes, bytes)?,
            input,
            output,
        )
    }

    fn try_parts(
        calls: NonZero<u32>,
        bytes: capacity::Count,
        input: Input,
        output: Output,
    ) -> Result<Self, Error> {
        let plan = Plan::new(calls, bytes, input, output)?;
        Ok(Self {
            calls,
            bytes,
            input,
            output,
            plan,
        })
    }

    pub fn try_calls(self, calls: usize) -> Result<Self, Error> {
        Self::try_parts(
            Input::count(Field::Calls, calls)?,
            self.bytes,
            self.input,
            self.output,
        )
    }

    pub fn try_bytes(self, bytes: usize) -> Result<Self, Error> {
        Self::try_parts(
            self.calls,
            try_count(Field::MessageBytes, bytes)?,
            self.input,
            self.output,
        )
    }

    pub fn try_input(self, input: Input) -> Result<Self, Error> {
        Self::try_parts(self.calls, self.bytes, input, self.output)
    }

    pub fn try_output(self, output: Output) -> Result<Self, Error> {
        Self::try_parts(self.calls, self.bytes, self.input, output)
    }

    pub const fn calls(&self) -> usize {
        self.calls.get() as usize
    }

    pub const fn bytes(&self) -> usize {
        self.bytes.get()
    }

    pub const fn input(&self) -> Input {
        self.input
    }

    pub const fn output(&self) -> Output {
        self.output
    }

    pub(in crate::client) const fn layout(&self) -> pool::Layout {
        self.plan.layout
    }

    pub(in crate::client) const fn call_count(&self) -> capacity::Count {
        capacity::Count::new(self.calls.get())
    }

    pub(in crate::client) const fn unary_count(&self) -> capacity::Count {
        self.plan.unary
    }

    pub(in crate::client) const fn slots(&self) -> usize {
        self.plan.slots
    }

    pub(in crate::client) const fn input_max(&self) -> usize {
        self.plan.input_max
    }

    pub(in crate::client) const fn request_max(&self) -> usize {
        self.plan.request_max
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

const _: () = {
    assert!(!core::mem::needs_drop::<Input>());
    assert!(!core::mem::needs_drop::<Output>());
    assert!(!core::mem::needs_drop::<Limits>());
};

fn try_count(field: Field, value: usize) -> Result<capacity::Count, Error> {
    capacity::Count::try_from(value).map_err(|error| Error::MaximumCapacity {
        field,
        maximum: u32::MAX as usize,
        error,
    })
}
