use dope::manifold::client::session::{codec::Admission, response::Outcome};
use o3::buffer::resident;
use sark_http::h1::{framing, head::response::Parts};

use crate::protocol::{Error, codec::State, response};

pub const DATA_LIMIT: usize = 16 * 1024;
pub const INFORMATIONAL_LIMIT: usize = 16;

/// Borrowed syntax available before response ownership and framing commit.
/// Body bytes can still be buffered into one `DATA_LIMIT` item after admission.
pub enum Preview<'a> {
    Head {
        parts: Parts,
        fields: &'a [u8],
    },
    Body {
        pending: &'a [u8],
        data: &'a [u8],
        trailers: Option<[&'a [u8]; 2]>,
        completion: Completion,
    },
}

impl<'a> Preview<'a> {
    pub(super) const fn body(data: &'a [u8], end: Completion) -> Self {
        Self::Body {
            pending: &[],
            data,
            trailers: None,
            completion: end,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Bound {
    bytes: usize,
}

impl Bound {
    pub const fn new(bytes: usize) -> Self {
        Self { bytes }
    }

    pub fn data_items(self) -> usize {
        self.bytes.div_ceil(DATA_LIMIT) * 2
    }
}

pub struct Pending<'d> {
    pub(super) parts: Parts,
    pub(super) fields: response::Fields<resident::Shared<'d>>,
}

pub enum Parsed<'d> {
    Ready(Output<'d>),
    Pending(Pending<'d>),
}

impl<'d> Parsed<'d> {
    pub(super) const fn pending(
        parts: Parts,
        fields: response::Fields<resident::Shared<'d>>,
    ) -> Self {
        Self::Pending(Pending { parts, fields })
    }
}

impl<'d> From<Output<'d>> for Parsed<'d> {
    fn from(output: Output<'d>) -> Self {
        Self::Ready(output)
    }
}

#[must_use = "codec output must be committed or explicitly discarded"]
pub struct Output<'d> {
    events: [Option<Result<response::Event<'d>, Error>>; 2],
    completion: Completion,
    control: Option<framing::Control>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Completion {
    Pending,
    Complete,
}

impl<'d> Output<'d> {
    pub(super) const fn events(
        events: [Option<Result<response::Event<'d>, Error>>; 2],
        state: Completion,
    ) -> Self {
        Self {
            events,
            completion: state,
            control: None,
        }
    }

    pub(super) fn event(event: response::Event<'d>, state: Completion) -> Self {
        Self::events([Some(Ok(event)), None], state)
    }

    pub(super) fn response(
        event: response::Event<'d>,
        state: Completion,
        control: framing::Control,
    ) -> Self {
        Self {
            events: [Some(Ok(event)), None],
            completion: state,
            control: Some(control),
        }
    }

    pub(super) fn silent(state: Completion) -> Self {
        Self::events([None, None], state)
    }

    pub(super) fn error(error: Error) -> Self {
        Self::events([Some(Err(error)), None], Completion::Complete)
    }

    pub const fn completion(&self) -> Completion {
        self.completion
    }

    pub const fn control(&self) -> Option<framing::Control> {
        self.control
    }

    pub fn into_events(self) -> impl Iterator<Item = Result<response::Event<'d>, Error>> {
        self.events.into_iter().flatten()
    }
}

const _: () = {
    assert!(core::mem::size_of::<Completion>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Completion>() == core::mem::align_of::<bool>());
};

pub(super) fn rejected<'d, T: Outcome, F>(
    error: Error,
    state: &mut State<'d>,
    admit: impl FnOnce(Result<Preview<'_>, &Error>) -> Admission<F>,
) -> T
where
    F: FnOnce(Result<Parsed<'d>, Error>, &mut State<'d>) -> T::Accepted,
{
    match admit(Err(&error)) {
        Admission::RetryInput => T::retry_input(),
        Admission::Ready(emit) => T::accept(emit(Err(error), state)),
    }
}
