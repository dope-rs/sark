use std::{mem, str};

use o3::alloc;

mod text;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    UnexpectedNonContinuation,
    UnexpectedContinuation,
    PayloadTooLarge,
}

pub(crate) enum PrepareError {
    Text(Utf8),
    Allocation(alloc::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Utf8 {
    pub valid: usize,
    pub error: Option<usize>,
}

impl Utf8 {
    fn at(base: usize, error: str::Utf8Error) -> Self {
        Self {
            valid: base + error.valid_up_to(),
            error: error.error_len(),
        }
    }

    fn check(bytes: &[u8], base: usize) -> Result<(), Self> {
        match str::from_utf8(bytes) {
            Ok(_) => Ok(()),
            Err(error) => Err(Self::at(base, error)),
        }
    }
}

impl From<str::Utf8Error> for Utf8 {
    fn from(error: str::Utf8Error) -> Self {
        Self::at(0, error)
    }
}

pub enum Push<'a> {
    DirectText(&'a str),
    DirectBinary(&'a [u8]),
    AssembledText(String),
    AssembledBinary(Vec<u8>),
    NeedMore,
}

/// One message committed by the frame whose admission already bounded it.
///
/// [`Push::NeedMore`] cannot be represented here: a value of this type exists
/// only for a frame that completed a message.
pub(crate) enum Message<'a> {
    WholeText(&'a str),
    WholeBinary(&'a [u8]),
    JoinedText(String),
    JoinedBinary(Vec<u8>),
}

pub(crate) struct Buffer {
    opcode: Option<Kind>,
    payload: Vec<u8>,
    max_payload: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Text,
    Binary,
}

/// The exact transition one admitted header performs, resolved once: a
/// completing frame's message length is fixed in the same step, so every later
/// layer reads the decision instead of re-deriving it from the same proof.
#[derive(Clone, Copy)]
pub(crate) enum Admission {
    /// This frame completes one message of exactly this length.
    Message(Complete),
    /// This frame leaves a partially assembled message awaiting more input.
    Fragment(Partial),
}

/// A frame that completes exactly one message of a known length.
#[derive(Clone, Copy)]
pub(crate) enum Complete {
    /// One whole message carried by this frame alone.
    Whole { kind: Kind, len: usize },
    /// The final fragment of a message whose total length is exactly `len`.
    Joined { kind: Kind, len: usize },
}

/// A frame that extends the partially assembled message it continues.
#[derive(Clone, Copy)]
pub(crate) enum Partial {
    Begin(Kind),
    Continue,
}

const _: () = assert!(mem::size_of::<Buffer>() == 5 * mem::size_of::<usize>());

impl Buffer {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            opcode: None,
            payload: Vec::new(),
            max_payload: max,
        }
    }

    pub(crate) fn admit(&self, opcode: u8, fin: bool, len: usize) -> Result<Admission, Error> {
        match opcode {
            0x1 => self.start(Kind::Text, fin, len),
            0x2 => self.start(Kind::Binary, fin, len),
            0x0 => {
                let opcode = self.opcode.ok_or(Error::UnexpectedContinuation)?;
                let Some(total) = self.payload.len().checked_add(len) else {
                    return Err(Error::PayloadTooLarge);
                };
                if total > self.max_payload {
                    return Err(Error::PayloadTooLarge);
                }
                Ok(if fin {
                    Admission::Message(Complete::Joined {
                        kind: opcode,
                        len: total,
                    })
                } else {
                    Admission::Fragment(Partial::Continue)
                })
            }
            _ => Err(Error::UnexpectedNonContinuation),
        }
    }

    fn start(&self, opcode: Kind, fin: bool, len: usize) -> Result<Admission, Error> {
        if self.opcode.is_some() {
            return Err(Error::UnexpectedNonContinuation);
        }
        if len > self.max_payload {
            return Err(Error::PayloadTooLarge);
        }
        Ok(if fin {
            Admission::Message(Complete::Whole { kind: opcode, len })
        } else {
            Admission::Fragment(Partial::Begin(opcode))
        })
    }

    /// Validates and reserves without publishing bytes or fragment progress.
    pub(crate) fn prepare<'state, 'payload>(
        &'state mut self,
        proof: Admission,
        data: &'payload [u8],
    ) -> Result<Prepared<'state, 'payload>, PrepareError> {
        match proof {
            Admission::Message(complete) => complete.prepare(self, data).map(Prepared::Message),
            Admission::Fragment(partial) => Ok(Prepared::Fragment(Pending {
                append: Append::try_new(self, data).map_err(PrepareError::Allocation)?,
                partial,
            })),
        }
    }
}

/// Reserves the exact suffix and retains its source and exclusive owner until commit.
struct Append<'state, 'payload> {
    buffer: &'state mut Buffer,
    data: &'payload [u8],
}

impl<'state, 'payload> Append<'state, 'payload> {
    fn try_new(buffer: &'state mut Buffer, data: &'payload [u8]) -> Result<Self, alloc::Error> {
        alloc::VecExt::try_reserve(&mut buffer.payload, data.len())?;
        Ok(Self { buffer, data })
    }

    fn commit(self) -> &'state mut Buffer {
        self.buffer.payload.extend_from_slice(self.data);
        self.buffer
    }

    fn join(self) -> Vec<u8> {
        let buffer = self.commit();
        buffer.opcode = None;
        mem::take(&mut buffer.payload)
    }
}

impl Complete {
    fn prepare<'state, 'payload>(
        self,
        buffer: &'state mut Buffer,
        data: &'payload [u8],
    ) -> Result<Ready<'state, 'payload>, PrepareError> {
        let (kind, len) = match self {
            Self::Whole { kind, len } => (
                ReadyKind::whole(kind, data).map_err(PrepareError::Text)?,
                len,
            ),
            Self::Joined { kind, len } => (ReadyKind::joined(buffer, kind, data)?, len),
        };
        Ok(Ready { kind, len })
    }
}

pub(crate) enum Prepared<'state, 'payload> {
    Message(Ready<'state, 'payload>),
    Fragment(Pending<'state, 'payload>),
}

impl<'payload> Prepared<'_, 'payload> {
    pub(crate) fn commit(self) -> Push<'payload> {
        match self {
            Self::Message(message) => match message.commit() {
                Message::WholeText(text) => Push::DirectText(text),
                Message::WholeBinary(data) => Push::DirectBinary(data),
                Message::JoinedText(text) => Push::AssembledText(text),
                Message::JoinedBinary(data) => Push::AssembledBinary(data),
            },
            Self::Fragment(fragment) => {
                fragment.commit();
                Push::NeedMore
            }
        }
    }
}

/// A validated completion retains the source and any fragment owner until commit.
pub(crate) struct Ready<'state, 'payload> {
    kind: ReadyKind<'state, 'payload>,
    len: usize,
}

enum ReadyKind<'state, 'payload> {
    WholeText(&'payload str),
    WholeBinary(&'payload [u8]),
    JoinedText(text::Text<'state, 'payload>),
    JoinedBinary(Append<'state, 'payload>),
}

impl<'state, 'payload> ReadyKind<'state, 'payload> {
    fn whole(kind: Kind, data: &'payload [u8]) -> Result<Self, Utf8> {
        match kind {
            Kind::Text => str::from_utf8(data)
                .map(Self::WholeText)
                .map_err(Utf8::from),
            Kind::Binary => Ok(Self::WholeBinary(data)),
        }
    }

    fn joined(
        buffer: &'state mut Buffer,
        kind: Kind,
        data: &'payload [u8],
    ) -> Result<Self, PrepareError> {
        match kind {
            Kind::Text => text::Text::try_new(buffer, data).map(Self::JoinedText),
            Kind::Binary => Append::try_new(buffer, data)
                .map(Self::JoinedBinary)
                .map_err(PrepareError::Allocation),
        }
    }
}

impl<'payload> Ready<'_, 'payload> {
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn commit(self) -> Message<'payload> {
        match self.kind {
            ReadyKind::WholeText(text) => Message::WholeText(text),
            ReadyKind::WholeBinary(data) => Message::WholeBinary(data),
            ReadyKind::JoinedText(text) => Message::JoinedText(text.commit()),
            ReadyKind::JoinedBinary(append) => Message::JoinedBinary(append.join()),
        }
    }
}

pub(crate) struct Pending<'state, 'payload> {
    append: Append<'state, 'payload>,
    partial: Partial,
}

impl Pending<'_, '_> {
    pub(crate) fn commit(self) {
        let buffer = self.append.commit();
        if let Partial::Begin(kind) = self.partial {
            buffer.opcode = Some(kind);
        }
    }
}
