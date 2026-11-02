use core::{hash, mem};
use std::num;

use dope_quic::{errors, wire, wire::stream::Id};
use sark_core::manifold::stream::{Boundary, Section};
use sark_http::{
    self,
    field::metadata::{
        block, packed,
        section::{
            rules,
            state::{RetainedHead, Selected},
        },
    },
};

use crate::frame;

pub enum Event<'d, S = Selected<rules::RetainRaw>, B = packed::Decoded, I: Identity = Id> {
    Settings(frame::Settings),
    Headers {
        message: Message<I>,
        head: RetainedHead<B, S, Option<block::Plan>>,
        section: Section,
    },
    Data {
        message: Message<I>,
        data: crate::Payload<'d>,
    },
    PushPromise {
        message: Message<I>,
        push_id: u64,
        head: RetainedHead<B, S, Option<block::Plan>>,
    },
    CancelPush {
        push_id: u64,
    },
    Finished {
        stream_id: I,
    },
    Reset {
        stream_id: I,
        error_code: errors::Code,
        origin: Origin,
        /// Receive credit already removed from parser storage.
        released: usize,
    },
    /// Malformed message isolated to its original stream; the consumer must cancel it.
    Rejected {
        stream_id: I,
        error: block::ContentError,
    },
    Stopped {
        stream_id: I,
        error_code: errors::Code,
        origin: Origin,
    },
    PushHeaders {
        message: Message<I>,
        push_id: u64,
        head: RetainedHead<B, S, Option<block::Plan>>,
        section: Section,
    },
    PushData {
        message: Message<I>,
        push_id: u64,
        data: crate::Payload<'d>,
    },
    GoAway {
        id: u64,
    },
}

/// Provenance of a terminal event's identity before projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Transport,
    Wire,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Completion<I = Id> {
    More,
    End(I),
    Reset { stream: I, code: errors::Code },
    Stopped { stream: I, code: errors::Code },
}

impl<S, B, I: Identity> Event<'_, S, B, I> {
    pub(crate) fn is_detached(&self) -> bool {
        matches!(
            self,
            Self::Reset {
                origin: Origin::Wire,
                ..
            } | Self::Stopped {
                origin: Origin::Wire,
                ..
            }
        )
    }

    pub fn completion(&self) -> Completion<I> {
        match self {
            Self::Headers { message, .. }
            | Self::Data { message, .. }
            | Self::PushPromise { message, .. }
            | Self::PushHeaders { message, .. }
            | Self::PushData { message, .. } => match message.boundary() {
                Boundary::More => Completion::More,
                Boundary::End => Completion::End(message.stream_id()),
            },
            Self::Finished { stream_id: stream } => Completion::End(*stream),
            Self::Reset {
                stream_id: stream,
                error_code: code,
                ..
            } => Completion::Reset {
                stream: *stream,
                code: *code,
            },
            Self::Rejected { .. } => Completion::More,
            Self::Stopped {
                stream_id: stream,
                error_code: code,
                ..
            } => Completion::Stopped {
                stream: *stream,
                code: *code,
            },
            Self::Settings(_) | Self::CancelPush { .. } | Self::GoAway { .. } => Completion::More,
        }
    }

    /// Returns the stream carrying this event, when it belongs to one.
    pub fn stream_id(&self) -> Option<I> {
        match self {
            Self::Headers { message, .. }
            | Self::Data { message, .. }
            | Self::PushPromise { message, .. }
            | Self::PushHeaders { message, .. }
            | Self::PushData { message, .. } => Some(message.stream_id()),
            Self::Finished { stream_id: stream }
            | Self::Reset {
                stream_id: stream, ..
            }
            | Self::Rejected {
                stream_id: stream, ..
            }
            | Self::Stopped {
                stream_id: stream, ..
            } => Some(*stream),
            Self::Settings(_) | Self::CancelPush { .. } | Self::GoAway { .. } => None,
        }
    }
}

impl<'d, S, B, I: Identity> Event<'d, S, B, I> {
    /// Reprojects the one resident identity without changing event ownership.
    pub fn map_identity<J: Identity, E>(
        self,
        mut map: impl FnMut(I) -> Result<J, E>,
    ) -> Result<Event<'d, S, B, J>, E> {
        Ok(match self {
            Self::Settings(settings) => Event::Settings(settings),
            Self::Headers {
                message,
                head,
                section,
            } => Event::Headers {
                message: message.try_map(&mut map)?,
                head,
                section,
            },
            Self::Data { message, data } => Event::Data {
                message: message.try_map(&mut map)?,
                data,
            },
            Self::PushPromise {
                message,
                push_id,
                head,
            } => Event::PushPromise {
                message: message.try_map(&mut map)?,
                push_id,
                head,
            },
            Self::CancelPush { push_id } => Event::CancelPush { push_id },
            Self::Finished { stream_id: stream } => Event::Finished {
                stream_id: map(stream)?,
            },
            Self::Reset {
                stream_id: stream,
                error_code: code,
                origin,
                released,
            } => Event::Reset {
                stream_id: map(stream)?,
                error_code: code,
                origin,
                released,
            },
            Self::Rejected {
                stream_id: stream,
                error,
            } => Event::Rejected {
                stream_id: map(stream)?,
                error,
            },
            Self::Stopped {
                stream_id: stream,
                error_code: code,
                origin,
            } => Event::Stopped {
                stream_id: map(stream)?,
                error_code: code,
                origin,
            },
            Self::PushHeaders {
                message,
                push_id,
                head,
                section,
            } => Event::PushHeaders {
                message: message.try_map(&mut map)?,
                push_id,
                head,
                section,
            },
            Self::PushData {
                message,
                push_id,
                data,
            } => Event::PushData {
                message: message.try_map(&mut map)?,
                push_id,
                data,
            },
            Self::GoAway { id } => Event::GoAway { id },
        })
    }
}

/// One 63-bit identity supplied by a resident transport domain.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Token(num::NonZero<u64>);

const _: () = assert!(mem::size_of::<Option<Token>>() == mem::size_of::<Token>());

impl Token {
    pub const fn new(raw: u64) -> Option<Self> {
        if raw >> 63 == 0 {
            Some(Self::from_raw(raw))
        } else {
            None
        }
    }

    const fn from_raw(raw: u64) -> Self {
        Self(num::NonZero::<u64>::MIN.saturating_add(raw))
    }

    pub const fn raw(self) -> u64 {
        self.0.get() - 1
    }
}

impl From<Id> for Token {
    fn from(id: Id) -> Self {
        Self(num::NonZero::<u64>::MIN.saturating_add(id.get()))
    }
}

/// Compact identity contract used by the packed semantic event boundary.
pub trait Identity: Copy {
    type Packed: Copy + Eq + hash::Hash;

    fn pack(self, boundary: Boundary) -> Self::Packed;
    fn unpack(packed: Self::Packed) -> (Self, Boundary);
}

impl Identity for Id {
    type Packed = wire::Packed;

    fn pack(self, boundary: Boundary) -> wire::Packed {
        let mark = match boundary {
            Boundary::More => wire::Mark::Clear,
            Boundary::End => wire::Mark::Set,
        };
        wire::Packed::new(self, mark)
    }

    fn unpack(packed: wire::Packed) -> (Self, Boundary) {
        let boundary = match packed.mark() {
            wire::Mark::Clear => Boundary::More,
            wire::Mark::Set => Boundary::End,
        };
        (packed.id(), boundary)
    }
}

impl Identity for Token {
    type Packed = u64;

    fn pack(self, boundary: Boundary) -> u64 {
        let marker = match boundary {
            Boundary::More => 0,
            Boundary::End => 1 << 63,
        };
        self.raw() | marker
    }

    fn unpack(packed: u64) -> (Self, Boundary) {
        let boundary = if packed >> 63 == 0 {
            Boundary::More
        } else {
            Boundary::End
        };
        (Self::from_raw(packed & !(1 << 63)), boundary)
    }
}

/// Resident identity and graceful boundary packed into one word.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Message<I: Identity = Id> {
    packed: I::Packed,
}

impl<I: Identity> Message<I> {
    pub(super) fn new(identity: I, boundary: Boundary) -> Self {
        Self {
            packed: identity.pack(boundary),
        }
    }

    pub fn stream_id(self) -> I {
        I::unpack(self.packed).0
    }

    pub fn boundary(self) -> Boundary {
        I::unpack(self.packed).1
    }

    fn try_map<J: Identity, E>(self, map: impl FnOnce(I) -> Result<J, E>) -> Result<Message<J>, E> {
        Ok(Message::new(map(self.stream_id())?, self.boundary()))
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Role {
    Client,
    Server,
}

#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Phase {
    #[default]
    Idle,
    Headers,
    Data,
    Trailers,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Site {
    Control,
    Request(Phase),
    Push(Phase),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Frame {
    pub stream_id: u64,
    pub kind: u64,
    pub role: Role,
    pub site: Site,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Violation {
    Receive(Frame),
    PromiseFromClient { stream_id: u64, push_id: u64 },
    MaxPushFromServer { push_id: u64 },
}

const _: () = {
    assert!(core::mem::size_of::<Message>() == core::mem::size_of::<Id>());
    assert!(core::mem::size_of::<Message<Token>>() == core::mem::size_of::<Token>());
    assert!(core::mem::size_of::<Completion>() <= 3 * core::mem::size_of::<u64>());
};
const _: () = assert!(core::mem::size_of::<Boundary>() == 1);
const _: () = assert!(core::mem::size_of::<Frame>() <= 3 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Violation>() <= 3 * core::mem::size_of::<usize>());
