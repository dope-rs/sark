use std::{num, task};

use dope::{
    core::{
        Errno,
        driver::route::{Identity, KeyTag},
    },
    manifold::{
        app::receive,
        client::{close, deadline},
    },
    net::link::{
        egress::{
            self, load,
            metadata::{self, batch},
            reserve::exact,
        },
        event,
        pool::change::opening,
    },
    runtime::client,
};
use dope_fiber::{abi, wait};
use o3::{
    alloc,
    base::cell::{local, region},
    buffer::{
        hold,
        resident::staging,
        storage,
        write::consume::{self, scope},
    },
};
use sark_http::{field, h1::codec::decode::response, uri::target};

pub mod channel;
mod config;
pub mod headers;
#[doc(hidden)]
pub mod protocol;
#[doc(hidden)]
pub use logic::Logic;
mod logic;
pub mod mode;
mod outbound;
pub mod send;

pub use config::Config;

const DEFAULT_MAX_MESSAGE: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_OUTBOUND_FRAME: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    Overhead { frames: usize, bytes: usize },
    Resident { base: usize, added: usize },
    EntryOverlap { base: usize, added: usize },
    EntryTotal { lanes: usize, each: usize },
    EntryRange { actual: usize },
    EntryZero { lanes: usize, each: usize },
    ResidentOverlap { base: usize, added: usize },
    ResidentTotal { lanes: usize, each: usize },
    ResidentRange { actual: usize },
}

#[derive(Debug)]
pub enum Error {
    NotConnected,
    Exhausted(opening::Exhaustion),
    Closing,
    Admission(egress::Admission),
    Inactive,
    Incomplete(egress::Credit),
    Load(load::Error),
    Rejected(batch::Rejected<storage::Shared>),
    Header(headers::Error),
    Agent(field::Error),
    Allocation(alloc::Error),
    Origin(target::Error),
    Target(target::Error),
    Layout(Layout),
    Staging(staging::Error),
    Lanes(metadata::Count),
    Entries {
        actual: usize,
        cause: num::TryFromIntError,
    },
    Retention(egress::ResidentOut),
    Entropy(Errno),
    Plan(crate::frame::PlanError),
    Batch(batch::Empty),
    Build(storage::BuildError<consume::Error>),
    Payload {
        actual: usize,
        maximum: usize,
    },
    Resident {
        entries: usize,
        frame: usize,
        tail: usize,
    },
}

const _: () = assert!(core::mem::size_of::<Layout>() <= 3 * core::mem::size_of::<usize>());

impl From<crate::frame::PlanError> for Error {
    fn from(error: crate::frame::PlanError) -> Self {
        Self::Plan(error)
    }
}

impl From<storage::BuildError<consume::Error>> for Error {
    fn from(error: storage::BuildError<consume::Error>) -> Self {
        Self::Build(error)
    }
}

impl From<batch::Empty> for Error {
    fn from(error: batch::Empty) -> Self {
        Self::Batch(error)
    }
}

impl From<headers::Error> for Error {
    fn from(error: headers::Error) -> Self {
        Self::Header(error)
    }
}

/// A WebSocket message borrowed for the duration of the handler call.
///
/// The payload cannot escape the callback; applications that need to retain it
/// must make that ownership boundary explicit by copying into their own storage.
///
#[derive(Clone, Copy)]
pub enum Message<'message> {
    Text(&'message str),
    Binary(&'message [u8]),
    Ping(&'message [u8]),
    Pong(&'message [u8]),
}

/// Public, one-shot explanation for a WebSocket connection ending.
pub enum CloseReason {
    RemoteClose { code: Option<u16>, reason: String },
    Allocation(alloc::Exhausted),
    Decode(Fault),
    Protocol,
    Io(Errno),
    Transport,
    Egress(egress::Rejection),
    Reserve(exact::Failure),
    Capacity(close::Capacity),
    EndpointRetired,
    Local(close::LocalOrigin),
    Timeout(deadline::Kind),
    Receive(event::ParkError),
    Ingress(receive::Failure),
    Handshake(Error),
    Send(Error),
}

impl CloseReason {
    fn lifecycle(&self) -> close::Reason {
        match self {
            Self::RemoteClose { .. } => close::Reason::Remote,
            Self::Decode(
                Fault::Frame(crate::inbound::Fault::Allocation(_)) | Fault::Retention(_),
            ) => close::Reason::Local(close::LocalOrigin::ResourceFailure),
            Self::Decode(_) | Self::Protocol => close::Reason::Protocol,
            Self::Io(_) => close::Reason::Transport,
            Self::Transport => close::Reason::Transport,
            Self::Egress(_) => close::Reason::Capacity(close::Capacity::Transport),
            Self::Reserve(cause) => close::Reason::Egress(*cause),
            Self::Capacity(cause) => close::Reason::Capacity(*cause),
            Self::EndpointRetired => close::Reason::EndpointRetired,
            Self::Local(origin) => close::Reason::Local(*origin),
            Self::Timeout(kind) => close::Reason::Timeout(*kind),
            Self::Receive(error) => close::Reason::Receive(*error),
            Self::Ingress(cause) => close::Reason::Ingress(*cause),
            Self::Handshake(_) | Self::Send(_) | Self::Allocation(_) => {
                close::Reason::Local(close::LocalOrigin::ResourceFailure)
            }
        }
    }
}

const _: () = assert!(
    core::mem::size_of::<CloseReason>()
        <= core::mem::size_of::<Error>() + core::mem::size_of::<usize>()
);

pub enum Fault {
    Open(response::Error<crate::upgrade::Error>),
    Handshake(crate::upgrade::Error),
    Input(scope::Error),
    Retention(hold::Error),
    OpenTail { actual: usize },
    Frame(crate::inbound::Fault),
    FrameTail { actual: usize },
}

impl Fault {
    pub(crate) const fn close_code(&self) -> crate::inbound::Code {
        match self {
            Self::Frame(cause) => cause.code(),
            Self::Retention(_) => crate::inbound::Code::Internal,
            _ => crate::inbound::Code::Protocol,
        }
    }
}

pub trait Handshake {
    fn handshake(&mut self, req: headers::Request<'_>) -> Result<headers::Encoded, Error>;
}

pub trait Observer: Handshake {
    fn open(&mut self);
    fn message(&mut self, msg: Message<'_>);
    fn close(&mut self, reason: CloseReason);
}

pub trait Egress<'d, ID: Identity = KeyTag<0>> {
    fn open(&mut self, send: &mut send::Sender<'_, '_, '_, 'd, ID>);
    fn message(&mut self, msg: Message<'_>);
    fn close(&mut self, reason: CloseReason);
}

pub trait Handler<'d> {
    fn handshake(
        &mut self,
        req: headers::Request<'_>,
        local: local::Access<'_, region::Identity<'d>>,
    ) -> Result<headers::Encoded, Error>;
    fn open(&mut self, local: local::Access<'_, region::Identity<'d>>);
    fn message(&mut self, msg: Message<'_>, local: local::Access<'_, region::Identity<'d>>);
    fn close(&mut self, reason: CloseReason, local: local::Access<'_, region::Identity<'d>>);
}

/// Activation failure retaining the caller's exact registration error.
#[derive(Debug)]
pub enum Active<E> {
    Wait(E),
    Exhausted(opening::Exhaustion),
}

impl Active<wait::Error<'_>> {
    pub const fn cause(&self) -> Active<wait::Cause> {
        match self {
            Self::Wait(error) => Active::Wait(error.cause()),
            Self::Exhausted(cause) => Active::Exhausted(*cause),
        }
    }
}

/// Copyable WebSocket operations scoped to the installed connector.
pub struct Handle<'app, 'life: 'app, 'e: 'life, 'd: 'e, B: ?Sized, ID: Identity = KeyTag<0>> {
    port: &'life channel::Port<'e, 'd, ID>,
    _bound: client::Bound<'app, 'd, B>,
}

impl<B: ?Sized, ID: Identity> Copy for Handle<'_, '_, '_, '_, B, ID> {}

impl<B: ?Sized, ID: Identity> Clone for Handle<'_, '_, '_, '_, B, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'app, 'life: 'app, 'e: 'life, 'd: 'e, B: ?Sized, ID: Identity>
    Handle<'app, 'life, 'e, 'd, B, ID>
{
    pub fn wait_active<'b>(
        &'b self,
    ) -> impl abi::Fiber<'d, Output = Result<(), Active<wait::Error<'d>>>> + 'b {
        use dope_fiber::task::done::Wait;
        let handle = *self;
        Wait::new(move |cx, waiter| {
            let shared = &handle.port.shared;
            match handle.port.active() {
                Ok(true) => return task::Poll::Ready(Ok(())),
                Err(cause) => return task::Poll::Ready(Err(Active::Exhausted(cause))),
                Ok(false) => {}
            }
            if let Err(error) = shared.active_waiters.try_register(waiter, cx.as_ref()) {
                return task::Poll::Ready(Err(Active::Wait(error)));
            }
            task::Poll::Pending
        })
    }

    pub fn send_text<'b>(
        &'b self,
        data: &'b str,
    ) -> impl abi::Fiber<'d, Output = Result<(), Error>> + 'b {
        self.port.send(0x1, data.as_bytes())
    }

    /// Claims the connector's sole capacity waiter until the driver is dropped.
    pub fn try_driver(&self) -> Result<send::Driver<'app, 'life, 'e, 'd, B, ID>, send::Claimed> {
        if self.port.shared.claimed.replace(true) {
            return Err(send::Claimed);
        }
        Ok(send::Driver { handle: *self })
    }

    pub fn send_binary<'b>(
        &'b self,
        data: &'b [u8],
    ) -> impl abi::Fiber<'d, Output = Result<(), Error>> + 'b {
        self.port.send(0x2, data)
    }

    pub fn close<'b>(
        &'b self,
        close: crate::inbound::Close<'b>,
    ) -> impl abi::Fiber<'d, Output = Result<(), Error>> + 'b {
        self.port.close(close)
    }
}
