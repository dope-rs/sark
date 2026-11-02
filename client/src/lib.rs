use std::{convert::Infallible, num};

use dope_fiber::{reply::recv::attach, task::time::budget};
use o3::{alloc, depot::queue::tagged::lane};
use protocol::redirect;
use reply::detach;
use sark_http::{field, payload::compress::inflate, uri};

mod failure;
mod safety;

pub mod capacity;
pub mod conn;
pub mod connect;
pub mod protocol;
pub mod reply;
pub mod timeout;

pub use failure::Failure;
pub use safety::Safety;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum OriginError {
    Lane(lane::Error<num::NonZero<u32>>),
    HttpsRequiresTls,
    UnsupportedScheme,
    MissingScheme,
    AuthorityTooLong,
    Capacity,
    Port(uri::PortError),
    Allocation(alloc::Exhausted),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RequestError {
    HeaderName(field::Error),
    HeaderValue(field::Error),
    ReservedHeader,
    UnsupportedTransition,
    CrossOrigin,
}

#[derive(Debug)]
pub enum Error<E = Infallible, M = Infallible> {
    NotConnected,
    /// An established transport failed, with its stable category and any
    /// wire-defined detail preserved by [`conn::Error`].
    Transport(conn::Error<E>),
    Capacity(capacity::Error),
    Timeout(timeout::Error),
    Timer(budget::Error),
    Origin(OriginError),
    /// A transport connection could not be established.
    Connect(connect::Error<M>),
    Protocol(protocol::Error),
    Redirect(redirect::Error),
    Request(RequestError),
    Decompression(inflate::DecodeError),
    Reply(attach::Error),
    Detach(detach::Error),
}

impl<M> Error<Infallible, M> {
    /// Lifts a cause that cannot contain wire detail into a caller's exact
    /// wire error type without allocation or formatting.
    #[doc(hidden)]
    pub fn generalize<E>(self) -> Error<E, M> {
        match self {
            Self::NotConnected => Error::NotConnected,
            Self::Transport(error) => Error::Transport(error.generalize()),
            Self::Capacity(error) => Error::Capacity(error),
            Self::Timeout(error) => Error::Timeout(error),
            Self::Timer(error) => Error::Timer(error),
            Self::Origin(error) => Error::Origin(error),
            Self::Connect(error) => Error::Connect(error),
            Self::Protocol(error) => Error::Protocol(error),
            Self::Redirect(error) => Error::Redirect(error),
            Self::Request(error) => Error::Request(error),
            Self::Decompression(error) => Error::Decompression(error),
            Self::Reply(error) => Error::Reply(error),
            Self::Detach(error) => Error::Detach(error),
        }
    }
}

impl<E> Error<E> {
    pub fn widen<M>(self) -> Error<E, M> {
        match self {
            Self::NotConnected => Error::NotConnected,
            Self::Transport(error) => Error::Transport(error),
            Self::Capacity(error) => Error::Capacity(error),
            Self::Timeout(error) => Error::Timeout(error),
            Self::Timer(error) => Error::Timer(error),
            Self::Origin(error) => Error::Origin(error),
            Self::Connect(error) => Error::Connect(error.generalize()),
            Self::Protocol(error) => Error::Protocol(error),
            Self::Redirect(error) => Error::Redirect(error),
            Self::Request(error) => Error::Request(error),
            Self::Decompression(error) => Error::Decompression(error),
            Self::Reply(error) => Error::Reply(error),
            Self::Detach(error) => Error::Detach(error),
        }
    }
}
