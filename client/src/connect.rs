use std::convert::Infallible;

use dope::{
    manifold::client::port::error::Open,
    net::link::{event, pool::change::opening},
};
use dope_fiber::reply;
use sark_http::uri;

#[derive(Debug)]
pub enum Error<M = Infallible> {
    Resolve(uri::Host),
    IdentityExhausted,
    Activation(Open),
    Reply(reply::Error),
    Mailbox(M),
    Attempt(event::ConnectFault),
}

impl<M> From<opening::Exhaustion> for Error<M> {
    fn from(cause: opening::Exhaustion) -> Self {
        match cause {
            opening::Exhaustion::Identity => Self::IdentityExhausted,
            opening::Exhaustion::Submission(error) => Self::Attempt(
                event::ConnectFault::Transport(event::ConnectError::Admission(error)),
            ),
        }
    }
}

impl Error<Infallible> {
    pub fn generalize<M>(self) -> Error<M> {
        match self {
            Self::Resolve(error) => Error::Resolve(error),
            Self::IdentityExhausted => Error::IdentityExhausted,
            Self::Activation(error) => Error::Activation(error),
            Self::Reply(error) => Error::Reply(error),
            Self::Mailbox(error) => match error {},
            Self::Attempt(error) => Error::Attempt(error),
        }
    }
}

const _: () = assert!(size_of::<Error>() <= 5 * size_of::<u32>());
