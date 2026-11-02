use std::convert::Infallible;

use dope_fiber::reply::{self, recv, usage};
use protocol::response;
use sark_client::{Error, Failure, Safety, capacity, conn, protocol};

pub(in crate::client) enum Outcome<'d, E = Infallible> {
    Event(response::Event<'d>),
    Error(protocol::Error),
    Terminal(capacity::Error),
    Transport(conn::Error<E>),
}

impl<'d, E> Outcome<'d, E> {
    pub(in crate::client) fn from_result(
        emission: Result<response::Event<'d>, protocol::Error>,
    ) -> Self {
        match emission {
            Ok(event) => Self::Event(event),
            Err(error) => Self::Error(error),
        }
    }

    pub(in crate::client) fn into_result(self) -> Result<response::Event<'d>, Error<E>> {
        match self {
            Self::Event(event) => Ok(event),
            Self::Error(error) => Err(Error::Protocol(error)),
            Self::Transport(error) => Err(Error::Transport(error)),
            Self::Terminal(error) => Err(Error::Capacity(error)),
        }
    }
}

impl<'s, 'b, 'c, 'd, E> recv::Extract<'s, 'b, 'c, 'd, Outcome<'d, E>> for Outcome<'d, E> {
    type Context = ();
    type Output = Result<response::Event<'d>, Failure<E>>;

    fn extract(
        _: &Self::Context,
        slot: &mut reply::Slot<'_, 's, 'b, 'c, 'd, Outcome<'d, E>, Outcome<'d, E>>,
    ) -> Option<Self::Output> {
        let item = slot.pop().or_else(|| slot.pop_error())?;
        Some(
            item.into_result()
                .map_err(|cause| Failure::new(cause, Safety::PossiblyCommitted)),
        )
    }
}

impl<E> usage::Footprint for Outcome<'_, E> {
    fn footprint(&self) -> usage::Usage {
        match self {
            Self::Event(event) => {
                let bytes = match event {
                    response::Event::Informational(head) | response::Event::Head(head) => {
                        head.headers().as_bytes().len()
                    }
                    response::Event::Data(data) => data.len(),
                    response::Event::Trailers(fields) => fields.as_bytes().len(),
                };
                usage::Usage::new(usage::Bytes::new(bytes), usage::Values::new(1))
            }
            Self::Error(_) => usage::Usage::new(usage::Bytes::ZERO, usage::Values::new(1)),
            Self::Terminal(_) | Self::Transport(_) => usage::Usage::ZERO,
        }
    }
}

impl<E> From<&capacity::Error> for Outcome<'_, E> {
    fn from(cause: &capacity::Error) -> Self {
        Self::Terminal(*cause)
    }
}

impl<E: conn::Detail> From<&conn::Error<E>> for Outcome<'_, E> {
    fn from(cause: &conn::Error<E>) -> Self {
        Self::Transport(cause.try_clone().unwrap_or_else(conn::Error::Retain))
    }
}
