use core::convert::Infallible;
use sark_core::manifold::{schedule::typed, task::response::State};
use sark_http::payload::response::status::Code;

use crate::{conn, frame::numbers};

pub(super) type Stored<'d, P, D, X> =
    o3::depot::queue::indexed::lanes::typed::Value<Response<'d, P, D, X>, Data<'d, D>, Infallible>;

type Progress<'d, R> = Result<
    typed::Control<
        super::Reply<'d, R>,
        super::Data<'d, R>,
        Infallible,
        conn::DataBlock,
        numbers::ErrorCode,
    >,
    numbers::ErrorCode,
>;

pub(super) mod deadline;
mod driver;
pub(in crate::server) mod expiry;
pub(super) mod input;
mod opening;
pub(super) mod raw;
pub(super) use driver::Driver;

pub(super) enum Count {
    Pending,
    Settled,
}

pub(super) enum Duplex<I, E = PushError<I, payload::Error>> {
    Other(I),
    Applied(Result<(), E>),
}

use o3::buffer::resident;
use sark_core::{
    decode::{exec, request},
    duplex::{self, input::PushError},
    life::{self, exchange},
    manifold::stream::Boundary,
};
use sark_http::{field::metadata::section::rules, owner::cause, payload::owned};

use crate::{
    conn::event,
    server::{
        payload,
        response::{Head, Pending},
    },
};

pub(super) enum Response<'d, P, D: duplex::Session<'d>, X> {
    Status(Code),
    Sync(request::Ready<exec::Sync, P, owned::Segments<'d>>),
    Fixed(X),
    Task,
    End,
    Head {
        head: Head<'d>,
        state: State,
        count: Count,
    },
    DuplexOpen {
        request: input::Request<'d, P>,
    },
    DuplexHead {
        head: Head<'d>,
        input: input::Input<'d, D>,
    },
}

pub(super) enum Data<'d, D: duplex::Session<'d>> {
    Body { pending: Pending<'d>, state: State },
    Duplex { input: input::Input<'d, D> },
}

impl<'d, P, D, X> Response<'d, P, D, X>
where
    D: duplex::Session<'d>,
{
    pub(in crate::server::task) fn push(
        self,
        data: event::DataPayload<resident::Shared<'d>>,
        boundary: Boundary,
    ) -> (Self, Duplex<event::DataPayload<resident::Shared<'d>>>) {
        match self {
            Self::DuplexOpen { mut request } => match boundary {
                Boundary::More => {
                    let result = request.append(data).map_err(PushError::Buffer);
                    (Self::DuplexOpen { request }, Duplex::Applied(result))
                }
                Boundary::End => {
                    let (request, result) = match request.try_end(data) {
                        Ok(request) => (request, Ok(())),
                        Err((rejected, request)) => (request, Err(PushError::Buffer(rejected))),
                    };
                    (Self::DuplexOpen { request }, Duplex::Applied(result))
                }
            },
            Self::DuplexHead { head, mut input } => {
                let result = input.push(data, boundary);
                (Self::DuplexHead { head, input }, Duplex::Applied(result))
            }
            other => (other, Duplex::Other(data)),
        }
    }

    pub(in crate::server::task) fn finish(self) -> (Self, Duplex<(), input::Error>) {
        match self {
            Self::DuplexOpen { request } => {
                let (request, result) = match request.try_finish() {
                    Ok(request) => (request, Ok(())),
                    Err(rejected) => {
                        let (request, error) = rejected.into_parts();
                        (request, Err(input::Error::Input(error)))
                    }
                };
                (Self::DuplexOpen { request }, Duplex::Applied(result))
            }
            Self::DuplexHead { head, mut input } => {
                let result = input.finish().map_err(input::Error::Machine);
                (Self::DuplexHead { head, input }, Duplex::Applied(result))
            }
            other => (other, Duplex::Other(())),
        }
    }
}

impl<'d, D: duplex::Session<'d>> Data<'d, D> {
    pub(in crate::server::task) fn push(
        &mut self,
        data: event::DataPayload<resident::Shared<'d>>,
        boundary: Boundary,
    ) -> Duplex<event::DataPayload<resident::Shared<'d>>> {
        match self {
            Self::Duplex { input } => Duplex::Applied(input.push(data, boundary)),
            Self::Body { .. } => Duplex::Other(data),
        }
    }

    pub(in crate::server::task) fn finish(&mut self) -> Duplex<(), input::Error> {
        match self {
            Self::Duplex { input } => {
                Duplex::Applied(input.finish().map_err(input::Error::Machine))
            }
            Self::Body { .. } => Duplex::Other(()),
        }
    }
}

/// Releases one queued response without deciding the exchange's terminal outcome.
pub(in crate::server::task) fn discard<'d, C, P, D, F, M, X>(
    value: Stored<'d, P, D, X>,
    connection: &mut conn::machine::Connection<crate::ServerRole, F, resident::Shared<'d>>,
    exchange: exchange::Id<C, crate::stream::Key>,
    mode: &mut M,
    cause: cause::Staging,
) where
    C: Copy,
    D: duplex::Session<'d>,
    F: rules::Policy,
    M: life::Mode<C, crate::stream::Key>,
{
    use o3::depot::queue::indexed::lanes::typed::Value;
    match value {
        Value::Data(Data::Body { pending, .. }) => {
            super::release_pending(mode, exchange, pending, cause);
        }
        Value::Control(Response::Head { head, .. }) => {
            head.discard(connection);
        }
        Value::Control(Response::DuplexHead { head, input }) => {
            head.discard(connection);
            drop(input);
            connection.release_duplex(exchange.stream());
        }
        Value::Data(Data::Duplex { input }) => {
            drop(input);
            connection.release_duplex(exchange.stream());
        }
        _ => {}
    }
}
