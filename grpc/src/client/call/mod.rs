use sark_core::manifold::client::Connection;
use sark_h2::conn;

use crate::Reason;
pub mod bidi;
pub mod request;
pub mod response;
pub mod unary;

use sark_core::{life::session::Storage, manifold::client::domain::Plan};
use sark_h2::stream;

use crate::{
    client::{receive, session},
    protocol::section,
};

#[must_use = "pending admission must be retried or rejected"]
pub enum Admission<T> {
    Ready(T),
    Pending,
    Rejected(crate::Status),
}

impl<T> Admission<T> {
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> Admission<U> {
        match self {
            Self::Ready(value) => Admission::Ready(map(value)),
            Self::Pending => Admission::Pending,
            Self::Rejected(status) => Admission::Rejected(status),
        }
    }

    pub fn into_result(self) -> Result<T, crate::Status> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Pending => Err(Self::reject()),
            Self::Rejected(status) => Err(status),
        }
    }

    pub fn reject() -> crate::Status {
        crate::Status::new(crate::Code::ResourceExhausted, Reason::RequestOwners)
    }
}

pub trait Session {
    type Unary: receive::Unary;
    type Event: receive::Event;
    type Call;
    type Request;
    type Response;
    type Bidi;
    type Send;
}

impl<'scope, P, C> Session for session::Session<'scope, P, C>
where
    C: Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    type Unary = receive::UnaryResult<C>;
    type Event = receive::Response<C>;
    type Call = unary::Handle<'scope, P::Key>;
    type Request = request::Stream<'scope, P::Key>;
    type Response = response::Stream<'scope, P::Key>;
    type Bidi = bidi::Stream<'scope, P::Key>;
    type Send = bidi::Send<'scope, P::Key>;
}
