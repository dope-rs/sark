use sark_core::{
    life::session,
    manifold::client::{Connection, domain::Plan},
};
use sark_h2::{conn, stream};

use crate::{
    Status,
    client::{receive, state::calls},
    protocol::section,
};

pub(in crate::client) trait Ticket<P, C>: Copy
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    fn reserve(ingress: &mut receive::Assembler<P, C>) -> Result<(Self, calls::Target), Status>;
}

impl<P, C> Ticket<P, C> for receive::UnaryKey
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    fn reserve(ingress: &mut receive::Assembler<P, C>) -> Result<(Self, calls::Target), Status> {
        let key = ingress.claim_unary()?;
        Ok((key, calls::Target::Unary(key)))
    }
}

impl<P, C> Ticket<P, C> for receive::StreamKey
where
    C: session::Storage,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
{
    fn reserve(ingress: &mut receive::Assembler<P, C>) -> Result<(Self, calls::Target), Status> {
        let key = ingress.claim_stream()?;
        Ok((key, calls::Target::Streaming(key)))
    }
}
