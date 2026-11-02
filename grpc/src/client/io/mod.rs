use dope::net::link::egress::reserve::exact;
use receive::policies::{Direct, Policy as _};
use sark_core::{
    life::session::Storage,
    manifold::{
        client::{
            Connection,
            domain::Plan,
            io::{self, Write as _},
        },
        schedule,
    },
};
use sark_h2::{conn, stream};

use crate::{
    client::{call, receive, session},
    protocol::section,
};

pub mod ingress;

pub(super) fn release(before: usize, after: usize) -> schedule::Egress {
    if after < before {
        schedule::Egress::Released
    } else {
        schedule::Egress::Unchanged
    }
}

pub enum Flush {
    Written,
    Exhausted,
    Saturated,
    Empty,
}

pub enum Failure {
    Prepare(io::Error),
    Payload(io::Failure),
    Reserve(exact::Failure),
    Drive(crate::Status),
}

type Drive = Result<schedule::Report, crate::Status>;

pub trait Session: call::Session {
    type Owner: Storage;

    fn outbound(&self) -> &[u8];
    fn drain(&mut self, n: usize) -> Drive;
    fn take_write(
        &mut self,
        prefix: &mut [u8],
    ) -> Result<(io::Output<Self::Owner>, Drive), io::Error>;
    fn outbound_len(&self) -> usize;
    fn owner_cap(&self) -> usize;
}

impl<P, C> Session for session::Session<'_, P, C>
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
    type Owner = C;

    fn outbound(&self) -> &[u8] {
        self.state.connection.outbound()
    }

    fn drain(&mut self, n: usize) -> Drive {
        let before = self.state.connection.outbound_len();
        self.state.connection.drain(n);
        let egress = release(before, self.state.connection.outbound_len());
        let mut policy = Direct;
        self.state
            .calls
            .drive(&mut self.state.connection, egress, &mut |record| {
                let _ = policy.cancel(&mut self.state.ingress, record.response);
            })
    }

    fn take_write(&mut self, prefix: &mut [u8]) -> Result<(io::Output<C>, Drive), io::Error> {
        let before = self.state.connection.outbound_len();
        let write = self.state.connection.take(prefix)?;
        let egress = release(before, self.state.connection.outbound_len());
        let mut policy = Direct;
        let driven = self
            .state
            .calls
            .drive(&mut self.state.connection, egress, &mut |record| {
                let _ = policy.cancel(&mut self.state.ingress, record.response);
            });
        Ok((write, driven))
    }

    fn outbound_len(&self) -> usize {
        self.state.connection.outbound_len()
    }

    fn owner_cap(&self) -> usize {
        self.state.egress.available()
    }
}
