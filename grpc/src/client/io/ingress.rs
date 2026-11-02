use receive::policies;
use sark_core::{
    life::session::Storage,
    manifold::client::{Connection, domain::Plan, io::Read as _},
};
use sark_h2::{conn, stream};

use crate::{
    client::{call, receive, session},
    protocol::section,
};

pub trait Session: call::Session {
    type Owner: Storage;

    fn ingest(&mut self, bytes: &[u8]) -> Result<(), crate::Status>;
    fn ingest_owned(&mut self, bytes: Self::Owner) -> Result<(), crate::Status>;
    fn ingest_many<I>(&mut self, source: &mut I) -> Result<(), crate::Status>
    where
        I: Iterator<Item = Self::Owner>;
    fn resume(&mut self) -> Result<(), crate::Status>;
    fn is_blocked(&self) -> bool;
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

    fn ingest(&mut self, bytes: &[u8]) -> Result<(), crate::Status> {
        self.state.ingress.take_error()?;
        let result = self.state.connection.ingest(bytes);
        let mut policy = policies::Direct;
        self.state.ingress.drive(
            &mut self.state.connection,
            &mut self.state.calls,
            &mut self.state.egress,
            result,
            &mut policy,
        )
    }

    fn ingest_owned(&mut self, bytes: C) -> Result<(), crate::Status> {
        self.state.ingress.take_error()?;
        let result = self.state.connection.ingest_owned(bytes);
        let mut policy = policies::Direct;
        self.state.ingress.drive(
            &mut self.state.connection,
            &mut self.state.calls,
            &mut self.state.egress,
            result,
            &mut policy,
        )
    }

    fn ingest_many<I>(&mut self, source: &mut I) -> Result<(), crate::Status>
    where
        I: Iterator<Item = C>,
    {
        let mut policy = policies::Direct;
        run(self, source, &mut policy)
    }

    fn resume(&mut self) -> Result<(), crate::Status> {
        self.state.ingress.take_error()?;
        let result = self.state.connection.resume();
        let mut policy = policies::Direct;
        self.state.ingress.drive(
            &mut self.state.connection,
            &mut self.state.calls,
            &mut self.state.egress,
            result,
            &mut policy,
        )
    }

    fn is_blocked(&self) -> bool {
        self.state.ingress.is_blocked()
    }
}

impl<P, C> session::Session<'_, P, C>
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
    pub(in crate::client) fn ingest_many_on<'notify, I>(
        &mut self,
        source: &mut I,
        done: &mut policies::Notify<'notify, '_, '_>,
    ) -> Result<(), crate::Status>
    where
        I: Iterator<Item = C>,
    {
        run(self, source, done)
    }

    pub(in crate::client) fn resume_on<'notify>(
        &mut self,
        done: &mut policies::Notify<'notify, '_, '_>,
    ) -> Result<(), crate::Status> {
        self.state.ingress.take_error()?;
        let result = self.state.connection.resume();
        self.state.ingress.drive(
            &mut self.state.connection,
            &mut self.state.calls,
            &mut self.state.egress,
            result,
            done,
        )
    }
}

fn run<P, C, I, R>(
    client: &mut session::Session<'_, P, C>,
    source: &mut I,
    policy: &mut R,
) -> Result<(), crate::Status>
where
    C: Storage,
    I: Iterator<Item = C>,
    P: Plan<
            C,
            Key = stream::Key,
            Fields = section::Owner,
            Selection = (),
            Connection: Connection<Error = conn::Error>,
        >,
    R: policies::Policy<P, C>,
{
    client.state.ingress.take_error()?;
    if client.state.ingress.is_blocked() {
        client.state.ingress.drive(
            &mut client.state.connection,
            &mut client.state.calls,
            &mut client.state.egress,
            Ok(()),
            policy,
        )?;
        if client.state.ingress.is_blocked() {
            return Ok(());
        }
    }
    let result = client.state.connection.ingest_from(source);
    client.state.ingress.drive(
        &mut client.state.connection,
        &mut client.state.calls,
        &mut client.state.egress,
        result,
        policy,
    )
}
