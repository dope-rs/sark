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
        self.state.drive_ingress(result, &mut policies::Direct)
    }

    fn ingest_owned(&mut self, bytes: C) -> Result<(), crate::Status> {
        self.state.ingress.take_error()?;
        let result = self.state.connection.ingest_owned(bytes);
        self.state.drive_ingress(result, &mut policies::Direct)
    }

    fn ingest_many<I>(&mut self, source: &mut I) -> Result<(), crate::Status>
    where
        I: Iterator<Item = C>,
    {
        self.ingest_many_on(source, &mut policies::Direct)
    }

    fn resume(&mut self) -> Result<(), crate::Status> {
        self.resume_on(&mut policies::Direct)
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
    pub(in crate::client) fn ingest_many_on<I, R>(
        &mut self,
        source: &mut I,
        policy: &mut R,
    ) -> Result<(), crate::Status>
    where
        I: Iterator<Item = C>,
        R: policies::Policy<P, C>,
    {
        self.state.ingress.take_error()?;
        if self.state.ingress.is_blocked() {
            self.state.drive_ingress(Ok(()), policy)?;
            if self.state.ingress.is_blocked() {
                return Ok(());
            }
        }
        let result = self.state.connection.ingest_from(source);
        self.state.drive_ingress(result, policy)
    }

    pub(in crate::client) fn resume_on<R: policies::Policy<P, C>>(
        &mut self,
        done: &mut R,
    ) -> Result<(), crate::Status> {
        self.state.ingress.take_error()?;
        let result = self.state.connection.resume();
        self.state.drive_ingress(result, done)
    }
}
