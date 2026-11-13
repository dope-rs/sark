use core::ops::ControlFlow;

use o3::buffer::storage;
use sark_core::{life::session, manifold::stream};
use sark_http::{field, field::metadata::section::rules};

use crate::{
    ServerRole,
    conn::{self, event, machine},
    stream::Key,
};

/// Exclusive write turn for one H2 server stream.
pub struct Writer<'a, F, C, E>
where
    F: rules::Policy,
    C: session::Storage,
    E: event::Evidence,
{
    connection: &'a mut machine::Connection<ServerRole, F, C, rules::RejectPush, E>,
    key: Key,
}

impl<'a, F, C, E> Writer<'a, F, C, E>
where
    F: rules::Policy,
    C: session::Storage,
    E: event::Evidence,
{
    pub(in crate::conn::machine) fn new(
        conn: &'a mut machine::Connection<ServerRole, F, C, rules::RejectPush, E>,
        key: Key,
    ) -> Self {
        Self {
            connection: conn,
            key,
        }
    }
}

impl<F, C, E> stream::Write<storage::Shared> for Writer<'_, F, C, E>
where
    F: rules::Policy,
    C: session::Storage,
    E: event::Evidence,
{
    type Block = conn::DataBlock;
    type Error = conn::Error;

    fn headers<S: field::Source>(
        &mut self,
        source: S,
        boundary: stream::Boundary,
    ) -> Result<stream::Control<Self::Block>, Self::Error> {
        match self
            .connection
            .streams()
            .headers()
            .try_send(self.key, source, boundary)
        {
            Ok(ControlFlow::Continue(())) => Ok(stream::Control::Written),
            Ok(ControlFlow::Break(_)) => Ok(stream::Control::Prepared(conn::DataBlock::Egress)),
            Err(error) if error.is_pressure() => {
                Ok(stream::Control::Blocked(conn::DataBlock::Egress))
            }
            Err(error) => Err(error),
        }
    }

    fn data(
        &mut self,
        prefix: &[u8],
        payload: &mut storage::Shared,
        offset: usize,
        boundary: stream::Boundary,
    ) -> Result<stream::Data<Self::Block>, Self::Error> {
        let head_len = prefix.len();
        let data_len = payload.len();
        let (prefix, body) = if offset < prefix.len() {
            (&prefix[offset..], 0)
        } else {
            (&[][..], offset - prefix.len())
        };
        if prefix.is_empty() && body >= payload.len() {
            return Err(conn::Error::DataRange {
                offset,
                prefix: head_len,
                body: data_len,
            });
        }
        let outcome = self
            .connection
            .streams()
            .retained()
            .try_parts(self.key, prefix, payload, body, boundary)?;
        match outcome {
            conn::DataOutcome::Sent(written) => Ok(stream::Data::Written(written)),
            conn::DataOutcome::Ended => Err(conn::Error::DataRange {
                offset,
                prefix: head_len,
                body: data_len,
            }),
            conn::DataOutcome::Blocked(block) => Ok(stream::Data::Blocked(block)),
        }
    }

    fn commit(&mut self) -> Result<ControlFlow<Self::Block>, Self::Error> {
        Ok(
            match self.connection.streams().headers().try_commit(self.key)? {
                ControlFlow::Continue(()) => ControlFlow::Continue(()),
                ControlFlow::Break(_) => ControlFlow::Break(conn::DataBlock::Egress),
            },
        )
    }

    fn discard(&mut self) {
        self.connection.streams().headers().discard(self.key);
    }
}
