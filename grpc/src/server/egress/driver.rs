use o3::{base::cell::region, buffer::storage};
use sark_core::{
    life::session,
    manifold::{client::event::Reset, schedule, server},
};

use crate::server::{call, calls, egress};

pub(in crate::server) struct Driver<'a, S: server::Connection, F, R> {
    sink: &'a mut F,
    connection: &'a mut S,
    store: &'a mut egress::Store<S::Key>,
    retire: &'a mut R,
    pub(in crate::server) released: egress::Release,
}

impl<'a, S, F, R> Driver<'a, S, F, R>
where
    F: FnMut(S::Key, egress::Error<S::Error>),
    S: server::Connection<Output = storage::Shared>,
{
    pub(in crate::server) fn new(
        conn: &'a mut S,
        store: &'a mut egress::Store<S::Key>,
        sink: &'a mut F,
        retire: &'a mut R,
    ) -> Self {
        Self {
            sink,
            connection: conn,
            store,
            retire,
            released: egress::Release::default(),
        }
    }

    fn remove(&mut self, stream: S::Key) -> Result<(), egress::Fault> {
        let released = self.store.remove(stream, self.connection)?;
        self.released.prepared |= released.prepared;
        self.released.output |= released.output;
        Ok(())
    }

    fn terminal(&mut self, stream: S::Key, error: egress::Error<S::Error>) {
        match &error {
            egress::Error::Fault(
                egress::Fault::Lane(_) | egress::Fault::Accounting | egress::Fault::Credit(_),
            ) => {
                self.connection.fail();
            }
            egress::Error::Write(..)
            | egress::Error::Fault(egress::Fault::Protocol | egress::Fault::Response(_)) => {
                self.connection.reset(stream, Reset::Internal)
            }
        }
        (self.sink)(stream, error);
    }
}

impl<'r, T, C, S, F, R> schedule::Settle<call::State<'r, T, C>> for Driver<'_, S, F, R>
where
    F: FnMut(S::Key, egress::Error<S::Error>),
    C: session::Storage,
    S: server::Connection<Owner = C, Output = storage::Shared>,
    R: FnMut(&mut call::State<'r, T, C>) -> Result<(), calls::Error>,
{
    type Key = S::Key;
    type Error = egress::Error<S::Error>;

    fn settle(
        &mut self,
        stream: S::Key,
        mut call: call::State<'r, T, C>,
        terminal: schedule::Terminal<Self::Error>,
    ) -> schedule::Settlement<call::State<'r, T, C>> {
        let failed = matches!(terminal, schedule::Terminal::Failed(_));
        let pending = match terminal {
            schedule::Terminal::Idle => self.store.try_pending(stream),
            schedule::Terminal::Complete => self.remove(stream).map(|()| false),
            schedule::Terminal::Failed(error) => {
                let pending = self.remove(stream).map(|()| false);
                self.terminal(stream, error);
                pending
            }
        };
        match pending {
            Ok(pending) if !failed && (pending || !call.is_done()) => {
                return schedule::Settlement::Keep(call);
            }
            Ok(_) => {}
            Err(fault) => {
                self.connection.fail();
                self.terminal(stream, fault.into());
                return schedule::Settlement::Keep(call);
            }
        }
        match (self.retire)(&mut call) {
            Ok(()) => schedule::Settlement::Remove,
            Err(error) => {
                self.connection.fail();
                self.terminal(stream, egress::Fault::Response(error.status()).into());
                schedule::Settlement::Keep(call)
            }
        }
    }
}

impl<'r, T, C, S, F, R> schedule::Driver<call::State<'r, T, C>> for Driver<'_, S, F, R>
where
    F: FnMut(S::Key, egress::Error<S::Error>),
    C: session::Storage,
    S: server::Connection<Owner = C, Output = storage::Shared>,
    R: FnMut(&mut call::State<'r, T, C>) -> Result<(), calls::Error>,
{
    type Block = S::Block;
    type Permit = ();

    fn permit(&mut self) -> Option<Self::Permit> {
        Some(())
    }

    fn control<'id>(
        &mut self,
        (): Self::Permit,
        entry: schedule::Occupied<
            '_,
            Self::Key,
            call::State<'r, T, C>,
            Self::Block,
            region::Proof<region::Identity<'id>>,
        >,
    ) -> schedule::Decision<'id, Self::Key> {
        entry.borrowed(self, |driver, key, _| {
            driver.store.control(
                key,
                || driver.connection.writer(key),
                &mut driver.released.prepared,
            )
        })
    }

    fn data(
        &mut self,
        (): Self::Permit,
        key: S::Key,
        _: &mut call::State<'r, T, C>,
    ) -> Result<schedule::DataTurn<Self::Block>, Self::Error> {
        self.store.data(
            key,
            || self.connection.writer(key),
            &mut self.released.output,
        )
    }
}
