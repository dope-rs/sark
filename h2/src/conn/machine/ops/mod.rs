use o3::buffer::{pool, storage};
use sark_core::{
    life::session,
    manifold::{client::io, schedule},
};
use sark_http::field::metadata::section::rules;

use crate::{
    ClientRole, client,
    conn::{self, event, machine, machine::Bound, outputs::payloads},
};

mod control;
mod drain;
mod ingress;
pub(crate) mod state;

pub use control::Control;
pub use drain::Drain;
pub use ingress::Ingress;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Opened {
    Ready,
    Idle,
}

const _: () = {
    assert!(core::mem::size_of::<Opened>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Opened>() == core::mem::align_of::<bool>());
};

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub(in crate::conn::machine) enum Shutdown {
    Immediate,
    Drain,
}

const _: () = {
    assert!(core::mem::size_of::<Shutdown>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Shutdown>() == core::mem::align_of::<bool>());
};

#[repr(transparent)]
pub struct Flow<
    'a,
    R: crate::Role,
    F: rules::Policy = rules::RetainRaw,
    C: session::Storage = storage::Shared,
    A: rules::Push = rules::RejectPush,
    E: event::Evidence = event::Retirement,
> {
    pub(in crate::conn::machine) conn: &'a mut machine::Connection<R, F, C, A, E>,
}

#[repr(transparent)]
pub struct Transfer<
    'a,
    R: crate::Role,
    F: rules::Policy = rules::RetainRaw,
    C: session::Storage = storage::Shared,
    A: rules::Push = rules::RejectPush,
    E: event::Evidence = event::Retirement,
> {
    pub(in crate::conn::machine) conn: &'a mut machine::Connection<R, F, C, A, E>,
}

impl<R, F, C, A, E> Flow<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub fn poll_wake(&mut self) -> Option<schedule::Wake<crate::stream::Key>> {
        self.conn.state.poll_wake()
    }
}

impl<'a, R, F, C, A, E> Transfer<'a, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage + 'a,
    A: rules::Push,
    E: event::Evidence,
{
    pub(in crate::conn::machine) fn new(conn: &'a mut machine::Connection<R, F, C, A, E>) -> Self {
        Self { conn }
    }

    pub fn drain(&mut self, n: usize) {
        self.conn.egress.outbound.transfer().drain(n);
        self.flush_resets();
    }

    pub fn drain_into(&mut self, buf: &mut [u8]) -> usize {
        let written = {
            let (first, second) = self.conn.output().slices();
            let left = first.len().min(buf.len());
            buf[..left].copy_from_slice(&first[..left]);
            let right = second.len().min(buf.len() - left);
            buf[left..left + right].copy_from_slice(&second[..right]);
            left + right
        };
        self.drain(written);
        written
    }

    pub fn discard(&mut self, mut release: impl FnMut(event::Retirement)) {
        if let Some(key) = self.conn.egress.headers.pending() {
            self.conn.egress.headers.discard(key);
        }
        let state = &mut self.conn.state;
        self.conn
            .egress
            .outbound
            .retained()
            .clear(|write| release(state.cancel_write(write)));
    }

    pub(crate) fn has_write(&mut self) -> bool {
        self.flush_resets();
        !self.conn.output().is_empty()
    }

    pub(crate) fn bind_link<const PREFIX: usize>(
        self,
    ) -> Result<Option<Bound<'a, R, C, PREFIX>>, io::Error> {
        let conn = self.conn;
        let Some(write) = conn.egress.outbound.transfer().bind_link()? else {
            return Ok(None);
        };
        Ok(Some(Bound {
            state: &mut conn.state,
            write,
        }))
    }

    pub fn take_write(
        &mut self,
        buf: &mut [u8],
    ) -> Result<event::Write<event::Retirement, C>, conn::Error> {
        self.flush_resets();
        let write = self
            .conn
            .egress
            .outbound
            .transfer()
            .take_write(buf)
            .map_err(conn::Error::Output)?;
        Ok(self.finish_with(write, event::Retirement::from_body))
    }

    /// Takes a write with zero-sized retained-owner evidence.
    /// Disabled lifecycle adapters compile its metadata path out.
    #[doc(hidden)]
    pub fn take_plain(&mut self, buf: &mut [u8]) -> Result<event::Write<(), C>, io::Error> {
        self.flush_resets();
        let write = self.conn.egress.outbound.transfer().take_write(buf)?;
        Ok(self.finish_with(write, |_, _| ()))
    }

    fn finish_with<T>(
        &mut self,
        write: payloads::Write<C>,
        evidence: impl FnOnce(crate::stream::Key, &C) -> T,
    ) -> event::Write<T, C> {
        let write = self
            .conn
            .state
            .finish_write(write, |stream, _, body| evidence(stream, body));
        self.flush_resets();
        write
    }

    fn flush_resets(&mut self) {
        if self.conn.state.active_streams.reset_len() == 0 {
            return;
        }
        self.conn.state.flush_resets(&mut self.conn.egress.outbound);
    }
}

impl<'a, F, C> Transfer<'a, ClientRole, F, C>
where
    F: rules::Policy,
    C: session::Storage + 'a,
{
    /// Borrows the exact client output head across final-link admission.
    pub fn prepare(mut self, prefix: pool::Cursor) -> Result<client::Prepared<'a, C>, io::Reject> {
        self.flush_resets();
        let conn = self.conn;
        let write = conn.egress.outbound.transfer().bind_write(prefix)?;
        Ok(client::Prepared::new(&mut conn.state, write))
    }
}

const _: () = {
    use core::mem::{align_of, size_of};

    assert!(size_of::<Flow<'static, crate::ClientRole>>() == size_of::<usize>());
    assert!(align_of::<Flow<'static, crate::ClientRole>>() == align_of::<usize>());
    assert!(size_of::<Transfer<'static, crate::ClientRole>>() == size_of::<usize>());
    assert!(align_of::<Transfer<'static, crate::ClientRole>>() == align_of::<usize>());
};
