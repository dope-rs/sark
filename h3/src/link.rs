//! Typed transport ownership boundary.

use std::num::NonZero;

use dope_quic::wire::stream::Id;
use o3::buffer::resident;
use sark_core::manifold::stream::Boundary;
use sark_http::{self, field::metadata::section::rules};

use crate::conn::{
    self, event,
    io::{self, write},
    output,
};

const RECEIVE_QUANTUM: usize = 32;
const CREDIT_QUANTUM: usize = 32;
const WRITE_QUANTUM: u8 = 32;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Flush {
    Drained,
    Pending,
    Blocked,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Open,
    Suspended,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Drain {
    No,
    Yes,
}

/// Ownership result of one retained receive handoff.
#[must_use]
#[derive(PartialEq, Eq)]
pub enum Handoff {
    Rejected(conn::Error),
    Accepted(Result<Status, conn::Error>),
}

impl Handoff {
    pub(crate) fn into_result(self) -> Result<(), conn::Error> {
        match self {
            Self::Rejected(error) | Self::Accepted(Err(error)) => Err(error),
            Self::Accepted(Ok(_)) => Ok(()),
        }
    }
}

/// One atomic receive-side transport handoff.
#[must_use]
pub enum Receive<'d, C> {
    Pending,
    Segment {
        bytes: resident::Shared<'d>,
        boundary: Boundary,
        credit: C,
    },
    Finished,
}

pub enum Outcome {
    Accepted,
    Blocked,
}

/// Drains one bounded turn while preserving each admitted write's exact brand.
pub(crate) fn pump<'d, P: rules::Policy, A: rules::Push, E: From<conn::Error>>(
    conn: &mut conn::Connection<'d, P, A>,
    mut send: impl for<'id, 'q> FnMut(
        write::Flight<'id, 'd>,
        write::Prepared<'id, 'q, 'd>,
    ) -> Result<Option<write::Completion>, E>,
) -> Result<(Flush, bool), E> {
    if io::ingress::schedule::Status::new(conn).cancelled(None) {
        return Ok((Flush::Blocked, false));
    }
    let mut released = false;
    for _ in 0..WRITE_QUANTUM {
        if let Err(error) = conn.publish_encoder()
            && !conn.queues.is_pressure(&error)
        {
            return Err(error.into());
        }
        let feedback = conn.session.qpack_decoder;
        let Some(taken) = conn.queues.take(&mut conn.qpack.feedback, feedback) else {
            return Ok((Flush::Drained, released));
        };
        let Some(done) = taken.with(&mut send)? else {
            return Ok((Flush::Blocked, released));
        };
        match done {
            write::Completion::Queued => released = true,
            write::Completion::Feedback => {
                conn.events().resume()?;
                if io::ingress::schedule::Status::new(conn).cancelled(None) {
                    return Ok((Flush::Blocked, released));
                }
            }
        }
    }
    let flush = if conn.queues.has_write(&conn.qpack.feedback) || conn.qpack.has_pending_encoder() {
        Flush::Pending
    } else {
        Flush::Drained
    };
    Ok((flush, released))
}

pub trait Transport<'d> {
    /// Attempts one indivisible write.
    fn send_write(&mut self, write: &output::Write<'d>) -> Outcome;

    fn pump_writes<P: rules::Policy, A: rules::Push>(
        &mut self,
        conn: &mut conn::Connection<'d, P, A>,
    ) -> Result<Flush, conn::Error> {
        pump(conn, |write, prepared| {
            Ok(match self.send_write(write.get()) {
                Outcome::Accepted => {
                    drop(write);
                    Some(prepared.commit())
                }
                Outcome::Blocked => {
                    prepared.restore(write);
                    None
                }
            })
        })
        .map(|(flush, _)| flush)
    }
}

pub trait Recv<'d> {
    type Credit;
    type Error: From<conn::Error>;

    /// Transfers one exact owner with its FIN fact.
    fn recv_stream(&mut self, id: Id) -> Result<Receive<'d, Self::Credit>, Self::Error>;

    /// Commits one successful owner handoff.
    fn accept(&mut self, credit: Self::Credit) -> Result<(), Self::Error>;

    /// Rolls back one failed owner handoff.
    fn reject(&mut self, credit: Self::Credit);

    /// Releases one non-empty byte credit; scheduling resumes bypass ownership.
    fn release(&mut self, id: Id, bytes: NonZero<usize>) -> Result<(), Self::Error>;
}

/// Exclusive receive pump over one transport and its H3 connection.
#[repr(C)]
pub struct Pump<
    'a,
    'd,
    R: Recv<'d>,
    P: rules::Policy = rules::RetainRaw,
    A: rules::Push = rules::RejectPush,
> {
    recv: &'a mut R,
    conn: &'a mut conn::Connection<'d, P, A>,
}

impl<'a, 'd, R, P, A> Pump<'a, 'd, R, P, A>
where
    R: Recv<'d>,
    P: rules::Policy,
    A: rules::Push,
{
    pub const fn new(recv: &'a mut R, conn: &'a mut conn::Connection<'d, P, A>) -> Self {
        Self { recv, conn }
    }

    pub fn event(mut self, id: Id) -> Result<(), R::Error> {
        let token = event::Token::from(id);
        self.pump(id, Some(token), Drain::Yes)
    }

    pub fn token(mut self, id: Id, token: u64) -> Result<(), R::Error> {
        let token = event::Token::new(token).ok_or(conn::Error::Identity { raw: token })?;
        self.pump(id, Some(token), Drain::Yes)
    }

    pub fn credit(&mut self) -> Result<(), R::Error> {
        for _ in 0..CREDIT_QUANTUM {
            let Some((id, bytes, resume)) = self.conn.events().poll_credit()? else {
                return Ok(());
            };
            if let Some(bytes) = NonZero::<usize>::new(bytes) {
                self.recv.release(id, bytes)?;
            }
            if matches!(resume, io::Resume::Ready) {
                self.pump(id, None, Drain::No)?;
            }
        }
        Ok(())
    }

    fn pump(&mut self, id: Id, token: Option<event::Token>, drain: Drain) -> Result<(), R::Error> {
        if io::Receive::new(self.conn).is_blocked(id) {
            return Ok(());
        }
        for _ in 0..RECEIVE_QUANTUM {
            let (bytes, boundary, credit) = match self.recv.recv_stream(id)? {
                Receive::Pending => return Ok(()),
                Receive::Finished => {
                    io::Receive::new(self.conn).finish(id, token)?;
                    if matches!(drain, Drain::Yes) {
                        self.credit()?;
                    }
                    return Ok(());
                }
                Receive::Segment {
                    bytes,
                    boundary,
                    credit,
                } => (bytes, boundary, credit),
            };
            let state = match io::Receive::new(self.conn).hold_with(id, token, bytes, boundary) {
                Handoff::Rejected(error) => {
                    self.recv.reject(credit);
                    return Err(error.into());
                }
                Handoff::Accepted(state) => {
                    self.recv.accept(credit)?;
                    state?
                }
            };
            if matches!(drain, Drain::Yes) {
                self.credit()?;
            }
            if state == Status::Suspended {
                return Ok(());
            }
            if matches!(boundary, Boundary::End) {
                return Ok(());
            }
        }
        Ok(io::Receive::new(self.conn).defer(id)?)
    }
}
