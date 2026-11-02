use core::mem::size_of;
use std::num::NonZero;

use dope_quic::wire::stream::Id;
use sark_http::{self, field::metadata::section::rules};

use crate::{
    conn::{
        self, event,
        io::{
            Resume,
            ingress::{
                Receive,
                stream::{drive, table},
            },
            write::feedback::{Feedback, FeedbackMut},
        },
    },
    qpack::stream,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::conn) enum Progress {
    Idle,
    Moved,
}

#[must_use = "discarded receive credit must be reconciled or deferred"]
pub(crate) struct Discard {
    token: Option<event::Token>,
    bytes: usize,
}

impl Discard {
    pub(crate) fn into_parts(self) -> (Option<event::Token>, usize) {
        (self.token, self.bytes)
    }
}

#[repr(transparent)]
pub(crate) struct Credit<
    'a,
    'd,
    P: rules::Policy = rules::RetainRaw,
    A: rules::Push = rules::RejectPush,
> {
    conn: &'a mut conn::Connection<'d, P, A>,
}

impl<'a, 'd, P: rules::Policy, A: rules::Push> Credit<'a, 'd, P, A> {
    pub(crate) const fn new(conn: &'a mut conn::Connection<'d, P, A>) -> Self {
        Self { conn }
    }
}

impl<P: rules::Policy, A: rules::Push> Credit<'_, '_, P, A> {
    pub(crate) fn hold(self) {
        self.conn.ingress.data_credit = conn::CreditMode::Held;
    }

    pub(crate) fn is_held(&self) -> bool {
        self.conn.ingress.data_credit == conn::CreditMode::Held
    }

    pub(crate) fn delivered(self, id: Id) {
        self.conn.streams.deliver_data(id);
    }

    pub(crate) fn release(self, bytes: usize) -> Result<(), conn::Error> {
        if bytes == 0 {
            return Ok(());
        }
        if self.conn.ingress.data_credit != conn::CreditMode::Held {
            return Err(conn::Error::CreditMode { bytes });
        }
        self.conn.release_rx(bytes)
    }

    /// Resumes one DATA barrier and queues the established transport pull.
    pub(crate) fn resume(self, id: Id) -> Result<(), conn::Error> {
        match self.conn.streams.release_data(id) {
            table::Release::Released => {}
            table::Release::Missing => return Ok(()),
            table::Release::NotHeld => {
                return Err(conn::Error::CreditOpen {
                    stream_id: id.get(),
                });
            }
        }
        if self.conn.queues.receive_credits.is_full() {
            return Receive::new(self.conn).defer(id);
        }
        drive(self.conn, id, Resume::Ready)?;
        Ok(())
    }

    pub(crate) fn discard(self, id: Id) -> Result<Discard, conn::Error> {
        self.clear(id, false)
    }

    pub(crate) fn reject(self, id: Id, open: Option<event::Token>) -> Result<Discard, conn::Error> {
        if let Some(token) = open
            && self.conn.streams.token(id).is_none()
        {
            self.conn
                .streams
                .receive(id, &self.conn.ingress.segments, Some(token))?;
        }
        let discarded = Credit::new(self.conn).clear(id, true)?;
        if self.conn.streams.rejected(id) {
            Receive::new(self.conn).defer(id)?;
        }
        Ok(discarded)
    }

    fn clear(self, id: Id, retain: bool) -> Result<Discard, conn::Error> {
        if self.conn.streams.is_critical(id) {
            return Err(conn::Error::ClosedCriticalStream(id.get()));
        }
        let cancel = if self.conn.settings.local_settings.table().get() != 0 {
            let limit = Feedback::new(
                &self.conn.qpack.feedback,
                &self.conn.session,
                &self.conn.queues,
            )
            .limit();
            let feedback = FeedbackMut::new(
                &mut self.conn.qpack.feedback,
                &mut self.conn.queues.write_budget,
            )
            .try_cancel(id.get(), limit)?;
            Some(feedback)
        } else {
            None
        };
        let blocked = self.conn.qpack.blocked.removal(id);
        let (token, mut bytes) = match self.conn.streams.clear(
            id,
            &mut self.conn.ingress.budget,
            &mut self.conn.queues.waiters,
            retain,
        )? {
            Some((token, bytes)) => (Some(token), bytes),
            None => (None, 0),
        };
        // Parser receipts already released the HTTP/3 budget; retire their
        // transport credit and queued resume together with this stream.
        for _ in 0..self.conn.queues.receive_credits.len() {
            let Some(front) = self.conn.queues.receive_credits.front_entry() else {
                break;
            };
            let (receipt, vacant) = front.take();
            if receipt.0.id() != id {
                vacant.push_back(receipt);
                continue;
            }
            bytes = conn::try_add_credit(bytes, receipt.1)?;
        }
        if let Some(feedback) = cancel {
            feedback.commit();
        }
        if let Some(remove) = blocked {
            remove.commit();
        }
        Ok(Discard { token, bytes })
    }

    pub(crate) fn discard_for(
        self,
        token: event::Token,
        bytes: usize,
    ) -> Result<(usize, usize), conn::Error> {
        let (queued, released) = conn::Discard::new(&mut self.conn.queues.events).try_for(token)?;
        Ok((queued, conn::try_add_credit(bytes, released)?))
    }
}

const _: () = assert!(size_of::<Credit<'static, 'static>>() == size_of::<usize>());

#[repr(transparent)]
pub(crate) struct Status<
    'a,
    'd,
    P: rules::Policy = rules::RetainRaw,
    A: rules::Push = rules::RejectPush,
> {
    conn: &'a conn::Connection<'d, P, A>,
}

impl<'a, 'd, P: rules::Policy, A: rules::Push> Status<'a, 'd, P, A> {
    pub(crate) const fn new(conn: &'a conn::Connection<'d, P, A>) -> Self {
        Self { conn }
    }
}

impl<P: rules::Policy, A: rules::Push> Status<'_, '_, P, A> {
    pub(crate) fn cancelled(&self, id: Option<Id>) -> bool {
        self.conn.queues.cancellations != 0
            && id.is_none_or(|id| {
                (&self.conn.queues.events).into_iter().any(|event| {
                    matches!(event, event::Event::QpackCancelled { stream_id } if id == *stream_id)
                })
            })
    }

    /// Whether this stream still owns an outstanding DATA delivery.
    pub(crate) fn data_held(&self, id: Id) -> bool {
        self.conn.streams.data_held(id)
    }

    pub(crate) fn rejected(&self, id: Id) -> bool {
        self.conn.streams.rejected(id)
    }

    pub(crate) fn retains(&self, local: dope_quic::conn::stream::resident::Local) -> bool {
        self.conn.streams.token(local.id()).is_some()
            || (&self.conn.queues.events).into_iter().any(|event| {
                !event.is_detached()
                    && event
                        .stream_id()
                        .is_some_and(|token| token.raw() == local.key().raw().get())
            })
    }

    pub(crate) fn has_work(self) -> bool {
        let count = self.conn.qpack.qpack_decoder.insert_count();
        !self.conn.queues.events.is_empty()
            || self.conn.queues.waiters.has_ready()
            || !self.conn.queues.receive_credits.is_empty()
            || self.conn.qpack.blocked.has_ready(count)
            || self.feedback().is_some()
    }

    /// Returns the head feedback waiter if its instruction fits.
    fn feedback(self) -> Option<Id> {
        let id = self.conn.queues.waiters.feedback()?;
        let cmd = if self.conn.streams.is_encoder(id) {
            stream::Instruction::InsertCountIncrement {
                increment: NonZero::<u64>::MIN,
            }
        } else {
            stream::Instruction::SectionAcknowledgment {
                stream_id: id.get(),
            }
        };
        Feedback::new(
            &self.conn.qpack.feedback,
            &self.conn.session,
            &self.conn.queues,
        )
        .can_prepare(cmd)
        .then_some(id)
    }
}

const _: () = assert!(size_of::<Status<'static, 'static>>() == size_of::<usize>());

pub(in crate::conn::io) struct Schedule<'connection, 'd, P: rules::Policy, A: rules::Push> {
    conn: &'connection mut conn::Connection<'d, P, A>,
}

impl<'connection, 'd, P: rules::Policy, A: rules::Push> Schedule<'connection, 'd, P, A> {
    pub(in crate::conn::io) const fn new(
        conn: &'connection mut conn::Connection<'d, P, A>,
    ) -> Self {
        Self { conn }
    }

    fn retry(&mut self) -> Result<(), conn::Error> {
        let count = self.conn.qpack.qpack_decoder.insert_count();
        let Some(id) = self.conn.qpack.blocked.pop_ready(count) else {
            return Ok(());
        };
        let recv = self
            .conn
            .streams
            .recv_mut(id)
            .ok_or(conn::Error::StreamMissing {
                stream_id: id.get(),
            })?;
        self.conn
            .queues
            .waiters
            .push_ready(id, recv)
            .map_err(conn::Error::Wait)?;
        Ok(())
    }

    fn promote(&mut self) -> Result<(), conn::Error> {
        let Some(id) = Status::new(self.conn).feedback() else {
            return Ok(());
        };
        let recv = self
            .conn
            .streams
            .recv_mut(id)
            .ok_or(conn::Error::StreamMissing {
                stream_id: id.get(),
            })?;
        self.conn
            .queues
            .waiters
            .promote(id, recv)
            .map_err(conn::Error::Wait)?;
        Ok(())
    }

    pub(in crate::conn::io) fn step(&mut self) -> Result<Progress, conn::Error> {
        if self.conn.queues.events.is_full() || self.conn.queues.receive_credits.is_full() {
            return Ok(Progress::Idle);
        }
        self.promote()?;
        self.retry()?;
        let Some(id) = self.conn.queues.waiters.pop_ready(&mut self.conn.streams)? else {
            return Ok(Progress::Idle);
        };
        drive(self.conn, id, Resume::Ready)?;
        Ok(Progress::Moved)
    }
}
