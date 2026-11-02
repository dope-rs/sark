use dope_quic::{errors, wire::stream::Id};
use o3::buffer::{hold, resident};
use sark_core::manifold::stream::Boundary;
use sark_http::field::metadata::section::rules;

mod buf;
mod recv;

use ingress::schedule::Credit;
pub(in crate::conn) use recv::Recv;

use crate::{
    conn::{
        self, event,
        io::{
            Resume, ingress,
            ingress::stream::{self, table},
        },
    },
    link::Handoff,
};

fn is_blocked<P: rules::Policy, A: rules::Push>(
    state: &conn::Connection<'_, P, A>,
    id: Id,
) -> bool {
    state.qpack.blocked.contains(id) || state.streams.is_blocked(id)
}

fn append<'d, P, A, F>(
    state: &mut conn::Connection<'d, P, A>,
    id: Id,
    token: Option<event::Token>,
    boundary: Boundary,
    bytes: usize,
    credit: conn::CreditMode,
    append: F,
) -> Handoff
where
    P: rules::Policy,
    A: rules::Push,
    F: FnOnce(&mut Recv<'d>) -> Result<(), conn::Error>,
{
    if let Err(error) = state.ingress.budget.try_reserve(bytes) {
        return Handoff::Rejected(error);
    }
    let transfer = if is_blocked(state, id) {
        Handoff::Rejected(match credit {
            conn::CreditMode::Open => conn::Error::AppendBlocked {
                stream_id: id.get(),
            },
            conn::CreditMode::Held => conn::Error::HandoffBlocked {
                stream_id: id.get(),
            },
        })
    } else {
        let admitted = state
            .streams
            .receive(id, &state.ingress.segments, token)
            .and_then(|recv| {
                append(recv)?;
                recv.merge(boundary);
                Ok(())
            });
        match admitted {
            Err(error) => Handoff::Rejected(error),
            Ok(()) => Handoff::Accepted(stream::drive(state, id, Resume::Idle)),
        }
    };
    match transfer {
        Handoff::Rejected(error) => Handoff::Rejected(match state.release_rx(bytes) {
            Ok(()) => error,
            Err(error) => error,
        }),
        accepted => accepted,
    }
}

/// Exclusive capability for applying one H3 input transaction.
#[repr(transparent)]
pub struct Receive<'a, 'd, P: rules::Policy = rules::RetainRaw, A: rules::Push = rules::RejectPush>
{
    conn: &'a mut conn::Connection<'d, P, A>,
}

impl<'a, 'd, P: rules::Policy, A: rules::Push> Receive<'a, 'd, P, A> {
    pub const fn new(conn: &'a mut conn::Connection<'d, P, A>) -> Self {
        Self { conn }
    }
}

impl<'d, P, A> Receive<'_, 'd, P, A>
where
    P: rules::Policy,
    A: rules::Push,
{
    pub fn borrowed(self, id: Id, bytes: &[u8], boundary: Boundary) -> Result<(), conn::Error> {
        append(
            self.conn,
            id,
            Some(id.into()),
            boundary,
            bytes.len(),
            conn::CreditMode::Open,
            |state| state.append(bytes),
        )
        .into_result()
    }

    pub fn owned(self, id: Id, bytes: Vec<u8>, boundary: Boundary) -> Result<(), conn::Error> {
        append(
            self.conn,
            id,
            Some(id.into()),
            boundary,
            bytes.len(),
            conn::CreditMode::Open,
            |state| state.append_owned(bytes),
        )
        .into_result()
    }

    pub fn shared(
        self,
        id: Id,
        bytes: resident::Shared<'d>,
        boundary: Boundary,
    ) -> Result<(), conn::Error> {
        append(
            self.conn,
            id,
            Some(id.into()),
            boundary,
            bytes.len(),
            conn::CreditMode::Open,
            |state| state.append_pool(bytes),
        )
        .into_result()
    }

    /// Transfers ownership while withholding its stream credit until consumed.
    pub fn hold(self, id: Id, bytes: resident::Shared<'d>, boundary: Boundary) -> Handoff {
        self.hold_with(id, Some(id.into()), bytes, boundary)
    }

    pub(crate) fn hold_with(
        self,
        id: Id,
        token: Option<event::Token>,
        bytes: resident::Shared<'d>,
        boundary: Boundary,
    ) -> Handoff {
        append(
            self.conn,
            id,
            token,
            boundary,
            bytes.len(),
            conn::CreditMode::Held,
            |state| state.append_held(bytes),
        )
    }

    pub(crate) fn finish(self, id: Id, token: Option<event::Token>) -> Result<(), conn::Error> {
        let conn = self.conn;
        if is_blocked(conn, id) {
            return Err(conn::Error::FinishBlocked {
                stream_id: id.get(),
            });
        }
        let recv = conn.streams.receive(id, &conn.ingress.segments, token)?;
        recv.merge(Boundary::End);
        stream::drive(conn, id, Resume::Idle)?;
        Ok(())
    }

    pub fn is_blocked(self, id: Id) -> bool {
        is_blocked(self.conn, id)
    }

    pub(crate) fn defer(self, id: Id) -> Result<(), conn::Error> {
        let conn = self.conn;
        if is_blocked(conn, id) {
            return Err(conn::Error::DeferBlocked {
                stream_id: id.get(),
            });
        }
        let recv = conn
            .streams
            .recv_mut(id)
            .ok_or(conn::Error::StreamMissing {
                stream_id: id.get(),
            })?;
        conn.queues
            .waiters
            .push_ready(id, recv)
            .map_err(conn::Error::Wait)?;
        Ok(())
    }

    pub fn reset(
        self,
        id: Id,
        supplied: Option<event::Token>,
        code: errors::Code,
    ) -> Result<hold::Charge, (conn::Error, hold::Charge)> {
        let conn = self.conn;
        (|| {
            table::direction(id)?;
            if conn.session.is_critical(id) || conn.streams.is_critical(id) {
                return Err(conn::Error::ClosedCriticalStream(id.get()));
            }
            if let (Some(expected), Some(actual)) = (conn.streams.token(id), supplied)
                && actual != expected
            {
                return Err(conn::Error::TokenMismatch {
                    expected: expected.raw(),
                    actual: actual.raw(),
                });
            }
            if conn.queues.events.is_full() {
                return Err(conn::Error::EventCapacity {
                    len: conn.queues.events.len(),
                    capacity: conn.queues.events.capacity(),
                });
            }
            let (stored, mut released) = Credit::new(conn).discard(id)?.into_parts();
            let (token, origin) = match stored.or(supplied) {
                Some(token) => (token, event::Origin::Transport),
                None => (id.into(), event::Origin::Wire),
            };
            let (queued, prior) =
                conn::Discard::new(&mut conn.queues.events).try_input(token, origin)?;
            conn.ingress.budget.release(queued)?;
            released = conn::try_add_credit(released, prior)?;
            if conn.ingress.data_credit == conn::CreditMode::Held {
                released = conn::try_add_credit(released, queued)?;
            }
            conn.queues.try_event()?.push_back(event::Event::Reset {
                stream_id: token,
                error_code: code,
                origin,
                released,
            });
            Ok(())
        })()
        .map_err(|error| (error, hold::Charge::default()))?;
        conn.queues.discard(id)
    }

    pub fn stop(
        self,
        id: Id,
        supplied: Option<event::Token>,
        code: errors::Code,
    ) -> Result<hold::Charge, (conn::Error, hold::Charge)> {
        table::direction(id).map_err(|error| (error, hold::Charge::default()))?;
        let conn = self.conn;
        if conn.session.is_critical(id) || conn.streams.is_critical(id) {
            return Err((
                conn::Error::ClosedCriticalStream(id.get()),
                hold::Charge::default(),
            ));
        }
        if let (Some(expected), Some(actual)) = (conn.streams.token(id), supplied)
            && actual != expected
        {
            return Err((
                conn::Error::TokenMismatch {
                    expected: expected.raw(),
                    actual: actual.raw(),
                },
                hold::Charge::default(),
            ));
        }
        let (token, origin) = match supplied {
            Some(token) => (token, event::Origin::Transport),
            None => (id.into(), event::Origin::Wire),
        };
        conn.queues
            .try_event()
            .map_err(|error| (error, hold::Charge::default()))?
            .push_front(event::Event::Stopped {
                stream_id: token,
                error_code: code,
                origin,
            });
        conn.queues.discard(id)
    }
}

const _: () =
    assert!(core::mem::size_of::<Receive<'static, 'static>>() == core::mem::size_of::<usize>());
