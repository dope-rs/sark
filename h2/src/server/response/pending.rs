use core::mem;
use std::convert::identity;

use o3::{
    buffer,
    buffer::{Residency as _, hold, resident},
};
use sark_core::{
    life::{self, mode, owners},
    manifold::{schedule, stream::Boundary},
};
use sark_http::{
    field::metadata::section::rules,
    owner::{self, cause, evidence},
    payload::owned::{self, iter},
};

use crate::{
    conn::{self, event, machine, machine::streams, outputs::payloads},
    stream,
};

struct State<'d> {
    current: resident::Shared<'d>,
    segments: iter::Segments<'d>,
}

impl<'d> State<'d> {
    fn next(body: Option<resident::Shared<'d>>, mut segments: iter::Segments<'d>) -> Option<Self> {
        let current = match body {
            Some(body) => body,
            None => segments.next()?.into(),
        };
        Some(Self { current, segments })
    }

    fn send<F, E, K, M, G>(
        pending: &mut Option<Self>,
        conn: &mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
        stream: stream::Key,
        owner: &mut owners::Sink<'_, K, M, G>,
    ) -> Result<Option<conn::DataOutcome>, conn::Error>
    where
        F: rules::Policy,
        E: event::Evidence,
        K: Copy,
        M: life::Mode<K, stream::Key>,
        G: FnMut(stream::Key) -> stream::Key,
    {
        let Some(Self { current, segments }) = pending.take() else {
            return Ok(None);
        };
        let before = if M::ENABLED {
            hold::Charge::from_owner(current.len(), current.resident())
        } else {
            hold::Charge::default()
        };
        let boundary = if segments.is_empty() {
            Boundary::End
        } else {
            Boundary::More
        };
        let cap = payloads::Ledger::<resident::Shared<'d>>::MAX_MATERIALIZED_TAIL;
        if current.len() <= cap && segments.byte_len() <= cap - current.len() {
            let outcome = match conn.streams().try_to(
                stream,
                current.as_ref(),
                &[],
                boundary,
                streams::Limit::QUANTUM,
            ) {
                Ok(outcome) => outcome,
                Err(error) => {
                    *pending = Some(Self { current, segments });
                    return Err(error);
                }
            };
            let body = match outcome {
                conn::DataOutcome::Sent(written) => {
                    buffer::raw::Segment::suffix(current, written.get())
                }
                conn::DataOutcome::Ended => None,
                conn::DataOutcome::Blocked(_) => Some(current),
            };
            Self::buffered(owner, stream, before, &body, outcome);
            *pending = Self::next(body, segments);
            return Ok(Some(outcome));
        }
        let (outcome, body) = match conn.streams().retained().try_to(
            stream,
            current,
            boundary,
            streams::Limit::QUANTUM,
        ) {
            Ok(result) => result,
            Err((error, current)) => {
                *pending = Some(Self { current, segments });
                return Err(error);
            }
        };
        Self::retained(owner, stream, before, &body, outcome);
        *pending = Self::next(body, segments);
        Ok(Some(outcome))
    }

    fn buffered<K, M, G>(
        owner: &mut owners::Sink<'_, K, M, G>,
        stream: stream::Key,
        before: hold::Charge,
        body: &Option<resident::Shared<'d>>,
        outcome: conn::DataOutcome,
    ) where
        K: Copy,
        M: life::Mode<K, stream::Key>,
        G: FnMut(stream::Key) -> stream::Key,
    {
        let conn::DataOutcome::Sent(written) = outcome else {
            return;
        };
        if !M::ENABLED {
            return;
        }
        let written = written.get();
        let freed = if before.is_empty() {
            hold::Charge::default()
        } else if body.is_some() {
            hold::Charge::from_commit(written, 0, 0)
        } else {
            before
        };
        if !freed.is_empty() {
            owner.apply(owner::Effect::new(
                stream,
                freed,
                owner::Change::StagingReleased(cause::Staging::Buffered),
            ));
        }
        owner.copied(
            stream,
            evidence::Copy {
                read: written,
                written,
                cause: evidence::Cause::Framed,
            },
        );
    }

    fn retained<K, M, G>(
        owner: &mut owners::Sink<'_, K, M, G>,
        stream: stream::Key,
        before: hold::Charge,
        body: &Option<resident::Shared<'d>>,
        outcome: conn::DataOutcome,
    ) where
        K: Copy,
        M: life::Mode<K, stream::Key>,
        G: FnMut(stream::Key) -> stream::Key,
    {
        if M::ENABLED
            && matches!(outcome, conn::DataOutcome::Sent(_))
            && body.is_some()
            && before.resident_bytes != 0
        {
            owner.apply(owner::Effect::new(
                stream,
                hold::Charge::from_split(before.resident_bytes),
                owner::Change::StagingAcquired(cause::Acquire::Split),
            ));
        }
    }
}

#[doc(hidden)]
pub struct Pending<'d> {
    state: Option<State<'d>>,
}

impl<'d> Pending<'d> {
    pub(super) fn new(body: owned::Segments<'d>) -> Option<Self> {
        State::next(None, body.into_iter()).map(|state| Self { state: Some(state) })
    }

    pub(super) fn single(body: resident::Shared<'d>) -> Option<Self> {
        if body.is_empty() {
            return None;
        }
        Some(Self {
            state: Some(State {
                current: body,
                segments: owned::Segments::new().into_iter(),
            }),
        })
    }

    #[doc(hidden)]
    pub fn retention(&self) -> hold::Charge {
        match &self.state {
            Some(state) => {
                let current =
                    hold::Charge::from_owner(state.current.len(), state.current.resident());
                let tail = state.segments.charge();
                hold::Charge::from_commit(
                    current.logical_bytes.wrapping_add(tail.logical_bytes),
                    current.resident_bytes.wrapping_add(tail.resident_bytes),
                    current.owners.wrapping_add(tail.owners),
                )
            }
            None => hold::Charge::default(),
        }
    }

    #[doc(hidden)]
    pub fn turn<F: rules::Policy, E: event::Evidence>(
        &mut self,
        conn: &mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
        stream: stream::Key,
    ) -> Result<schedule::Turn<conn::DataBlock>, conn::Error> {
        let mut mode = mode::Disabled;
        let mut owner = owners::Sink::new(&mut mode, (), identity);
        self.turn_seen(conn, stream, &mut owner)
    }

    #[doc(hidden)]
    pub fn turn_seen<F, E, K, M, G>(
        &mut self,
        conn: &mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
        stream: stream::Key,
        owner: &mut owners::Sink<'_, K, M, G>,
    ) -> Result<schedule::Turn<conn::DataBlock>, conn::Error>
    where
        F: rules::Policy,
        E: event::Evidence,
        K: Copy,
        M: life::Mode<K, stream::Key>,
        G: FnMut(stream::Key) -> stream::Key,
    {
        let Some(outcome) = State::send(&mut self.state, conn, stream, owner)? else {
            return Ok(schedule::Turn::Idle);
        };
        let more = self.state.is_some();
        Ok(match outcome {
            conn::DataOutcome::Sent(_) | conn::DataOutcome::Ended if !more => {
                schedule::Turn::Complete
            }
            conn::DataOutcome::Sent(_) | conn::DataOutcome::Ended => schedule::Turn::Progress,
            conn::DataOutcome::Blocked(block) => schedule::Turn::Blocked(block),
        })
    }
}

const _: () =
    assert!(mem::size_of::<Pending<'static>>() == mem::size_of::<Option<State<'static>>>());
