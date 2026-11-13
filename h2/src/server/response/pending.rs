use core::mem;

use o3::buffer::{Residency as _, hold, resident, write::consume::Consume as _};
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
    server::response::raw::Inline,
    stream,
};

enum State<'d> {
    Retained {
        current: resident::Shared<'d>,
        segments: iter::Segments<'d>,
    },
    Inline(Inline),
}

impl<'d> State<'d> {
    fn send<F, E, K, M>(
        pending: &mut Option<Self>,
        conn: &mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
        stream: stream::Key,
        end: Boundary,
        owner: &mut owners::Sink<'_, K, M>,
    ) -> Result<Option<conn::DataOutcome>, conn::Error>
    where
        F: rules::Policy,
        E: event::Evidence,
        K: Copy,
        M: life::Mode<K, stream::Key>,
    {
        let Some(state) = pending.as_mut() else {
            return Ok(None);
        };
        let (boundary, before) = match state {
            Self::Retained { current, segments } => {
                let before = if M::ENABLED {
                    hold::Charge::from_owner(current.len(), current.resident())
                } else {
                    hold::Charge::default()
                };
                let boundary = if iter::Segments::is_empty(segments) {
                    end
                } else {
                    Boundary::More
                };
                let cap = payloads::Ledger::<resident::Shared<'d>>::MAX_MATERIALIZED_TAIL;
                if current.len() > cap || segments.byte_len() > cap - current.len() {
                    let (outcome, body) = match conn.streams().retained().try_to(
                        stream,
                        mem::take(current),
                        boundary,
                        streams::Limit::QUANTUM,
                    ) {
                        Ok(result) => result,
                        Err((error, returned)) => {
                            *current = returned;
                            return Err(error);
                        }
                    };
                    Self::retained(owner, stream, before, &body, outcome);
                    *current = match body {
                        Some(body) => body,
                        None => match segments.next() {
                            Some(body) => body.into(),
                            None => {
                                *pending = None;
                                return Ok(Some(outcome));
                            }
                        },
                    };
                    return Ok(Some(outcome));
                }
                (boundary, before)
            }
            Self::Inline(body) => {
                let before = if M::ENABLED {
                    hold::Charge::from_commit(body.len(), 0, 0)
                } else {
                    hold::Charge::default()
                };
                (end, before)
            }
        };
        let bytes = match &*state {
            Self::Retained { current, .. } => current.as_ref(),
            Self::Inline(body) => body.as_slice(),
        };
        let outcome =
            conn.streams()
                .try_to(stream, bytes, &[], boundary, streams::Limit::QUANTUM)?;
        let more = match state {
            Self::Retained { current, .. } => match outcome {
                conn::DataOutcome::Sent(written) => {
                    current.consume_max(written.get());
                    !current.is_empty()
                }
                conn::DataOutcome::Ended => {
                    *current = resident::Shared::new();
                    false
                }
                conn::DataOutcome::Blocked(_) => true,
            },
            Self::Inline(body) => match outcome {
                conn::DataOutcome::Sent(written) => {
                    body.advance(written.get());
                    body.len() != 0
                }
                conn::DataOutcome::Ended => false,
                conn::DataOutcome::Blocked(_) => true,
            },
        };
        Self::buffered(owner, stream, before, more, outcome);
        if !more {
            if let Self::Retained { current, segments } = state
                && let Some(next) = segments.next()
            {
                *current = next.into();
            } else {
                *pending = None;
            }
        }
        Ok(Some(outcome))
    }

    fn buffered<K, M>(
        owner: &mut owners::Sink<'_, K, M>,
        stream: stream::Key,
        before: hold::Charge,
        more: bool,
        outcome: conn::DataOutcome,
    ) where
        K: Copy,
        M: life::Mode<K, stream::Key>,
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
        } else if more {
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

    fn retained<K, M>(
        owner: &mut owners::Sink<'_, K, M>,
        stream: stream::Key,
        before: hold::Charge,
        body: &Option<resident::Shared<'d>>,
        outcome: conn::DataOutcome,
    ) where
        K: Copy,
        M: life::Mode<K, stream::Key>,
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
    pub(super) fn inline(body: Inline) -> Option<Self> {
        (body.len() != 0).then_some(Self {
            state: Some(State::Inline(body)),
        })
    }

    pub(super) fn new(body: owned::Segments<'d>) -> Option<Self> {
        let mut segments = body.into_iter();
        let current = segments.next()?.into();
        Some(Self {
            state: Some(State::Retained { current, segments }),
        })
    }

    pub(in crate::server) fn single(body: resident::Shared<'d>) -> Option<Self> {
        if body.is_empty() {
            return None;
        }
        Some(Self {
            state: Some(State::Retained {
                current: body,
                segments: owned::Segments::new().into_iter(),
            }),
        })
    }

    #[doc(hidden)]
    pub fn retention(&self) -> hold::Charge {
        match &self.state {
            Some(State::Retained { current, segments }) => {
                let current = hold::Charge::from_owner(current.len(), current.resident());
                let tail = segments.charge();
                hold::Charge::from_commit(
                    current.logical_bytes.wrapping_add(tail.logical_bytes),
                    current.resident_bytes.wrapping_add(tail.resident_bytes),
                    current.owners.wrapping_add(tail.owners),
                )
            }
            Some(State::Inline(body)) => hold::Charge::from_commit(body.len(), 0, 0),
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
        let mut owner = owners::Sink::new(&mut mode, ());
        self.turn_seen(conn, stream, Boundary::End, &mut owner)
    }

    #[doc(hidden)]
    pub fn turn_seen<F, E, K, M>(
        &mut self,
        conn: &mut machine::Connection<
            crate::ServerRole,
            F,
            resident::Shared<'d>,
            rules::RejectPush,
            E,
        >,
        stream: stream::Key,
        end: Boundary,
        owner: &mut owners::Sink<'_, K, M>,
    ) -> Result<schedule::Turn<conn::DataBlock>, conn::Error>
    where
        F: rules::Policy,
        E: event::Evidence,
        K: Copy,
        M: life::Mode<K, stream::Key>,
    {
        let Some(outcome) = State::send(&mut self.state, conn, stream, end, owner)? else {
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

const _: () = assert!(
    mem::size_of::<Pending<'static>>()
        == mem::size_of::<(resident::Shared<'static>, iter::Segments<'static>)>()
);
