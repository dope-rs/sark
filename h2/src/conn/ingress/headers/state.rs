use core::mem;

use sark_http::field::metadata::section::{rules, storage};

use crate::{conn, conn::ingress::headers::Active, hpack::decode};

pub(super) enum Mode<P: rules::Policy> {
    Idle(decode::Decoder, storage::Scratch),
    Active(Active<P>),
    Failed,
}

pub(super) struct State<P: rules::Policy> {
    mode: Mode<P>,
}

impl<P: rules::Policy> State<P> {
    pub(super) fn new(decoder: decode::Decoder) -> Self {
        Self {
            mode: Mode::Idle(decoder, storage::Scratch::default()),
        }
    }

    pub(super) fn active(&self) -> Option<&Active<P>> {
        match &self.mode {
            Mode::Active(active) => Some(active),
            Mode::Idle(_, _) | Mode::Failed => None,
        }
    }

    pub(super) fn try_active(&self) -> Result<&Active<P>, conn::Error> {
        match &self.mode {
            Mode::Active(active) => Ok(active),
            Mode::Idle(_, _) => Err(conn::Error::HeaderState(conn::HeaderState {
                expected: conn::HeaderPhase::Active,
                actual: conn::HeaderPhase::Idle,
            })),
            Mode::Failed => Err(conn::Error::HeaderState(conn::HeaderState {
                expected: conn::HeaderPhase::Active,
                actual: conn::HeaderPhase::Failed,
            })),
        }
    }

    pub(super) fn try_mut(&mut self) -> Result<&mut Active<P>, conn::Error> {
        match &mut self.mode {
            Mode::Active(active) => Ok(active),
            Mode::Idle(_, _) => Err(conn::Error::HeaderState(conn::HeaderState {
                expected: conn::HeaderPhase::Active,
                actual: conn::HeaderPhase::Idle,
            })),
            Mode::Failed => Err(conn::Error::HeaderState(conn::HeaderState {
                expected: conn::HeaderPhase::Active,
                actual: conn::HeaderPhase::Failed,
            })),
        }
    }

    /// Both maps leave self.mode at Failed while change owns the extracted payload.
    pub(super) fn map_idle<R>(
        &mut self,
        change: impl FnOnce(decode::Decoder, storage::Scratch) -> (Mode<P>, Result<R, conn::Error>),
    ) -> Result<R, conn::Error> {
        let (mode, result) = match mem::replace(&mut self.mode, Mode::Failed) {
            Mode::Idle(decoder, scratch) => change(decoder, scratch),
            Mode::Active(active) => (
                Mode::Active(active),
                Err(conn::Error::HeaderState(conn::HeaderState {
                    expected: conn::HeaderPhase::Idle,
                    actual: conn::HeaderPhase::Active,
                })),
            ),
            Mode::Failed => (
                Mode::Failed,
                Err(conn::Error::HeaderState(conn::HeaderState {
                    expected: conn::HeaderPhase::Idle,
                    actual: conn::HeaderPhase::Failed,
                })),
            ),
        };
        mem::forget(mem::replace(&mut self.mode, mode));
        result
    }

    pub(super) fn map_active<R>(
        &mut self,
        change: impl FnOnce(Active<P>) -> (Mode<P>, Result<R, conn::Error>),
    ) -> Result<R, conn::Error> {
        let (mode, result) = match mem::replace(&mut self.mode, Mode::Failed) {
            Mode::Active(active) => change(active),
            Mode::Idle(decoder, scratch) => (
                Mode::Idle(decoder, scratch),
                Err(conn::Error::HeaderState(conn::HeaderState {
                    expected: conn::HeaderPhase::Active,
                    actual: conn::HeaderPhase::Idle,
                })),
            ),
            Mode::Failed => (
                Mode::Failed,
                Err(conn::Error::HeaderState(conn::HeaderState {
                    expected: conn::HeaderPhase::Active,
                    actual: conn::HeaderPhase::Failed,
                })),
            ),
        };
        mem::forget(mem::replace(&mut self.mode, mode));
        result
    }
}
