use std::{convert, marker, mem};

use conn::write::{self, body};
use dope::{
    self,
    core::driver::{retained, route::Identity},
    manifold::{
        app::receive,
        network::listener::{conn, handler},
    },
    net::{link::egress, wire},
};
use duplex::{admit, machine};
use egress::{
    data::{self, compose},
    load::Value,
};
use hold::scope;
use o3::buffer::{hold, resident::bound};
use sark_core::duplex;
use sark_http::payload::owned;

use crate::{server::payload, session, switch};

const POLICY: hold::Policy = hold::Policy::share();

enum Wake {
    Idle,
    Turn,
}

enum Closed {
    Continue,
    ContinueTurn,
    Close,
    CloseTurn,
}

impl Closed {
    const fn close(self) -> Self {
        match self {
            Self::Continue | Self::Close => Self::Close,
            Self::ContinueTurn | Self::CloseTurn => Self::CloseTurn,
        }
    }
}

/// One borrowed raw-transition I/O turn.
pub struct Runtime<'a, C, K = ()> {
    conn: &'a mut C,
    tag: marker::PhantomData<fn() -> K>,
}

impl<'a, C, K> Runtime<'a, C, K> {
    pub fn new(conn: &'a mut C) -> Self {
        Self {
            conn,
            tag: marker::PhantomData,
        }
    }
}

impl<'a, 'turn, 'c, 'p, 's, 'w, 'd: 'w, State, Pending, Ready, Heads, ID: Identity, W, D, K>
    Runtime<
        'a,
        conn::Ctx<
            'turn,
            'c,
            'p,
            's,
            'w,
            'd,
            ID,
            W,
            session::Conn<
                'd,
                State,
                Pending,
                switch::Enabled<machine::Machine<'d, D>>,
                Ready,
                Heads,
            >,
            data::Bound<
                compose::Either<payload::Body<'d, K>, payload::Stream<'d, K>>,
                bound::Unbounded,
            >,
        >,
        K,
    >
where
    W: wire::Spec,
    D: duplex::Session<'d>,
    K: Copy + Default + 'd,
{
    /// Applies one raw activation, preserving input finish after HTTP drain.
    pub fn activate(mut self, driver: &mut retained::Context<'_, '_, 'd>) {
        if self.conn.is_closing() {
            self.finish_input(driver);
            return;
        }
        match self.flush(driver) {
            receive::Outcome::Continue => {}
            receive::Outcome::CloseAfter | receive::Outcome::Failed(_) => {
                self.conn.close_after();
            }
        }
    }

    /// Transfers one callback-owned chunk into its raw connection.
    pub fn chunk<R: hold::raw::Retain<'d>>(
        mut self,
        input: scope::Deferred<'_, 'd, R>,
        driver: &mut retained::Context<'_, '_, 'd>,
    ) -> receive::Outcome {
        let raw = match self.conn.state_mut().raw_mut() {
            Ok(raw) => raw,
            Err(switch::Mode::Http | switch::Mode::Switching(_)) => {
                return receive::Outcome::CloseAfter;
            }
        };
        if matches!(raw.hold(), machine::Hold::Held) {
            return receive::Outcome::CloseAfter;
        }
        let slot = match raw.input() {
            machine::Access::Vacant(slot) => slot,
            machine::Access::Blocked => {
                return receive::Outcome::Failed(receive::Failure::Contract);
            }
        };
        let input = match Self::retain(input) {
            Ok(input) => input,
            Err(error) => return receive::Outcome::Failed(error),
        };
        slot.commit(input);
        self.flush(driver)
    }

    /// Stages the pump-proven raw suffix until the HTTP drain completes.
    pub fn handoff<R: hold::raw::Retain<'d>>(
        self,
        input: scope::Deferred<'_, 'd, R>,
    ) -> receive::Outcome {
        let raw = match self.conn.state_mut().staged_mut() {
            Ok(raw) => raw,
            Err(switch::Mode::Http | switch::Mode::Raw(_)) => {
                return receive::Outcome::CloseAfter;
            }
        };
        let slot = match raw.input() {
            machine::Access::Vacant(slot) => slot,
            machine::Access::Blocked => return receive::Outcome::CloseAfter,
        };
        let tail = match Self::retain(input) {
            Ok(tail) => tail,
            Err(error) => return receive::Outcome::Failed(error),
        };
        slot.commit(tail);
        receive::Outcome::Continue
    }

    /// Delivers an orderly peer input finish without closing live output.
    pub fn finish_input(mut self, driver: &mut retained::Context<'_, '_, 'd>) -> handler::Drain {
        let raw = match self.conn.state_mut().raw_mut() {
            Ok(raw) => raw,
            Err(switch::Mode::Http | switch::Mode::Switching(_)) => {
                return handler::Drain::Complete;
            }
        };
        match raw.finish(()) {
            Ok((finish, ())) => finish.commit(),
            Err((machine::FinishError::Finished, ())) => {}
            Err((machine::FinishError::Closed, ())) => {
                return handler::Drain::Complete;
            }
        }
        match self.flush(driver) {
            receive::Outcome::CloseAfter => return handler::Drain::Complete,
            receive::Outcome::Continue => {}
            receive::Outcome::Failed(
                receive::Failure::Staging(..)
                | receive::Failure::Input(..)
                | receive::Failure::Accept(..)
                | receive::Failure::Hold(..)
                | receive::Failure::FinishCapacity
                | receive::Failure::ParseCapacity
                | receive::Failure::Contract,
            ) => {
                return handler::Drain::Complete;
            }
        }
        let hold = match self.conn.state().raw() {
            Ok(raw) => raw.hold(),
            Err(switch::Mode::Http | switch::Mode::Switching(_)) => machine::Hold::Released,
        };
        match hold {
            machine::Hold::Held => handler::Drain::Pending,
            machine::Hold::Released => match self.conn.write().pending() {
                egress::Pending::Present => handler::Drain::Pending,
                egress::Pending::Absent => handler::Drain::Complete,
            },
        }
    }

    fn retain<R: hold::raw::Retain<'d>>(
        input: scope::Deferred<'_, 'd, R>,
    ) -> Result<owned::Segments<'d>, receive::Failure> {
        let part = input
            .bind(POLICY)
            .try_into()
            .map_err(receive::Failure::Hold)?;
        Ok(owned::Segments::from(part))
    }

    fn flush(&mut self, driver: &mut retained::Context<'_, '_, 'd>) -> receive::Outcome {
        use write::error::Error;

        let raw = match self.conn.state_mut().raw_mut() {
            Ok(raw) => raw,
            Err(switch::Mode::Http | switch::Mode::Switching(_)) => {
                return receive::Outcome::CloseAfter;
            }
        };
        match raw.progress() {
            machine::Progress::Idle | machine::Progress::Finished => {
                return receive::Outcome::Continue;
            }
            machine::Progress::Turn => {}
        }
        match raw.output() {
            machine::Output::Open => {}
            machine::Output::Finished => {
                let closed = Self::flush_closed(raw);
                return self.apply_closed(closed);
            }
        }
        match self.conn.write().try_draft(
            driver,
            move |state, _| -> Result<_, convert::Infallible> {
                let raw = match state.raw_mut() {
                    Ok(raw) => raw,
                    Err(switch::Mode::Http | switch::Mode::Switching(_)) => {
                        return Ok(write::Draft::new(0, None, Wake::Idle).close_after());
                    }
                };
                Ok(
                    match raw.transact(|send| {
                        let (payload, end, after) = send.into_parts();
                        let wake = match after {
                            machine::After::Idle | machine::After::Finished => Wake::Idle,
                            machine::After::Turn => Wake::Turn,
                        };
                        let draft = |output: Option<_>| {
                            let draft = write::Draft::new(0, output.map(Value::Unchecked), wake);
                            match end {
                                machine::End::Open => draft,
                                machine::End::FinishOutput => draft.shutdown(),
                                machine::End::FinishSession => draft.close_after(),
                            }
                        };
                        match payload {
                            admit::Payload::Present { output, permit } => {
                                let output = egress::Load::new(payload::Body::duplex(
                                    owned::Segments::from(output),
                                    K::default(),
                                ))
                                .into_left()
                                .unbounded();
                                permit.commit::<_, convert::Infallible>(draft(Some(output)))
                            }
                            admit::Payload::Vacant(permit) => {
                                permit.commit::<_, convert::Infallible>(draft(None))
                            }
                        }
                    }) {
                        machine::Transaction::Committed { receipt: done, .. } => done,
                        machine::Transaction::Violation(machine::Violation::OutputAfterFinish) => {
                            write::Draft::new(0, None, Wake::Idle).close_after()
                        }
                        machine::Transaction::Idle => write::Draft::new(0, None, Wake::Idle),
                        machine::Transaction::Pending { receipt } => receipt.close_after(),
                        machine::Transaction::Rejected(never) => match never {},
                    },
                )
            },
        ) {
            Ok(write::Outcome::Committed(Wake::Idle)) => receive::Outcome::Continue,
            Ok(write::Outcome::Committed(Wake::Turn)) => {
                self.conn.wake_target().wake();
                receive::Outcome::Continue
            }
            Ok(write::Outcome::Deferred(..)) => receive::Outcome::Continue,
            Ok(write::Outcome::Closed) => receive::Outcome::CloseAfter,
            Err(Error::Capacity(cause)) => {
                self.conn.write().reject(body::Failure::Capacity(cause));
                receive::Outcome::CloseAfter
            }
            Err(Error::Mismatch(error)) => {
                let (cause, _) = error.into_parts();
                self.conn.write().reject(body::Failure::Mismatch(cause));
                receive::Outcome::CloseAfter
            }
            Err(Error::Build(never)) => match never {},
        }
    }

    fn flush_closed(raw: &mut machine::Machine<'d, D>) -> Closed {
        let turn = raw.transact(|send| {
            let (payload, end, after) = send.into_parts();
            match payload {
                admit::Payload::Present { output, permit } => {
                    drop(output);
                    let receipt = match after {
                        machine::After::Turn => Closed::CloseTurn,
                        machine::After::Idle | machine::After::Finished => Closed::Close,
                    };
                    permit.commit::<_, convert::Infallible>(receipt)
                }
                admit::Payload::Vacant(permit) => {
                    let receipt = match (end, after) {
                        (machine::End::FinishSession, machine::After::Turn) => Closed::CloseTurn,
                        (
                            machine::End::FinishSession,
                            machine::After::Idle | machine::After::Finished,
                        )
                        | (
                            machine::End::Open | machine::End::FinishOutput,
                            machine::After::Finished,
                        ) => Closed::Close,
                        (machine::End::Open | machine::End::FinishOutput, machine::After::Turn) => {
                            Closed::ContinueTurn
                        }
                        (machine::End::Open | machine::End::FinishOutput, machine::After::Idle) => {
                            Closed::Continue
                        }
                    };
                    permit.commit::<_, convert::Infallible>(receipt)
                }
            }
        });
        match turn {
            machine::Transaction::Committed { receipt: done, .. } => done,
            machine::Transaction::Idle => Closed::Continue,
            machine::Transaction::Violation(machine::Violation::OutputAfterFinish) => Closed::Close,
            machine::Transaction::Pending { receipt } => receipt.close(),
            machine::Transaction::Rejected(never) => match never {},
        }
    }

    fn apply_closed(&mut self, closed: Closed) -> receive::Outcome {
        match closed {
            Closed::Continue => receive::Outcome::Continue,
            Closed::ContinueTurn => {
                self.conn.wake_target().wake();
                receive::Outcome::Continue
            }
            Closed::Close => {
                self.conn.close_after();
                receive::Outcome::CloseAfter
            }
            Closed::CloseTurn => {
                self.conn.wake_target().wake();
                self.conn.close_after();
                receive::Outcome::CloseAfter
            }
        }
    }
}

const _: () = assert!(mem::size_of::<Runtime<'static, (), ()>>() == mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Wake>() == mem::size_of::<u8>());
const _: () = assert!(mem::size_of::<Closed>() == mem::size_of::<u8>());
