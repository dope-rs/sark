use std::{convert, marker, mem};

use conn::write::{self, body};
use dope::{
    self,
    core::driver::{self, route::Identity},
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
use o3::buffer::{hold, resident::bound, write::Slice};
use sark_core::duplex;
use sark_http::payload::owned;

use crate::{server::payload, session, switch};

const POLICY: hold::Policy = hold::Policy::share();

/// One borrowed raw-transition I/O turn.
pub struct Runtime<'a, C, K = ()> {
    conn: &'a mut C,
    scratch: &'a mut Vec<u8>,
    tag: marker::PhantomData<fn() -> K>,
}

impl<'a, C, K> Runtime<'a, C, K> {
    pub fn new(conn: &'a mut C, scratch: &'a mut Vec<u8>) -> Self {
        Self {
            conn,
            scratch,
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
    pub fn activate(mut self, driver: &mut driver::Context<'_, 'd>) {
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
        driver: &mut driver::Context<'_, 'd>,
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
    pub fn finish_input(mut self, driver: &mut driver::Context<'_, 'd>) -> handler::Drain {
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

    fn flush(&mut self, driver: &mut driver::Context<'_, 'd>) -> receive::Outcome {
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
                return match Self::flush_closed(raw, self.scratch) {
                    machine::After::Idle => receive::Outcome::Continue,
                    machine::After::Turn => {
                        self.conn.wake_target().wake();
                        receive::Outcome::Continue
                    }
                    machine::After::Finished => {
                        self.conn.close_after();
                        receive::Outcome::CloseAfter
                    }
                };
            }
        }
        let scratch = &mut *self.scratch;
        match self.conn.write().try_commit(
            driver,
            move |state, mut prefix| -> Result<_, convert::Infallible> {
                let raw = match state.raw_mut() {
                    Ok(raw) => raw,
                    Err(switch::Mode::Http | switch::Mode::Switching(_)) => {
                        return Ok(prefix.commit(None, machine::After::Idle).close_after());
                    }
                };
                let (body, end, after) = match raw.transact(prefix.writer(), scratch, |send| {
                    let (payload, end, after) = send.into_parts();
                    match payload {
                        admit::Payload::Present { output, permit } => {
                            let output =
                                egress::Load::new(payload::Body::duplex(output, K::default()))
                                    .into_left()
                                    .unbounded();
                            permit.commit::<_, convert::Infallible>((
                                Some(Value::Unchecked(output)),
                                end,
                                after,
                            ))
                        }
                        admit::Payload::Vacant(permit) => {
                            permit.commit::<_, convert::Infallible>((None, end, after))
                        }
                    }
                }) {
                    machine::Transaction::Committed { receipt, .. } => receipt,
                    machine::Transaction::Violation(
                        machine::Violation::OutputAfterFinish | machine::Violation::PrefixPending,
                    ) => (None, machine::End::FinishSession, machine::After::Idle),
                    machine::Transaction::Idle => (None, machine::End::Open, machine::After::Idle),
                    machine::Transaction::Pending {
                        receipt: (body, _, after),
                    } => (body, machine::End::FinishSession, after),
                    machine::Transaction::Rejected(never) => match never {},
                };
                let commit = prefix.commit(body, after);
                Ok(match end {
                    machine::End::Open => commit,
                    machine::End::FinishOutput => commit.shutdown(),
                    machine::End::FinishSession => commit.close_after(),
                })
            },
        ) {
            Ok(write::Outcome::Committed(machine::After::Idle | machine::After::Finished)) => {
                receive::Outcome::Continue
            }
            Ok(write::Outcome::Committed(machine::After::Turn)) => {
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

    fn flush_closed(raw: &mut machine::Machine<'d, D>, scratch: &mut Vec<u8>) -> machine::After {
        let turn = raw.transact(Slice::new(&mut []).reborrow(), scratch, |send| {
            // Finished output rejects every payload before this admission.
            let admit::Payload::Vacant(permit) = send.into_parts().0 else {
                unreachable!("closed")
            };
            permit.commit::<_, convert::Infallible>(())
        });
        match turn {
            machine::Transaction::Committed { after, .. } => after,
            machine::Transaction::Idle => machine::After::Idle,
            machine::Transaction::Violation(
                machine::Violation::OutputAfterFinish | machine::Violation::PrefixPending,
            ) => machine::After::Finished,
            machine::Transaction::Pending { .. } => unreachable!("committed"),
            machine::Transaction::Rejected(never) => match never {},
        }
    }
}

const _: () = assert!(mem::size_of::<Runtime<'static, (), ()>>() == 2 * mem::size_of::<usize>());
