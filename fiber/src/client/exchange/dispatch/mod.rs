//! One linear connection-admission and request-submission transition.

mod admit;
mod backend;
mod phases;
mod prepared;

use std::{ops::ControlFlow, pin, task};

pub(in crate::client) use admit::Admit;
pub(in crate::client) use backend::Backend;
use dope::core::driver::schedule::ready::wake::queue;
use dope_fiber::{
    abi::{self, machine},
    context,
    task::time::budget,
};
pub(in crate::client) use phases::{Rejected, Submitted};
pub(in crate::client) use prepared::Prepared;
use sark_client::Error;

use crate::client::exchange::request::{Admitted, Encoded, Headers};

enum State<'scope, 'request, 'app, 'd: 'app, R, D, L: 'app> {
    Prepare {
        request: &'scope Admitted<'request>,
        headers: Headers<'request>,
    },
    Wait {
        prepared: Prepared<R>,
        registration: queue::Registration<'app, 'd, L>,
    },
    Dial {
        dial: abi::Task<D>,
        encoded: Encoded,
    },
}

#[must_use = "a request is not dispatched unless this fiber is polled"]
pub(in crate::client) struct Dispatch<'scope, 'request, 'app, 'd, B: Backend<'app, 'd>> {
    backend: B,
    check: budget::Check<'d>,
    state: machine::Machine<State<'scope, 'request, 'app, 'd, B::Route, B::Dial, B::Lanes>>,
}

impl<'app, 'd, B: Backend<'app, 'd>> Drop for Dispatch<'_, '_, 'app, 'd, B> {
    fn drop(&mut self) {
        if let Some(State::Wait {
            mut registration, ..
        }) = self.state.take()
        {
            self.backend.cancel(&mut registration);
        }
    }
}

impl<'scope, 'request, 'app, 'd, B: Backend<'app, 'd>> Dispatch<'scope, 'request, 'app, 'd, B> {
    pub(in crate::client) const fn new(
        backend: B,
        request: &'scope Admitted<'request>,
        headers: Headers<'request>,
        check: budget::Check<'d>,
    ) -> Self {
        Self {
            backend,
            check,
            state: machine::Machine::new(State::Prepare { request, headers }),
        }
    }
}

impl<'app, 'd, B: Backend<'app, 'd>> abi::Fiber<'d> for Dispatch<'_, '_, 'app, 'd, B> {
    type Output = Result<Submitted<B::Stream>, Rejected<B::Cause, B::Mailbox>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut admit) = call.into_admit();
        let this = this.get_mut();
        let backend = this.backend;
        let check = this.check;
        this.state.step(|mut state| {
            loop {
                state = match state {
                    State::Prepare { request, headers } => {
                        match backend.prepare(request, headers) {
                            Ok(prepared) => State::Wait {
                                prepared,
                                registration: queue::Registration::new(),
                            },
                            Err(error) => {
                                return ControlFlow::Break(Err(Rejected::new(error)));
                            }
                        }
                    }
                    State::Wait {
                        prepared,
                        registration: mut reg,
                    } => match backend.admit(prepared, check, admit.context().driver()) {
                        Ok(Admit::Stream(stream)) => {
                            return ControlFlow::Break(Ok(Submitted::new(stream)));
                        }
                        Ok(Admit::Dial(dial, encoded)) => State::Dial {
                            dial: abi::Task::new(dial),
                            encoded,
                        },
                        Ok(Admit::Wait(prepared)) => {
                            if let Err(error) = backend.try_register(
                                prepared.route(),
                                &mut reg,
                                admit.context().as_ref(),
                            ) {
                                backend.cancel(&mut reg);
                                return ControlFlow::Break(Err(Rejected::new(Error::Capacity(
                                    error,
                                ))));
                            }
                            return ControlFlow::Continue(State::Wait {
                                prepared,
                                registration: reg,
                            });
                        }
                        Err(error) => {
                            backend.cancel(&mut reg);
                            return ControlFlow::Break(Err(Rejected::new(error)));
                        }
                    },
                    State::Dial { mut dial, encoded } => {
                        let (poll, mut context) = admit.poll(pin::Pin::new(&mut dial));
                        return match poll {
                            task::Poll::Ready(Ok(conn)) => {
                                let result =
                                    backend.submit(conn, encoded, check, context.as_mut().driver());
                                ControlFlow::Break(
                                    result.map(Submitted::new).map_err(Rejected::new),
                                )
                            }
                            task::Poll::Ready(Err(error)) => {
                                ControlFlow::Break(Err(Rejected::new(Error::Connect(error))))
                            }
                            task::Poll::Pending => {
                                ControlFlow::Continue(State::Dial { dial, encoded })
                            }
                        };
                    }
                };
            }
        })
    }
}
