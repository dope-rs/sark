//! Compile-time wire-failure retention policy.

use std::{convert::Infallible, mem};

use dope::{
    core::Errno,
    manifold::client::conn::Transmit,
    net::{
        wire,
        wire::{failure, terminal},
    },
};
use dope_fiber::reply;
use o3::{
    alloc,
    base::cell::region,
    depot::{
        arena::pool::{Unique, shared},
        capacity,
    },
};
use sark_client::{
    conn,
    conn::{Cause, Error},
};

use crate::client::config;

/// Proves that this client has no wire failure value to retain.
#[derive(Clone, Copy)]
pub struct Never;

/// Retains each exact wire failure in preallocated domain-local storage.
pub enum Retain {}

#[doc(hidden)]
pub struct Store<E>(shared::Store<E>);

#[doc(hidden)]
pub trait Class: config::Guard {
    fn capacity(live: usize, retained: usize) -> Result<capacity::Count, config::Error>;
}

impl Class for Never {
    fn capacity(_: usize, _: usize) -> Result<capacity::Count, config::Error> {
        Ok(capacity::Count::EMPTY)
    }
}

impl Class for Retain {
    fn capacity(live: usize, retained: usize) -> Result<capacity::Count, config::Error> {
        let total = live
            .checked_add(retained)
            .ok_or(config::Error::FailureReports { live, retained })?;
        capacity::Count::try_from(total).map_err(config::Error::FailureReportCapacity)
    }
}

#[doc(hidden)]
pub enum Arm<D> {
    Ready,
    Exhausted,
    Failed(Error<D>),
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[doc(hidden)]
pub enum Disarm {
    Vacant,
    Detailed,
}

#[doc(hidden)]
pub enum Failure<E> {
    Wire(E),
    Receive(Errno),
    Transmit(Transmit),
}

fn native<D>(receive: Option<Errno>, transmit: Option<Transmit>) -> Option<Error<D>> {
    match transmit {
        Some(error) => Some(Error::Transmit {
            error,
            receive,
            wire: None,
        }),
        None => receive.map(Error::receive),
    }
}

#[doc(hidden)]
pub trait Policy<E>: config::Guard {
    type Detail<'f>: conn::Detail + 'f
    where
        E: 'f;
    type Pool;
    type Source<'f>
    where
        E: 'f;
    type State<'f, 'd>
    where
        E: 'f;

    fn try_pool(capacity: capacity::Count) -> Result<Self::Pool, alloc::Error>;

    fn source<'f>(pool: &'f Self::Pool) -> Self::Source<'f>
    where
        E: 'f;

    fn vacant<'f, 'd>(token: &region::Token<region::Identity<'d>>) -> Self::State<'f, 'd>
    where
        E: 'f;

    fn arm<'f, 'd>(
        pool: &Self::Source<'f>,
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Arm<Self::Detail<'f>>
    where
        E: 'f;

    fn disarm<'f, 'd>(
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Disarm
    where
        E: 'f;

    fn failed<'f, 'd>(
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
        cause: Failure<E>,
    ) where
        E: 'f;

    /// Clears the report before returning `Complete`; `Pending` preserves it.
    fn retire<'f, 'd>(
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
        run: impl FnOnce(Option<&Error<Self::Detail<'f>>>, reply::Retirement<'_, 'd>) -> reply::Progress,
    ) -> reply::Progress
    where
        E: 'f;
}

trait Impossible {
    fn eliminate<T>(self) -> T;
}

impl Impossible for Infallible {
    fn eliminate<T>(self) -> T {
        match self {}
    }
}

impl<'d, W: wire::Spec<TerminalFailure = failure::Never> + 'd> Impossible
    for terminal::Error<'d, W>
{
    fn eliminate<T>(self) -> T {
        match *self.causes().0 {}
    }
}

impl<E: Impossible> Policy<E> for Never {
    type Detail<'f>
        = Infallible
    where
        E: 'f;
    type Pool = Self;
    type Source<'f>
        = Self
    where
        E: 'f;
    type State<'f, 'd>
        = region::Value<region::Identity<'d>, (Option<Errno>, Option<Transmit>)>
    where
        E: 'f;

    fn try_pool(_: capacity::Count) -> Result<Self::Pool, alloc::Error> {
        Ok(Self)
    }

    fn source<'f>(pool: &'f Self::Pool) -> Self::Source<'f>
    where
        E: 'f,
    {
        *pool
    }

    fn vacant<'f, 'd>(token: &region::Token<region::Identity<'d>>) -> Self::State<'f, 'd>
    where
        E: 'f,
    {
        token.bind((None, None))
    }

    fn arm<'f, 'd>(
        _: &Self::Source<'f>,
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Arm<Self::Detail<'f>>
    where
        E: 'f,
    {
        let (receive, transmit) = *token.borrow(state);
        match native(receive, transmit) {
            None => Arm::Ready,
            Some(error) => Arm::Failed(error),
        }
    }

    fn disarm<'f, 'd>(
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Disarm
    where
        E: 'f,
    {
        match token.borrow(state) {
            (None, None) => Disarm::Vacant,
            _ => Disarm::Detailed,
        }
    }

    fn failed<'f, 'd>(
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
        cause: Failure<E>,
    ) where
        E: 'f,
    {
        let (receive, transmit) = token.borrow_mut(state);
        match cause {
            Failure::Wire(cause) => cause.eliminate(),
            Failure::Receive(errno) => {
                receive.get_or_insert(errno);
            }
            Failure::Transmit(failure) => {
                transmit.get_or_insert(failure);
            }
        }
    }

    fn retire<'f, 'd>(
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
        run: impl FnOnce(Option<&Error<Self::Detail<'f>>>, reply::Retirement<'_, 'd>) -> reply::Progress,
    ) -> reply::Progress
    where
        E: 'f,
    {
        let (receive, transmit) = mem::take(token.borrow_mut(state));
        let cause = native(receive, transmit);
        let progress = run(cause.as_ref(), reply::Retirement::new(token));
        if matches!(progress, reply::Progress::Pending(_)) {
            *token.borrow_mut(state) = (receive, transmit);
        }
        progress
    }
}

enum Record<'f, E> {
    Vacant,
    Armed {
        slot: Unique<'f, shared::Resident<E>, shared::Header<E>>,
        receive: Option<Errno>,
        transmit: Option<Transmit>,
    },
    Detailed(Error<Cause<'f, E>>),
}

#[doc(hidden)]
pub struct State<'f, 'd, E>(region::Value<region::Identity<'d>, Record<'f, E>>);

impl<E> Policy<E> for Retain {
    type Detail<'f>
        = Cause<'f, E>
    where
        E: 'f;
    type Pool = Store<E>;
    type Source<'f>
        = &'f shared::Header<E>
    where
        E: 'f;
    type State<'f, 'd>
        = State<'f, 'd, E>
    where
        E: 'f;

    fn try_pool(capacity: capacity::Count) -> Result<Self::Pool, alloc::Error> {
        Ok(Store(shared::Store::try_new(capacity)?))
    }

    fn source<'f>(pool: &'f Self::Pool) -> Self::Source<'f>
    where
        E: 'f,
    {
        pool.0.vacancies()
    }

    fn vacant<'f, 'd>(token: &region::Token<region::Identity<'d>>) -> Self::State<'f, 'd>
    where
        E: 'f,
    {
        State(token.bind(Record::Vacant))
    }

    fn arm<'f, 'd>(
        pool: &Self::Source<'f>,
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Arm<Self::Detail<'f>>
    where
        E: 'f,
    {
        let report = token.borrow_mut(&state.0);
        match report {
            Record::Armed { .. } => Arm::Ready,
            Record::Detailed(error) => Arm::Failed(error.try_clone().unwrap_or_else(Error::Retain)),
            Record::Vacant => match pool.vacancy() {
                Some(vacancy) => {
                    *report = Record::Armed {
                        slot: vacancy.reserve(),
                        receive: None,
                        transmit: None,
                    };
                    Arm::Ready
                }
                None => Arm::Exhausted,
            },
        }
    }

    fn disarm<'f, 'd>(
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Disarm
    where
        E: 'f,
    {
        let report = token.borrow_mut(&state.0);
        match report {
            Record::Vacant
            | Record::Armed {
                receive: None,
                transmit: None,
                ..
            } => {
                *report = Record::Vacant;
                Disarm::Vacant
            }
            Record::Armed {
                receive, transmit, ..
            } => {
                match native(*receive, *transmit) {
                    Some(error) => *report = Record::Detailed(error),
                    None => unreachable!("report"),
                }
                Disarm::Detailed
            }
            Record::Detailed(_) => Disarm::Detailed,
        }
    }

    fn failed<'f, 'd>(
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
        cause: Failure<E>,
    ) where
        E: 'f,
    {
        let report = token.borrow_mut(&state.0);
        *report = match (mem::replace(report, Record::Vacant), cause) {
            (Record::Vacant, Failure::Wire(_)) => Record::Vacant,
            (Record::Vacant, Failure::Receive(errno)) => Record::Detailed(Error::receive(errno)),
            (Record::Vacant, Failure::Transmit(error)) => Record::Detailed(Error::transmit(error)),
            (
                Record::Armed {
                    slot,
                    mut receive,
                    transmit,
                },
                Failure::Receive(errno),
            ) => {
                receive.get_or_insert(errno);
                Record::Armed {
                    slot,
                    receive,
                    transmit,
                }
            }
            (
                Record::Armed {
                    slot,
                    receive,
                    mut transmit,
                },
                Failure::Transmit(error),
            ) => {
                transmit.get_or_insert(error);
                Record::Armed {
                    slot,
                    receive,
                    transmit,
                }
            }
            (
                Record::Armed {
                    slot,
                    receive,
                    transmit,
                },
                Failure::Wire(cause),
            ) => {
                let cause = Cause::new(slot.commit(cause));
                let error = match receive {
                    Some(errno) => Error::wire_receive(cause, errno),
                    None => Error::wire(cause),
                };
                let error = match transmit {
                    Some(transmit) => error.with_transmit(transmit),
                    None => error,
                };
                Record::Detailed(error)
            }
            (Record::Detailed(error), Failure::Receive(errno)) => {
                Record::Detailed(error.with_receive(errno))
            }
            (Record::Detailed(error), Failure::Transmit(transmit)) => {
                Record::Detailed(error.with_transmit(transmit))
            }
            (detailed @ Record::Detailed(_), Failure::Wire(_)) => detailed,
        };
    }

    fn retire<'f, 'd>(
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
        run: impl FnOnce(Option<&Error<Self::Detail<'f>>>, reply::Retirement<'_, 'd>) -> reply::Progress,
    ) -> reply::Progress
    where
        E: 'f,
    {
        let record = mem::replace(token.borrow_mut(&state.0), Record::Vacant);
        let retiring = reply::Retirement::new(token);
        let progress = match &record {
            Record::Detailed(error) => run(Some(error), retiring),
            Record::Armed {
                receive, transmit, ..
            } => run(native(*receive, *transmit).as_ref(), retiring),
            Record::Vacant => run(None, retiring),
        };
        if matches!(progress, reply::Progress::Pending(_)) {
            *token.borrow_mut(&state.0) = record;
        }
        progress
    }
}

const _: () = assert!(
    mem::size_of::<<Never as Policy<Infallible>>::State<'static, 'static>>()
        == mem::size_of::<(Option<Errno>, Option<Transmit>)>()
);
const _: () = assert!(mem::size_of::<<Never as Policy<Infallible>>::Pool>() == 0);
const _: () = assert!(mem::size_of::<<Never as Policy<Infallible>>::Source<'static>>() == 0);
const _: () =
    assert!(mem::size_of::<Store<Infallible>>() == mem::size_of::<shared::Store<Infallible>>());
const _: () = assert!(
    mem::size_of::<State<'static, 'static, Infallible>>()
        == mem::size_of::<Record<'static, Infallible>>()
);
