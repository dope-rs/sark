//! Compile-time wire-failure retention policy.

use std::{convert::Infallible, mem};

use dope::{
    core::Errno,
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
        = region::Value<region::Identity<'d>, Option<Errno>>
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
        token.bind(None)
    }

    fn arm<'f, 'd>(
        _: &Self::Source<'f>,
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Arm<Self::Detail<'f>>
    where
        E: 'f,
    {
        match *token.borrow_mut(state) {
            None => Arm::Ready,
            Some(errno) => Arm::Failed(Error::receive(errno)),
        }
    }

    fn disarm<'f, 'd>(
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
    ) -> Disarm
    where
        E: 'f,
    {
        match token.borrow_mut(state) {
            Some(_) => Disarm::Detailed,
            None => Disarm::Vacant,
        }
    }

    fn failed<'f, 'd>(
        state: &Self::State<'f, 'd>,
        token: &mut region::Token<region::Identity<'d>>,
        cause: Failure<E>,
    ) where
        E: 'f,
    {
        match cause {
            Failure::Wire(cause) => cause.eliminate(),
            Failure::Receive(errno) => {
                let stored = token.borrow_mut(state);
                if stored.is_none() {
                    *stored = Some(errno);
                }
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
        let cause = (*token.borrow(state)).map(Error::receive);
        let progress = run(cause.as_ref(), reply::Retirement::new(token));
        match progress {
            reply::Progress::Complete(_) => *token.borrow_mut(state) = None,
            reply::Progress::Pending(_) => {}
        }
        progress
    }
}

enum Record<'f, E> {
    Vacant,
    Armed {
        slot: Unique<'f, shared::Resident<E>, shared::Header<E>>,
        receive: Option<Errno>,
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
            Record::Vacant | Record::Armed { receive: None, .. } => {
                *report = Record::Vacant;
                Disarm::Vacant
            }
            Record::Armed {
                receive: Some(errno),
                ..
            } => {
                *report = Record::Detailed(Error::receive(*errno));
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
            (
                Record::Armed {
                    slot,
                    receive: None,
                },
                Failure::Receive(errno),
            ) => Record::Armed {
                slot,
                receive: Some(errno),
            },
            (
                armed @ Record::Armed {
                    receive: Some(_), ..
                },
                Failure::Receive(_),
            ) => armed,
            (Record::Armed { slot, receive }, Failure::Wire(cause)) => {
                let cause = Cause::new(slot.commit(cause));
                let error = match receive {
                    Some(errno) => Error::wire_receive(cause, errno),
                    None => Error::wire(cause),
                };
                Record::Detailed(error)
            }
            (
                Record::Detailed(Error::Wire {
                    detail,
                    receive: None,
                }),
                Failure::Receive(errno),
            ) => Record::Detailed(Error::wire_receive(detail, errno)),
            (detailed @ Record::Detailed(_), Failure::Receive(_) | Failure::Wire(_)) => detailed,
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
                receive: Some(errno),
                ..
            } => run(Some(&Error::receive(*errno)), retiring),
            Record::Vacant | Record::Armed { receive: None, .. } => run(None, retiring),
        };
        let next = match progress {
            reply::Progress::Complete(_) => Record::Vacant,
            reply::Progress::Pending(_) => record,
        };
        *token.borrow_mut(&state.0) = next;
        progress
    }
}

const _: () = assert!(
    mem::size_of::<<Never as Policy<Infallible>>::State<'static, 'static>>()
        == mem::size_of::<Option<Errno>>()
);
const _: () = assert!(mem::size_of::<<Never as Policy<Infallible>>::Pool>() == 0);
const _: () = assert!(mem::size_of::<<Never as Policy<Infallible>>::Source<'static>>() == 0);
const _: () =
    assert!(mem::size_of::<Store<Infallible>>() == mem::size_of::<shared::Store<Infallible>>());
const _: () = assert!(
    mem::size_of::<State<'static, 'static, Infallible>>()
        == mem::size_of::<Record<'static, Infallible>>()
);
