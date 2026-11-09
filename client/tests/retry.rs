use std::num::NonZero;

use dope::core::driver::{life::recovery, settings::claim::Needs};
use sark_client::{RequestError, protocol};
use sark_http::method::Method;

pub mod common;

use std::time::Duration;

use common::retry::aggressive;
use dope::{
    core::Errno,
    manifold::client::{attempt, close::LocalOrigin},
    net::link::{event, pool::change::opening::Exhaustion},
};
use dope_fiber::task::time::budget;
use dope_test::alloc::Tracker;
use sark_client::{
    Error, Failure, Safety, conn, connect,
    protocol::retry::{Policy, State},
    timeout,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn attempt_failures_final() {
    use o3::depot::arena::slab::key;

    let observed = key::Observation::Generation {
        actual: key::Generation::MIN,
    };
    for cause in [
        attempt::ConnectFault::RuntimeShutdown,
        attempt::ConnectFault::Attempt(attempt::AttemptError::Lookup(observed)),
        attempt::ConnectFault::Cleanup {
            cause: event::ConnectError::Connect(Errno::NO_BUFFER_SPACE),
            error: attempt::AttemptError::Lookup(observed),
        },
    ] {
        let (failure, counts) = Tracker::measure(|| {
            let failure: Failure = Failure::new(
                Error::Connect(connect::Error::Attempt(cause)),
                Safety::BeforeSubmit,
            );
            let retry = State::new(aggressive(), &Method::GET);
            let retry = retry.retry_after(&failure).err().expect("terminal failure");
            retry.finish(failure)
        });
        assert_eq!(counts, [0; 4]);
        match failure.cause() {
            Error::Connect(connect::Error::Attempt(attempt::ConnectFault::RuntimeShutdown)) => {}
            Error::Connect(connect::Error::Attempt(attempt::ConnectFault::Attempt(
                attempt::AttemptError::Lookup(actual),
            ))) => assert!((*actual) == (observed)),
            Error::Connect(connect::Error::Attempt(attempt::ConnectFault::Cleanup {
                cause: event::ConnectError::Connect(errno),
                error: attempt::AttemptError::Lookup(actual),
            })) => {
                assert_eq!(*errno, Errno::NO_BUFFER_SPACE);
                assert!((*actual) == (observed));
            }
            _ => panic!("both the connection cause and cleanup observation survive"),
        }
    }
}

#[test]
fn budget_is_final() {
    let cause = timeout::Phase::Head.expire(budget::Elapsed::Budget);
    let failure: Failure = Failure::new(Error::Timeout(cause), Safety::PossiblyCommitted);
    let retry = State::new(aggressive(), &Method::GET);
    let retry = retry
        .retry_after(&failure)
        .err()
        .expect("absolute budget exhausted");
    let failure = retry.finish(failure);
    assert!(matches!(failure.cause(), Error::Timeout(error) if *error == cause));
    assert_eq!(failure.attempt(), 1);
}

#[test]
fn reply_domain_is_final() {
    use dope_fiber::reply::{self, LaneIndex};

    let cause = reply::Error::Lane {
        index: LaneIndex::new::<1>(),
        capacity: NonZero::<u32>::MIN,
    };
    for method in [Method::GET, Method::POST] {
        let failure: Failure = Failure::new(
            Error::Connect(connect::Error::Reply(cause)),
            Safety::BeforeSubmit,
        );
        let retry = State::new(aggressive(), &method);
        let retry = retry.retry_after(&failure).err().expect("terminal failure");
        let failure = retry.finish(failure).generalize::<Errno>();
        assert!(matches!(failure.cause(),
            Error::Connect(connect::Error::Reply(observed)) if *observed == cause
        ));
    }
}

#[test]
fn pre_submit_can_retry() {
    let failure: Failure = Failure::new(Error::NotConnected, Safety::BeforeSubmit);
    let retry = State::new(Policy::default(), &Method::POST);

    assert!(failure.safety() == Safety::BeforeSubmit);
    let backoff = retry.retry_after(&failure).ok().expect("retry admitted");
    assert_eq!(backoff.delay(), Duration::from_millis(25));
    assert!(backoff.start().retry_after(&failure).is_err());
}

#[test]
fn connect_before_only() {
    for cause in [
        attempt::ConnectFault::Transport(event::ConnectError::Connect(Errno::NO_BUFFER_SPACE)),
        attempt::ConnectFault::ConnectTimeout,
    ] {
        let failure: Failure = Failure::new(
            Error::Connect(connect::Error::Attempt(cause)),
            Safety::BeforeSubmit,
        );
        for method in [Method::GET, Method::POST] {
            let retry = State::new(Policy::default(), &method);
            let backoff = retry.retry_after(&failure).ok().expect("retry admitted");
            assert_eq!(backoff.delay(), Duration::from_millis(25));
        }
        let after: Failure = Failure::new(
            Error::Connect(connect::Error::Attempt(cause)),
            Safety::PossiblyCommitted,
        );
        let retry = State::new(aggressive(), &Method::POST);
        let retry = retry.retry_after(&after).err().expect("terminal failure");
        let terminal = retry.finish(after);
        assert!(terminal.safety() == Safety::PossiblyCommitted);
        assert!(matches!(terminal.cause(), Error::Connect(_)));
    }
}

#[test]
fn idempotent_obeys_limit() {
    let failure: Failure = Failure::new(
        Error::Transport(conn::Error::Classified(conn::Kind::RemoteEof)),
        Safety::PossiblyCommitted,
    );
    for method in [
        Method::GET,
        Method::HEAD,
        Method::OPTIONS,
        Method::TRACE,
        Method::PUT,
        Method::DELETE,
    ] {
        let retry = State::new(Policy::default(), &method);
        let backoff = retry.retry_after(&failure).ok().expect("retry admitted");
        assert_eq!(backoff.delay(), Duration::from_millis(25));
        let retry = backoff.start();
        let backoff = retry.retry_after(&failure).ok().expect("retry admitted");
        assert_eq!(backoff.delay(), Duration::from_millis(50));
        assert!(backoff.start().retry_after(&failure).is_err());
    }
}

#[test]
fn last_attempt_is_terminal() {
    let failure = || -> Failure { Failure::new(Error::NotConnected, Safety::BeforeSubmit) };
    let policy = Policy {
        idempotent_attempts: NonZero::<u8>::MAX,
        ..Policy::default()
    };
    let mut retry = State::new(policy, &Method::GET);
    for attempt in 1..u8::MAX {
        assert_eq!(retry.finish(failure()).attempt(), attempt);
        retry = retry
            .retry_after(&failure())
            .ok()
            .expect("remaining attempt")
            .start();
    }
    let retry = retry.retry_after(&failure()).err().expect("attempt limit");
    assert_eq!(retry.finish(failure()).attempt(), u8::MAX);
}

#[test]
fn backoff_keeps_attempt() {
    let retry = State::new(Policy::default(), &Method::GET);
    let failure: Failure = Failure::new(Error::NotConnected, Safety::BeforeSubmit);
    let backoff = retry.retry_after(&failure).ok().expect("retry admitted");
    assert_eq!(backoff.delay(), Duration::from_millis(25));
    let expired: Failure = Failure::new(
        Error::Timeout(timeout::Phase::Backoff.expire(budget::Elapsed::Budget)),
        Safety::BeforeSubmit,
    );
    assert_eq!(backoff.finish(expired).attempt(), 1);
    assert_eq!(backoff.start().finish(failure).attempt(), 2);
}

#[test]
fn permanent_is_not_retried() {
    let failure: Failure = Failure::new(
        Error::Request(RequestError::CrossOrigin),
        Safety::PossiblyCommitted,
    );
    let retry = State::new(aggressive(), &Method::GET);

    let retry = retry.retry_after(&failure).err().expect("terminal failure");
    assert!(matches!(
        retry.finish(failure).cause(),
        Error::Request(RequestError::CrossOrigin)
    ));
}

#[test]
fn identity_is_terminal() {
    use sark_client::capacity;

    let failure: Failure = Failure::new(
        Error::Capacity(capacity::Error::IdentityExhausted),
        Safety::PossiblyCommitted,
    );
    let retry = State::new(aggressive(), &Method::GET);
    let retry = retry.retry_after(&failure).err().expect("terminal failure");
    assert!(matches!(
        retry.finish(failure).generalize::<Errno>().cause(),
        Error::Capacity(capacity::Error::IdentityExhausted)
    ));
}

#[test]
fn connect_ids_are_final() {
    use dope::manifold::client::port::error::Open;

    for method in [Method::GET, Method::POST] {
        for cause in [
            Exhaustion::Identity.into(),
            connect::Error::Activation(Open::Exhausted),
        ] {
            let active = matches!(cause, connect::Error::Activation(_));
            let failure: Failure = Failure::new(Error::Connect(cause), Safety::BeforeSubmit);
            let retry = State::new(aggressive(), &method);
            let retry = retry.retry_after(&failure).err().expect("terminal failure");
            let failure = retry.finish(failure).generalize::<Errno>();
            assert!(if active {
                matches!(
                    failure.cause(),
                    Error::Connect(connect::Error::Activation(Open::Exhausted))
                )
            } else {
                matches!(
                    failure.cause(),
                    Error::Connect(connect::Error::IdentityExhausted)
                )
            });
        }
    }
}

#[test]
fn submit_cause_is_kept() {
    use dope::core::driver::result::{SubmitCause, SubmitError};
    use dope::manifold::client::conn::Transmit;
    use o3::depot::arena::slab::Vacancy;

    for vacancy in [Vacancy::Full, Vacancy::IdentityExhausted] {
        let error = SubmitError::from(vacancy);
        let cause = error.cause();
        for error in [
            Error::Transport(conn::Error::Park(event::ParkError::Submit(error))),
            Error::Transport(conn::Error::transmit(Transmit::Rejected(
                event::SendFailure::Queued(error),
            ))),
            Error::Connect(Exhaustion::Submission(error).into()),
        ] {
            let failure: Failure = Failure::new(error, Safety::BeforeSubmit);
            let retry = State::new(aggressive(), &Method::GET);
            let result = retry.retry_after(&failure);
            assert_eq!(result.is_ok(), cause == SubmitCause::Full);
            let failure = match result {
                Ok(backoff) => backoff.finish(failure),
                Err(retry) => retry.finish(failure),
            }
            .generalize::<Errno>();
            let actual = match failure.cause() {
                Error::Transport(conn::Error::Park(event::ParkError::Submit(error)))
                | Error::Transport(conn::Error::Transmit {
                    error: Transmit::Rejected(event::SendFailure::Queued(error)),
                    ..
                })
                | Error::Connect(connect::Error::Attempt(attempt::ConnectFault::Transport(
                    event::ConnectError::Admission(error),
                ))) => Some(error.cause()),
                _ => None,
            };
            assert_eq!(actual, Some(cause));
        }
    }
}

#[test]
fn policy_cannot_replay() {
    let failure: Failure = Failure::new(
        Error::Transport(conn::Error::Classified(conn::Kind::RemoteEof)),
        Safety::PossiblyCommitted,
    );

    for method in [
        Method::POST,
        Method::PATCH,
        Method::from_bytes(b"CUSTOM-EXTENSION-METHOD").expect("extension method"),
    ] {
        let retry = State::new(aggressive(), &method);
        assert!(retry.retry_after(&failure).is_err());
    }
}

#[test]
fn local_stop_is_not_stale() {
    for local in [
        LocalOrigin::OperationCancelled,
        LocalOrigin::RuntimeShutdown,
        LocalOrigin::ApiRequest,
    ] {
        let failure: Failure = Failure::new(
            Error::Transport(conn::Error::Classified(conn::Kind::Local(local))),
            Safety::PossiblyCommitted,
        );
        let retry = State::new(aggressive(), &Method::GET);
        assert!(retry.retry_after(&failure).is_err());
    }
}

#[test]
fn failure_preserves_facts() {
    let timeout: Failure = Failure::new(
        Error::Timeout(timeout::Phase::Head.expire(budget::Elapsed::Budget)),
        Safety::PossiblyCommitted,
    );
    assert!(matches!(
        timeout.cause(),
        Error::Timeout(error)
            if error.phase() == timeout::Phase::Head
                && error.elapsed() == budget::Elapsed::Budget
    ));
    assert!(timeout.safety() == Safety::PossiblyCommitted);

    let protocol: Failure = Failure::new(
        Error::Protocol(protocol::Error::IncompleteHead),
        Safety::PossiblyCommitted,
    );
    let (cause, safety) = protocol.into_parts();
    assert!(matches!(
        cause,
        Error::Protocol(protocol::Error::IncompleteHead)
    ));
    assert!(safety == Safety::PossiblyCommitted);

    let (_, cost) = Tracker::usage(|| -> Failure {
        Failure::new(
            Error::Timeout(timeout::Phase::Head.expire(budget::Elapsed::Budget)),
            Safety::PossiblyCommitted,
        )
    });
    assert_eq!(cost, (0, 0));
    assert_eq!(
        size_of::<Failure>(),
        size_of::<Error>() + align_of::<Error>()
    );
}

#[test]
fn state_accumulates_safety() {
    assert_eq!(
        size_of::<State>(),
        4,
        "reconnect policy, mode, attempt, and selected limit must remain byte-packed",
    );
    let retry = State::new(Policy::default(), &Method::GET);
    let after: Failure = Failure::new(
        Error::Timeout(timeout::Phase::Head.expire(budget::Elapsed::Limit)),
        Safety::PossiblyCommitted,
    );
    let backoff = retry.retry_after(&after).ok().expect("retry admitted");
    assert_eq!(backoff.delay(), Duration::from_millis(25));

    let before: Failure = Failure::new(Error::NotConnected, Safety::BeforeSubmit);
    let terminal = backoff.finish(before);
    assert!(terminal.safety() == Safety::PossiblyCommitted);
    assert!(matches!(terminal.cause(), Error::NotConnected));
}

#[test]
fn mailbox_preserves_cause() {
    use std::{convert::Infallible, net::SocketAddr};

    use common::Mailbox;
    use dope::{
        core::{driver::life::creation::Attempt, io::socket::option},
        manifold::{client::attempt, timing::Throughput},
        runtime::executor::Executor,
    };
    use o3::depot::capacity;

    let plan = Mailbox::<()>::new(capacity::Count::new(1));
    let config = Needs::from_tree(&plan)
        .and_then(|needs| needs.resolve::<Throughput>())
        .expect("mailbox requirements");
    let mut retired = Vec::new();
    let mut create = Attempt::new();
    Executor::new(&mut create, config)
        .ok()
        .expect("test executor")
        .with_plan(plan)
        .run(|executor| {
            executor
                .try_create(
                    |session| {
                        use attempt::queue::mailbox;

                        let domain = session.storage();
                        let peer = SocketAddr::from(([127, 0, 0, 1], 0));
                        let first = domain.try_reserve().expect("first attempt reservation");
                        let first =
                            first.start(0, attempt::StreamTarget::new(peer, option::Set::EMPTY));
                        let receipt = first.receipt();
                        assert!(matches!(first.cancel(), mailbox::Cancellation::Pending));
                        let current = domain
                            .try_reserve()
                            .expect("replacement attempt reservation");
                        let current =
                            current.start(1, attempt::StreamTarget::new(peer, option::Set::EMPTY));
                        let (cause, ()) = (domain.try_settle(&receipt, ()))
                            .err()
                            .expect("late completion cannot settle the replacement");
                        for method in [Method::GET, Method::POST] {
                            let (failure, cost) = Tracker::usage(|| {
                                Failure::<Infallible, _>::new(
                                    Error::Connect(connect::Error::Mailbox(cause)),
                                    Safety::BeforeSubmit,
                                )
                                .generalize::<Errno>()
                            });
                            assert_eq!(cost, (0, 0));
                            let retry = State::new(aggressive(), &method);
                            let retry =
                                retry.retry_after(&failure).err().expect("terminal failure");
                            let (error, safety) = retry.finish(failure).into_parts();
                            assert!(safety == Safety::BeforeSubmit);
                            match error {
                                Error::Connect(connect::Error::Mailbox(observed)) => {
                                    assert!((observed) == (cause));
                                }
                                _ => panic!("mailbox failure must reach the caller intact"),
                            }
                        }
                        assert!(matches!(current.cancel(), mailbox::Cancellation::Pending));
                    },
                    recovery::Policy::new(
                        |event| retired.push(event),
                        |error: recovery::Recovery<'_, '_>| -> Infallible {
                            panic!("native recovery: {:?}", error.cause())
                        },
                    ),
                )
                .as_ref()
                .map_err(|error| error.cause().map_storage(|rejected| &rejected.cause))
                .expect("mailbox storage generation and finalization");
        });
    assert!(
        retired.is_empty(),
        "unexpected mailbox retirement: {retired:?}"
    );
}
