pub mod common;

use common::retry::aggressive;
use dope_fiber::task::time::budget;
use sark_client::{Error, Failure, Safety, protocol::retry::State, timeout};
use sark_http::method::Method;

#[test]
fn dispatch_limit_is_final() {
    let cause = timeout::Phase::Dispatch.expire(budget::Elapsed::Limit);
    for method in [Method::GET, Method::POST] {
        for prior in [Safety::BeforeSubmit, Safety::PossiblyCommitted] {
            let stale: Failure = Failure::new(Error::NotConnected, Safety::BeforeSubmit);
            let retry = State::new(aggressive(), &method)
                .retry_after(&stale)
                .ok()
                .expect("second dispatch admitted")
                .start()
                .with_prior(prior);
            let failure: Failure = Failure::new(Error::Timeout(cause), Safety::BeforeSubmit);
            let retry = retry
                .retry_after(&failure)
                .err()
                .expect("dispatch limit exhausted");
            let failure = retry.finish(failure);
            assert!(matches!(failure.cause(), Error::Timeout(error) if *error == cause));
            assert_eq!(failure.attempt(), 2);
            assert_eq!(failure.safety(), prior);
        }
    }
}
