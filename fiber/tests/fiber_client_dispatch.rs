pub mod common;

use std::time::Duration;

use common::client::recover;
use common::client::{
    config::FLAVORS,
    retry::AGGRESSIVE,
    runtime::{cases::request::Request, peer},
};
use dope::core::{driver::life::recovery, io::Reclaimed, platform::wake};
use dope_fiber::task::time::budget;
use dope_test::harness::{GUARD, peer::AcceptScript};
use sark_client::{Error, Safety, timeout};
use sark_fiber::client::config::{Config, Stage};
use sark_http::method::Method;

#[test]
fn dispatch_before_submit() {
    let (quiesced, release) = wake::Ends::blocking()
        .expect("dispatch observation gate")
        .split();
    AcceptScript::with(
        move |acceptor| {
            quiesced
                .wait_timeout(GUARD)
                .expect("dispatch drivers quiesce");
            peer::empty(acceptor)
        },
        |server| {
            for flavor in FLAVORS {
                let addr = server.addr().to_string();
                let config = Config::try_new(&addr)
                    .expect("HTTP host allocation")
                    .retry(AGGRESSIVE)
                    .timeout(Duration::from_secs(3))
                    .limit(Stage::Dispatch, Duration::ZERO);
                Request::new(flavor, &addr, config, Method::GET, "/", &[], &[])
                    .try_run(
                        |outcome| {
                            let failure = outcome.err().expect("dispatch limit");
                            assert!(matches!(failure.cause(), Error::Timeout(error)
                                if error.phase() == timeout::Phase::Dispatch
                                    && error.elapsed() == budget::Elapsed::Limit));
                            assert_eq!(failure.attempt(), 1);
                            assert_eq!(failure.safety(), Safety::BeforeSubmit);
                        },
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("dispatch request execution");
            }
            release.notify().expect("dispatch observation release");
            server
                .finish()
                .expect("peer thread")
                .expect("no request bytes published");
        },
    )
    .expect("dispatch peer");
}
