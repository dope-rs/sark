pub mod common;

use std::convert::Infallible;

use common::server::h3::{proof::request::Spec, sessions::Scenario};
use dope::core::driver::life::recovery::Recovery;
use sark_core::{life::session::Disabled, manifold::stream::Boundary};
use sark_h3::manifold::server::deadline;

#[test]
fn head_response_paths() {
    let mut instant = Spec::new(b"/body", b"");
    instant.head.method = b"HEAD";
    let mut sync = Spec::new(b"/body", b"sync-body");
    sync.head.method = b"HEAD";
    sync.data.split = Some(4);
    let mut task = Spec::new(b"/async/raw", b"task-body");
    task.head.method = b"HEAD";
    let mut guard = Spec::new(b"/guarded", b"unread");
    guard.head.method = b"HEAD";
    guard.data.boundary = Boundary::More;
    guard.expect.status = b"401";
    guard.expect.input_finished = false;
    Scenario {
        name: "head-response-paths",
        requests: vec![instant, sync, task, guard],
        expected_bodies: vec![Some(b""), Some(b""), Some(b""), Some(b"")],
        task_capacity: 2,
        cancellation: None,
        must_finish_before_last: None,
        linked: 0,
    }
    .try_run_with(
        Default::default(),
        Disabled,
        deadline::Disabled,
        |error: Recovery<'_, '_>| -> Infallible { panic!("{:?}", error.cause()) },
        |cause| eprintln!("lifecycle diagnostic: {cause:?}"),
        |outcome| outcome.expect("HEAD sync, task and guard responses"),
    );
}
