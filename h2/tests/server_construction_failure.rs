pub mod common;

use std::{cell::Cell, convert::Infallible, pin::Pin};

use common::recipe::Lexical;
use dope::{
    core::driver::{route::KeyTag, settings},
    manifold::network::listener::handler::Application,
    net::wire,
};
use o3::alloc;
use sark_h2::server::{
    self,
    runtime::{self, app, modes},
    service::Route,
};

#[test]
fn reports_construction() {
    let calls = Cell::new(0);
    let observed = Cell::new(false);
    let report = app::Report::new(
        |failure: app::Failure<
            '_,
            '_,
            '_,
            wire::Identity,
            KeyTag<0>,
            Infallible,
            runtime::Build<alloc::Error>,
        >| {
            if let app::Failure::Construction(cause) = failure {
                assert_eq!(cause, runtime::Build::App(alloc::Error::Overflow));
                observed.set(true);
            }
        },
    );
    let plan = server::Plan::new(
        server::Config {
            max_request_body_bytes: 0,
            max_request_body_chunks: 0,
            max_connection_body_bytes: 0,
            max_connection_body_chunks: 0,
            max_outbound_bytes: 4096,
        },
        settings::Receive::fixed::<2, 4096>(),
    )
    .expect("server plan");
    let mut app = <_ as modes::Direct<modes::Sync, wire::Identity, KeyTag<0>>>::build(
        (Route::new(Lexical(&calls)), report),
        plan,
    )
    .expect("application admission");
    Application::<KeyTag<0>>::build_failed(
        Pin::new(&mut app),
        runtime::Build::App(alloc::Error::Overflow),
    );
    assert!(observed.get());
}
