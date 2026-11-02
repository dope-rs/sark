pub mod common;
use std::cell::Cell;

use common::recipe::{Lexical, ROUTE};
use dope::{
    core::driver::{route, settings},
    manifold::{self, network::listener::handler::failures, timing},
};
use sark_h2::server::{
    runtime::modes,
    service::{Handler, Route},
};

#[test]
fn plan_keeps_borrow() {
    let calls = Cell::new(0);
    let layout = {
        let handler = Lexical(&calls).with_bound::<0>();
        let plan = (Lexical::try_plan()).expect("valid H2 lifetime plan");
        let prepared = (plan
            .with_plain::<_, modes::Sync>((Route::new(handler), failures::Close))
            .for_install::<timing::Throughput>())
        .expect("fixed receive sizing must satisfy the prepared topology");
        assert_eq!(calls.get(), 0);
        prepared.layout()
    };
    assert!(
        (layout.queue_layout()) == (<manifold::timing::Throughput as settings::Profile>::QUEUES)
    );
}

#[test]
fn duplicate_route_fails() {
    let first = Lexical::try_plan().expect("first H2 lifetime plan");
    let other = Lexical::try_plan().expect("second H2 lifetime plan");
    assert!(matches!(
        settings::claim::Needs::from_tree(&(first, other)),
        Err(settings::errors::Claim::DuplicateRoute(id)) if id == route::Id::fixed::<ROUTE>()
    ));
}
