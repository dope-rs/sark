pub mod common;

use std::{pin, rc::Rc};

use common::server::h3::{
    apps::SrvApp,
    observer::Probe,
    proof::{Failure, LISTENER, request::Spec},
    sessions::Scenario,
};
use common::server::recover;
use dope::{
    core::{
        driver::{life::recovery, result::SubmitError, route::KeyTag},
        io::datagram::ReceiveError,
    },
    manifold::{app::dispatch::raw::Projection as _, network::datagram::hook::ingress},
    net::wire,
};
use dope_fiber::task::storage::scope::Brand;
use dope_quic::runtime::mux::app::Silent;
use o3::depot::arena::slab::Vacancy;
use sark_core::life::session::Disabled;
use sark_h3::manifold::server::{self, deadline, failure::Quiet};

#[test]
fn retains_terminal_cause() {
    let mut scenario = Scenario {
        name: "quiet-native-receive",
        requests: vec![Spec::new(b"/async/quick", b"")],
        expected_bodies: vec![Some(b"quick")],
        task_capacity: 1,
        cancellation: None,
        must_finish_before_last: None,
        linked: 0,
    };
    let (prepared, proof) = scenario
        .try_prepare(Default::default(), None)
        .expect("server plan");
    let mut reports = Vec::new();
    prepared.enter_with(
        (),
        |scope| {
            scope.lend(|topology, stage| {
                drop(topology.reservation);
                Brand::with(|brand| {
                    let router = SrvApp::multiplexed::<wire::Identity, KeyTag<LISTENER>, _>(
                        brand,
                        stage.storage(),
                        sark_fiber::Config::new(1),
                    );
                    let mut driver = stage.driver();
                    let endpoint = topology
                        .plan
                        .try_with::<Silent, _, _, _, _, _>(
                            router,
                            Quiet::new(Probe(Rc::clone(&proof), |_: &dyn std::fmt::Debug| {})),
                            Disabled,
                            deadline::Disabled,
                            server::Tasks::new(),
                            &mut driver,
                        )
                        .expect("quiet server binding");
                    let mut endpoint = pin::pin!(endpoint);
                    let now = driver.driver_ref().clock().now();
                    let full = ReceiveError::Submit(SubmitError::from(Vacancy::Full));
                    <ingress::Coupled as ingress::Failure<'_, KeyTag<LISTENER>, _>>::failed(
                        endpoint.as_mut().inner_mut(),
                        full,
                        now,
                        driver.state_token(),
                    );
                    assert!(proof.failures.values.take().is_empty());
                    let terminal =
                        ReceiveError::Submit(SubmitError::from(Vacancy::IdentityExhausted));
                    <ingress::Coupled as ingress::Failure<'_, KeyTag<LISTENER>, _>>::failed(
                        endpoint.as_mut().inner_mut(),
                        terminal,
                        now,
                        driver.state_token(),
                    );
                    assert!(matches!(proof.failures.values.take().as_slice(),
                        [Failure::Socket(error)] if *error == terminal));
                });
            });
        },
        recovery::Policy::new(|event| reports.push(event), recover),
        |outcome| {
            assert!(
                outcome.is_ok(),
                "driver retirement: {:?}",
                outcome.as_ref().err().map(|error| error.cause()),
            );
        },
    );
    assert!(reports.is_empty(), "unexpected retirement: {reports:?}");
}
