use std::{convert::Infallible, mem, time};

use dope::{
    core::{
        driver::{
            life::{creation::Attempt, recovery},
            route::KeyTag,
            settings,
        },
        io::Reclaimed,
    },
    manifold::{service::health, timing::Balanced},
    net::link::egress,
    runtime::executor::{Executor, session},
};
use dope_test::scope::Scoped;
use sark_ws::client::{
    self, Config, Error, Layout,
    channel::{Factory, PlanError, Port},
};

#[test]
fn handles_are_one_pointer() {
    assert_eq!(
        mem::size_of::<client::Handle<'static, 'static, 'static, 'static, ()>>(),
        mem::size_of::<&'static ()>()
    );
    assert_eq!(
        mem::size_of::<(client::Logic<()>, &())>(),
        mem::size_of::<&()>()
    );
}

#[test]
fn ws_storage_layout() {
    use dope::manifold::network::client::topology::plan;
    let mut retired = Vec::new();
    {
        let config = Config::new("127.0.0.1", "/");
        let config = config.expect("WebSocket configuration");
        let plan = Port::factory(config.message(1).outbound(1), 1, 1);
        let plan = plan.expect("WebSocket factory");
        let mut attempt = Attempt::new();
        let executor = Executor::new(&mut attempt, settings::Config::for_profile::<Balanced>());
        executor
            .as_ref()
            .map_err(|error| error.kind())
            .expect("WebSocket executor");
        let executor = executor.ok().expect("checked WebSocket executor");
        executor.with_plan(plan).run(|executor| {
            let result = executor.try_enter(
                Scoped::new(|scope: session::Scope<'_, '_, '_, '_, Factory>| {
                    assert!(matches!(
                        scope
                            .storage()
                            .plan(time::Duration::ZERO, time::Duration::ZERO),
                        Err(PlanError::Backoff(health::Error::Base))
                    ));
                    let base = time::Duration::from_millis(1);
                    let max = time::Duration::from_millis(2);
                    let actual = scope.storage().plan(base, max);
                    let expected = plan::Plan::<KeyTag<0>, { u32::MAX as usize }>::new(
                        1,
                        plan::Spec::new(base, max)
                            .expect("valid backoff")
                            .with_egress(egress::Config::shared(egress::Credit::new(
                                egress::Entries::new(2),
                                egress::Resident::new(16_523),
                            ))),
                    );
                    let actual = actual.expect("installed WebSocket plan");
                    let expected = expected.expect("expected WebSocket plan");
                    assert!(
                        actual == expected,
                        "the installed WebSocket port retains its exact egress plan"
                    );
                }),
                recovery::Policy::new(
                    |event| retired.push(event),
                    |failure: recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("native WebSocket recovery: {:?}", failure.cause())
                    },
                ),
            );
            result
                .as_ref()
                .map_err(|error| error.cause().map_storage(|rejected| &rejected.cause))
                .expect("WebSocket installation");
        });
    };
    assert!(
        retired
            .iter()
            .all(|event| matches!(event, Reclaimed::Shutdown)),
        "unexpected reports client completion: {retired:?}"
    );
}

#[test]
fn ws_storage_bounds() {
    let config = Config::new("127.0.0.1", "/");
    let config = config.expect("WebSocket configuration");
    assert!(matches!(
        Port::factory(config, 0, 1),
        Err(Error::Layout(Layout::EntryZero { lanes: 0, each: 1 }))
    ));
    let config = Config::new("127.0.0.1", "/");
    let config = config.expect("WebSocket configuration");
    assert!(Port::factory(config.outbound(usize::MAX), 1, 1).is_ok());
    let config = Config::new("127.0.0.1", "/");
    let config = config.expect("WebSocket configuration");
    assert!(matches!(
        Port::factory(config.message(usize::MAX).outbound(usize::MAX), 1, 1),
        Err(Error::Layout(Layout::Resident {
            base: usize::MAX,
            added: 14,
        }))
    ));
}
