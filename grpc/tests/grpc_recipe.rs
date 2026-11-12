pub mod common;

use std::cell::Cell;

use dope::core::driver::route::table::conn;
use dope_test::alloc::Tracker;
use sark_core::manifold::schedule;
use sark_grpc::server::{Bank, config::Limits, mode, runtime::state};
use sark_h2::{conn::event, server::app};

use crate::common::grpc::call::borrowed::Borrowed;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn reserves_once() {
    let calls = Cell::new(0);
    let bank = Bank::try_new(
        conn::Capacity::fixed::<1>().count(),
        Limits::default().try_calls(1).expect("one call"),
    )
    .expect("one readiness resident");
    let app = mode::Direct.build(Borrowed(&calls), &bank);
    check_reservations(app);
}

#[test]
fn drops_prefix() {
    use o3::alloc;

    let cap = conn::Capacity::fixed::<4>().count();
    let limits = Limits::default().try_calls(1).expect("one call");
    let (result, cold) = Tracker::measure(|| Bank::try_new(cap, limits));
    let bank = result.expect("readiness residents");
    assert_eq!(cold[0], 5);
    assert_eq!(cold[2], 0);
    let ((), freed) = Tracker::measure(|| drop(bank));
    assert_eq!(freed, [0, 0, cold[0], cold[1]]);

    for fail in 0..cold[0] {
        let (result, usage) =
            Tracker::measure(|| Tracker::fail_after(fail, || Bank::try_new(cap, limits)));
        assert_eq!(
            result.err().expect("readiness allocation failure"),
            alloc::Error::Exhausted
        );
        assert_eq!(usage[0], fail);
        assert_eq!(usage[2], fail);
        assert_eq!(usage[1], usage[3]);
    }
}

fn check_reservations<'d, A>(app: A)
where
    A: app::Application<'d, event::Ignore, (), Build = state::Error>,
{
    use app::Capacity;
    use o3::depot::capacity::Count;
    use state::Error;

    let (result, usage) = Tracker::measure(|| app.state(Capacity::new(Count::new(2), 0)));
    let mismatch = result.err().expect("foreign call extent");
    assert!(matches!(mismatch, Error::Extent { requested, capacity: 1 } if requested.get() == 2));
    assert_eq!(usage, [0; 4]);
    let (first, usage) = Tracker::measure(|| app.state(Capacity::new(Count::new(1), 0)));
    let first = first.expect("rejected reservation returns to its bank");
    assert_eq!(usage, [0; 4]);
    let (result, usage) = Tracker::measure(|| app.state(Capacity::new(Count::new(1), 0)));
    let full = result.err().expect("resident remains reserved");
    assert!(matches!(full, Error::Connections { capacity } if capacity.get() == 1));
    assert_eq!(usage, [0; 4]);
    drop(first);
    let reused = app
        .state(Capacity::new(Count::new(1), 0))
        .expect("same bank accepts the returned reservation");
    assert!(matches!(A::activity(&reused), schedule::Quiescence::Quiet));
}

#[test]
fn protocol_failure_stops_before_finish() {
    let calls = Cell::new(0);
    let bank = Bank::try_new(
        conn::Capacity::fixed::<1>().count(),
        Limits::default().try_calls(1).expect("one call"),
    )
    .expect("bank");
    let error = first_event_failure(mode::Direct.build(Borrowed(&calls), &bank), None);
    assert!(matches!(
        error,
        app::Error::Connection(sark_h2::conn::Error::CriticalDeadline)
    ));
    assert_eq!(calls.get(), 0);
}

#[test]
fn first_head_preserves_deferred_failure_and_lease() {
    use common::grpc::wire::deadline::{Deferred, Direct, Facts};
    use sark_grpc::server::runtime::run::Failure;

    let facts = Facts::default();
    let bank = Bank::try_new(
        conn::Capacity::fixed::<1>().count(),
        Limits::default().try_calls(1).expect("one call"),
    )
    .expect("bank");
    let error = first_event_failure(mode::Direct.build(Direct(&facts), &bank), Some(1));
    assert!(
        matches!(
            error,
            app::Error::Application(Failure::Build(state::Error::Storage(
                o3::alloc::Error::Exhausted
            )))
        ),
        "{error:?}"
    );
    let error = first_event_failure(mode::Direct.build(Deferred(Direct(&facts)), &bank), Some(0));
    assert!(
        matches!(error, app::Error::Application(Failure::Build(
        state::Error::Deadlines(o3::depot::heap::Error::Allocation { capacity, error: o3::alloc::Exhausted, .. })
    )) if capacity.raw() == 1),
        "{error:?}"
    );
    assert_eq!(facts.starts.get(), 0);
    assert_eq!(facts.drops.get(), 0);
}

fn first_event_failure<A>(
    mut service: A,
    allocation: Option<usize>,
) -> app::Error<sark_grpc::server::runtime::run::Failure>
where
    for<'d> A: app::Application<
            'd,
            event::Retirement,
            (),
            Build = state::Error,
            Failure = sark_grpc::server::runtime::run::Failure,
            Fields = sark_grpc::protocol::section::Plan,
            Egress<'d> = o3::buffer::resident::bound::Unbounded,
        >,
{
    use common::grpc::session::fairness::{CHECK, Fairness};
    use dope::core::driver::{
        life::recovery,
        route::{Epoch, KeyTag, indices::SlotIndex},
        settings,
    };
    use dope_test::scope::Driver;
    use o3::buffer::resident;
    use sark_core::manifold::{
        schedule,
        server::Event,
        stream::{Boundary, Section},
    };
    use sark_grpc::protocol::section;
    use sark_h2::server::app;
    use sark_h2::{
        ClientRole, ServerRole,
        conn::{Error, config, machine::Connection},
    };

    let config = settings::Config::new(
        settings::QueueLayout::fixed::<32, 32>(),
        settings::Schedule::fixed::<1, 0>(),
        settings::FileSlots::fixed::<0, 1>(),
        settings::Receive::fixed::<2, 4096>(),
        settings::Domain::ONE,
        settings::Progress::Prompt,
    );
    Driver::new(config).with_scope(
        |scope| {
            let (_, mut context, mut scheduler) = scope.split();
            let plan = config::Validated::<ServerRole>::new(config::Config {
                stream_capacity: 1,
                ..Default::default()
            })
            .expect("one stream");
            let budget = context
                .storage
                .allocator()
                .try_budget(plan.inbound_capacity())
                .expect("retained ingress");
            let driver = context.driver_ref();
            let ready = driver
                .ready()
                .make_slot(
                    driver
                        .targets::<KeyTag<0>>()
                        .bind(SlotIndex::ZERO, Epoch::INITIAL)
                        .dispatch(),
                )
                .expect("ready target");
            let mut server =
                Connection::<ServerRole, section::Plan, resident::Shared<'_>>::from_plan(plan)
                    .expect("server");
            server
                .ingress()
                .try_bind(budget)
                .expect("ingress ownership");
            let mut state = app::Application::<sark_h2::conn::event::Retirement, ()>::state(
                &service,
                app::Capacity::new(plan.stream_count(), plan.data_events().raw()),
            )
            .expect("service state");
            let mut client = Connection::<ClientRole>::try_new().expect("client");
            client
                .streams()
                .headers()
                .request(
                    Fairness::try_headers(CHECK).expect("headers"),
                    Boundary::End,
                )
                .expect("request");
            while !client.output().is_empty() {
                let bytes = client.output().first();
                let count = bytes.len();
                server.ingress().ingest(bytes).expect("wire ingress");
                client.transfer().drain(count);
            }
            let active = scheduler.begin();
            let mut turn = app::Turn::new((), active.turn().app(), ready.target(), &mut context);
            let mut consumed = 0;
            let mut finished = false;
            let (result, usage) = Tracker::measure(|| {
                let mut drive = || {
                    app::Application::drive(
                        &mut service,
                        &mut state,
                        &mut server,
                        &mut turn,
                        schedule::Egress::Unchanged,
                        &mut (),
                        |_, conn, drain, selection| {
                            assert!(selection.is_none());
                            let event = drain.next(conn);
                            if allocation.is_none() {
                                assert!(event.is_some());
                                consumed += 1;
                                return Err(Error::CriticalDeadline);
                            }
                            if matches!(
                                event,
                                Some(Event::Head {
                                    section: Section::Initial,
                                    ..
                                })
                            ) {
                                consumed += 1;
                            }
                            Ok(event)
                        },
                        |_, _| {},
                        |_, _| finished = true,
                        |_, error| panic!("unexpected report: {error:?}"),
                    )
                };
                match allocation {
                    Some(count) => Tracker::fail_after(count, drive),
                    None => drive(),
                }
            });
            let error = result.err().expect("first event failure");
            if let Some(count) = allocation {
                assert_eq!(usage[0], count);
            }
            assert_eq!(consumed, 1);
            assert!(!finished);
            let full = app::Application::<event::Retirement, ()>::state(
                &service,
                app::Capacity::new(o3::depot::capacity::Count::new(2), 0),
            )
            .err()
            .expect("failed first event retains lease");
            assert!(matches!(full, state::Error::Connections { capacity } if capacity.get() == 1));
            app::Application::close(&mut service, &mut state, &mut server, ());
            let capacity = app::Capacity::new(plan.stream_count(), plan.data_events().raw());
            assert!(matches!(
                app::Application::<event::Retirement, ()>::state(&service, capacity),
                Err(state::Error::Connections { .. }),
            ));
            drop(state);
            let reused = app::Application::<event::Retirement, ()>::state(&service, capacity)
                .expect("cleanup returns lease once");
            assert!(matches!(A::activity(&reused), schedule::Quiescence::Quiet));
            error
        },
        recovery::Policy::new(
            |event| panic!("unexpected native event: {event:?}"),
            common::recover,
        ),
        |result| match result {
            Ok(error) => error,
            Err(error) => panic!("driver scope: {:?}", error.cause()),
        },
    )
}
