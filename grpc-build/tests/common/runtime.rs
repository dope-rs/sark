use std::{convert::Infallible, num::NonZero};

use dope::core::driver::{
    life::recovery,
    retained,
    route::{Epoch, KeyTag, indices::SlotIndex},
    settings,
};
use dope_test::scope::Driver;
use sark_core::manifold::{
    schedule,
    server::{
        app::{self, Application as _},
        drain,
    },
    stream::Boundary,
};
use sark_grpc::{
    Code, Status,
    metadata::Metadata,
    o3::buffer::resident,
    protocol::{Block, section},
    server::{
        Bank,
        config::Limits,
        mode,
        service::{handler::Handler, timers},
    },
};
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        DataOutcome,
        config::{Config, Validated},
        event::{self, Event},
        machine::Connection,
    },
};
use sark_http::field::metadata::section::rules;

type Server<'d> =
    Connection<ServerRole, section::Plan, resident::Shared<'d>, rules::RejectPush, event::Ignore>;

pub fn execute<H: Handler<Timer = timers::Off>>(handler: H, cases: &[(&[u8], Code)]) {
    let plan = Validated::<ServerRole>::new(Config {
        stream_capacity: 1,
        ..Config::default()
    })
    .expect("server plan");
    let bank = Bank::try_new(
        NonZero::<u32>::MIN,
        Limits::default().try_calls(1).expect("one call"),
    )
    .expect("readiness bank");
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
            scope.with_driver(|context, mut scheduler| {
                let budget = context
                    .storage
                    .allocator()
                    .try_budget(plan.inbound_capacity())
                    .expect("resident input budget");
                let driver = context.driver_ref();
                let route = driver
                    .targets::<KeyTag<0>>()
                    .bind(SlotIndex::ZERO, Epoch::INITIAL)
                    .dispatch();
                let ready = driver
                    .ready()
                    .make_slot(route)
                    .expect("application readiness");
                let owner = retained::raw::Owner::from_driver(driver);
                let mut context = retained::Context::new(context, owner);
                let mut application =
                    <mode::Direct as mode::Build<'_, '_, H, Server<'_>, ()>>::build(
                        mode::Direct,
                        handler,
                        &bank,
                    )
                    .expect("direct application");
                for &(path, expected) in cases {
                    let mut server = Server::from_plan(plan).expect("server");
                    server.ingress().try_bind(budget).expect("bind input owner");
                    let mut state = app::Application::<Server<'_>, ()>::state(
                        &application,
                        app::Capacity::new(plan.stream_count(), plan.data_events().raw()),
                    )
                    .expect("application state");
                    let mut client = Connection::<ClientRole>::try_new().expect("client");
                    let request =
                        Block::request(path, None, &Metadata::new()).expect("request fields");
                    let boundary = if expected == Code::Ok {
                        Boundary::More
                    } else {
                        Boundary::End
                    };
                    let stream = client
                        .streams()
                        .headers()
                        .request(request, boundary)
                        .expect("request");
                    let mut sent = false;
                    let mut status = None;
                    let mut bytes = Vec::new();
                    for _ in 0..32 {
                        while !client.output().is_empty() {
                            let wire = client.output().first();
                            let count = wire.len();
                            assert_ne!(count, 0, "client request is buffered");
                            server.ingress().ingest(wire).expect("request ingress");
                            client.transfer().drain(count);
                        }
                        let active = scheduler.begin();
                        let mut turn =
                            app::Turn::new((), active.turn().app(), ready.target(), &mut context);
                        let result = application.drive(
                            &mut state,
                            &mut server,
                            &mut turn,
                            schedule::Egress::Released,
                            |error| panic!("application report: {error:?}"),
                        );
                        if let Some(error) = drain::Progress::error(result, ready.target()) {
                            panic!("generated dispatch: {error:?}");
                        }
                        let mut prefix = [0; 4096];
                        loop {
                            match server
                                .transfer()
                                .take_plain(&mut prefix)
                                .expect("response ownership")
                            {
                                event::Write::Buffered(0) => break,
                                event::Write::Buffered(len) => client
                                    .ingress()
                                    .ingest(&prefix[..len])
                                    .expect("response ingress"),
                                event::Write::Owned {
                                    prefix_len, body, ..
                                }
                                | event::Write::Split {
                                    prefix_len, body, ..
                                } => {
                                    client
                                        .ingress()
                                        .ingest(&prefix[..prefix_len])
                                        .expect("response prefix");
                                    client
                                        .ingress()
                                        .ingest(body.as_slice())
                                        .expect("response body");
                                }
                            }
                        }
                        while let Some(event) = client.ingress().poll_event() {
                            match event {
                                Event::Headers {
                                    stream_id,
                                    head,
                                    boundary,
                                    ..
                                } => {
                                    assert_eq!(stream_id.id(), stream);
                                    if boundary == Boundary::End {
                                        let result = Status::from_fields(&head.into_parts().0)
                                            .expect("gRPC status");
                                        assert!(
                                            status.replace(result.code()).is_none(),
                                            "one terminal status"
                                        );
                                    }
                                }
                                Event::Data {
                                    stream_id, data, ..
                                } => {
                                    assert_eq!(stream_id.id(), stream);
                                    data.for_each(|part| bytes.extend_from_slice(part));
                                }
                                Event::PrefaceComplete
                                | Event::SettingsApplied
                                | Event::SettingsAck => {}
                                event => panic!(
                                    "unexpected response: {:?}",
                                    std::mem::discriminant(&event)
                                ),
                            }
                        }
                        if status.is_some() {
                            break;
                        }
                        if expected == Code::Ok && !sent {
                            let outcome = client
                                .streams()
                                .try_send(stream, &[0; 5], Boundary::End)
                                .expect("request message");
                            assert!(
                                matches!(outcome, DataOutcome::Sent(count) if count.get() == 5)
                            );
                            sent = true;
                        }
                    }
                    application.close(&mut state, &mut server, ());
                    assert_eq!(status, Some(expected), "path {path:?}");
                    assert_eq!(
                        bytes.as_slice(),
                        if expected == Code::Ok {
                            [0; 5].as_slice()
                        } else {
                            &[]
                        }
                    );
                }
                assert_eq!(budget.resident(), 0, "all request owners released");
            })
        },
        recovery::Policy::new(
            |event| panic!("unexpected native event: {event:?}"),
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("driver recovery: {:?}", failure.cause())
            },
        ),
        |result| match result {
            Ok(()) => {}
            Err(error) => panic!("driver scope: {:?}", error.cause()),
        },
    );
}
