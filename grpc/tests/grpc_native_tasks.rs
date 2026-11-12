pub mod common;

use std::convert::Infallible;

use common::grpc::{
    native::{DueFacts, DueHandler, Facts, Handler},
    settings::Settings,
    wire::{Peer, deadline},
};
use dope::{
    core::driver::{
        life::recovery,
        route::{FrameworkTag, KeyTag, Tag, kind},
        storage,
    },
    manifold::timing,
    runtime::{
        executor::session::Completion,
        topology::{lending, recipe},
    },
};
use dope_test::{harness::server::Pair, reclaim::Reports};
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Code,
    metadata::Metadata,
    protocol::request::Fields,
    server::{
        Bank, Plan,
        config::Limits,
        mode::{Demand, StreamTag, UnaryTag},
        service::task::stream::Shape,
    },
};
use sark_h2::conn::{DataOutcome, config};

#[test]
fn unary_yields_to_complete() {
    Pair::bind()
        .expect("gRPC listener")
        .run(
            |source, listener| {
                let facts = Facts::default();
                let limits = Limits::default().try_calls(1).expect("one call");
                let plan = Settings::new(listener.local_addr(), limits)
                    .h2::<KeyTag<0>>()
                    .expect("H2 plan");
                let bank = Bank::try_new(plan.connections().count(), limits).expect("gRPC bank");
                let prepared = plan
                    .with_bound(listener)
                    .with_server(
                        storage::Value::new(Handler(&facts)),
                        Plan::new(
                            recipe::Borrow,
                            &bank,
                            Demand::<UnaryTag>::try_new(1).expect("one unary task"),
                        ),
                    )
                    .for_install::<timing::Throughput>()
                    .expect("gRPC topology");
                let mut reports =
                    Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                let mut finish = Completion::new();
                prepared.enter_with(
                    source,
                    |worker| {
                        worker
                            .with(lending::Clean(&mut finish))
                            .expect("unary installation")
                            .expect("unary execution");
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("driver");
                    },
                );
                assert_eq!(facts.polls.get(), 2);
                assert_eq!(facts.messages.get(), 1);
                assert_eq!(facts.drops.get(), 1);
                assert!(reports.into_events().expect("retirement").is_empty());
                Ok::<(), Infallible>(())
            },
            |address| {
                let mut peer =
                    Peer::try_config(address, config::Config::default()).expect("gRPC peer");
                let metadata = Metadata::new();
                let fields = Fields::new(b"/test/Echo", Some(b"localhost"), &metadata)
                    .expect("gRPC request");
                let stream = peer
                    .client
                    .streams()
                    .headers()
                    .request(fields, Boundary::More)
                    .expect("request headers");
                let sent = peer
                    .client
                    .streams()
                    .try_send(stream, &[0; 5], Boundary::End)
                    .expect("empty protobuf request");
                assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == 5));
                let replies = deadline::try_replies(&mut peer, &[stream]).expect("unary response");
                assert_eq!(replies[0].body, [0; 5]);
                assert_eq!(replies[0].code, Some(Code::Ok));
            },
        )
        .expect("unary runtime");
}

#[test]
fn streaming_finishes() {
    for shape in [Shape::Unary, Shape::Client, Shape::Server, Shape::Bidi] {
        Pair::bind()
            .expect("gRPC listener")
            .run(
                move |source, listener| {
                    let facts = Facts {
                        shape: Some(shape),
                        ..Facts::default()
                    };
                    let limits = Limits::default().try_calls(1).expect("one call");
                    let plan = Settings::new(listener.local_addr(), limits)
                        .h2::<KeyTag<0>>()
                        .expect("H2 plan");
                    let bank =
                        Bank::try_new(plan.connections().count(), limits).expect("gRPC bank");
                    let prepared = plan
                        .with_bound(listener)
                        .with_server(
                            storage::Value::new(Handler(&facts)),
                            Plan::new(
                                recipe::Borrow,
                                &bank,
                                Demand::<StreamTag>::try_new(1).expect("one stream task"),
                            ),
                        )
                        .for_install::<timing::Throughput>()
                        .expect("gRPC topology");
                    let mut reports =
                        Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                    let mut finish = Completion::new();
                    prepared.enter_with(
                        source,
                        |worker| {
                            worker
                                .with(lending::Clean(&mut finish))
                                .expect("stream installation")
                                .expect("stream execution");
                        },
                        recovery::Policy::new(|event| reports.push(event), common::recover),
                        |result| {
                            result
                                .as_ref()
                                .map_err(|error| error.cause())
                                .expect("driver");
                        },
                    );
                    assert!(facts.polls.get() >= 2);
                    assert_eq!(facts.messages.get(), 1);
                    assert!(facts.closed.get());
                    assert_eq!(facts.drops.get(), 2);
                    assert!(reports.into_events().expect("retirement").is_empty());
                    Ok::<(), Infallible>(())
                },
                |address| {
                    let mut peer =
                        Peer::try_config(address, config::Config::default()).expect("gRPC peer");
                    for (data, code) in [([1, 0, 0, 0, 0], Code::Internal), ([0; 5], Code::Ok)] {
                        let metadata = Metadata::new();
                        let fields = Fields::new(b"/test/Echo", Some(b"localhost"), &metadata)
                            .expect("gRPC request");
                        let stream = peer
                            .client
                            .streams()
                            .headers()
                            .request(fields, Boundary::More)
                            .expect("request headers");
                        let sent = peer
                            .client
                            .streams()
                            .try_send(stream, &data, Boundary::End)
                            .expect("protobuf request");
                        assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == 5));
                        let replies =
                            deadline::try_replies(&mut peer, &[stream]).expect("stream response");
                        assert_eq!(
                            replies[0].body,
                            if code == Code::Ok { &data[..] } else { &[] }
                        );
                        assert_eq!(replies[0].code, Some(code));
                    }
                },
            )
            .expect("stream runtime");
    }
}

#[test]
fn unary_expiry() {
    Pair::bind()
        .expect("deadline listener")
        .run(
            |source, listener| {
                let facts = DueFacts::default();
                let limits = Limits::default()
                    .try_calls(1)
                    .expect("one call per connection");
                let plan = Settings::new(listener.local_addr(), limits)
                    .h2::<KeyTag<0>>()
                    .expect("H2 plan");
                let bank = Bank::try_new(plan.connections().count(), limits).expect("gRPC bank");
                let prepared = plan
                    .with_bound(listener)
                    .with_server(
                        storage::Value::new(DueHandler(&facts)),
                        Plan::new(
                            recipe::Borrow,
                            &bank,
                            Demand::<UnaryTag>::try_new(2).expect("two tasks"),
                        ),
                    )
                    .for_install::<timing::Throughput>()
                    .expect("deadline topology");
                let mut reports =
                    Reports::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
                let mut finish = Completion::new();
                prepared.enter_with(
                    source,
                    |worker| {
                        worker
                            .with(lending::Clean(&mut finish))
                            .expect("deadline installation")
                            .expect("deadline execution");
                    },
                    recovery::Policy::new(|event| reports.push(event), common::recover),
                    |result| {
                        result
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("driver");
                    },
                );
                assert!(
                    facts.overlap.get(),
                    "both connections had live pending tasks"
                );
                assert!(facts.polls.iter().all(|polls| polls.get() != 0));
                assert_eq!([facts.drops[0].get(), facts.drops[1].get()], [1, 1]);
                assert!(reports.into_events().expect("retirement").is_empty());
                Ok::<(), Infallible>(())
            },
            |address| {
                let mut expired =
                    Peer::try_config(address, config::Config::default()).expect("timed peer");
                let mut live =
                    Peer::try_config(address, config::Config::default()).expect("untimed peer");
                let metadata = Metadata::new();
                let timed = deadline::try_fields(b"/deadline/expired", Some(b"500m"))
                    .expect("timed fields");
                let timed_id = expired
                    .client
                    .streams()
                    .headers()
                    .request(&timed, Boundary::More)
                    .expect("timed request");
                let sent = expired
                    .client
                    .streams()
                    .try_send(timed_id, &[0; 5], Boundary::End)
                    .expect("timed unary message");
                assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == 5));
                let live_id = live
                    .client
                    .streams()
                    .headers()
                    .request(
                        Fields::new(b"/deadline/live", None, &metadata).expect("untimed fields"),
                        Boundary::More,
                    )
                    .expect("untimed request");
                let sent = live
                    .client
                    .streams()
                    .try_send(live_id, &[0; 5], Boundary::End)
                    .expect("untimed unary message");
                assert!(matches!(sent, DataOutcome::Sent(count) if count.get() == 5));
                expired.try_flush().expect("timed request flush");
                live.try_flush().expect("untimed request flush");
                let replies =
                    deadline::try_replies(&mut expired, &[timed_id]).expect("deadline response");
                assert_eq!(replies[0].code, Some(Code::DeadlineExceeded));
                assert!(replies[0].body.is_empty());
                let replies =
                    deadline::try_replies(&mut live, &[live_id]).expect("sibling response");
                assert_eq!(replies[0].code, Some(Code::Ok));
                assert_eq!(replies[0].body, [0; 5]);
            },
        )
        .expect("isolated unary cancellation");
}
