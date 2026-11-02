pub mod common;

use std::{convert::Infallible, time::Duration};

use common::server::recover;
use common::{
    h2::{
        facts::State,
        peer::{Peer, Sender},
        server::{CONFIG, Server},
    },
    server::{
        self,
        grpc::{DueFacts, DueFields, DueHandler, Facts, Handler},
    },
};
use dope::{
    core::driver::{life::recovery, storage},
    manifold::timing,
    runtime::{
        executor::session::Completion,
        topology::{lending, recipe},
    },
};
use dope_test::harness::server::Pair;
use sark_core::manifold::stream::Boundary;
use sark_fiber::manifold::runtime::NativeFiber;
use sark_grpc::{
    Code, Status,
    metadata::Metadata,
    protocol::request::Fields,
    server::{
        Bank, Plan,
        config::Limits,
        mode::{Demand, StreamTag, UnaryTag},
    },
};

#[test]
fn unary_yields_to_complete() {
    Pair::bind()
        .expect("gRPC listener")
        .run(
            |source, listener| {
                let facts = Facts::default();
                let plan = Server::new(CONFIG, 1, State::default())
                    .try_plan(listener.local_addr())
                    .expect("H2 plan");
                let limits = Limits::default().try_calls(1).expect("one call");
                let bank = Bank::try_new(plan.connections().count(), limits).expect("gRPC bank");
                let mut reports = server::Plan::try_reports(&plan).expect("driver claims");
                let prepared = plan
                    .with_bound(listener)
                    .with_server(
                        storage::Value::new(Handler(&facts)),
                        Plan::new(
                            recipe::Borrow,
                            &bank,
                            Demand::<NativeFiber, UnaryTag>::try_new(1).expect("one unary task"),
                        ),
                    )
                    .for_install::<timing::Throughput>()
                    .expect("gRPC topology");
                let mut finish = Completion::new();
                prepared.enter_with(
                    source,
                    |worker| {
                        worker
                            .with(lending::Clean(&mut finish))
                            .expect("unary installation")
                            .expect("unary execution");
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
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
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("gRPC peer");
                let metadata = Metadata::new();
                let fields = Fields::new(b"/test/Echo", Some(b"localhost"), &metadata)
                    .expect("gRPC request");
                let stream = client
                    .streams()
                    .headers()
                    .request(fields, Boundary::More)
                    .expect("request headers");
                Sender::new(&mut client)
                    .try_send(stream, &[0; 5], Boundary::End)
                    .expect("empty protobuf request");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("request flush");
                let (trailers, body) = Peer::new(&mut socket, &mut client)
                    .try_response(stream)
                    .expect("unary response");
                assert_eq!(body, [0; 5]);
                assert_eq!(
                    Status::from_fields(&trailers).expect("gRPC status").code(),
                    Code::Ok
                );
            },
        )
        .expect("unary runtime");
}

#[test]
fn streaming_finishes() {
    Pair::bind()
        .expect("gRPC listener")
        .run(
            |source, listener| {
                let facts = Facts::default();
                let plan = Server::new(CONFIG, 1, State::default())
                    .try_plan(listener.local_addr())
                    .expect("H2 plan");
                let limits = Limits::default().try_calls(1).expect("one call");
                let bank = Bank::try_new(plan.connections().count(), limits).expect("gRPC bank");
                let mut reports = server::Plan::try_reports(&plan).expect("driver claims");
                let prepared = plan
                    .with_bound(listener)
                    .with_server(
                        storage::Value::new(Handler(&facts)),
                        Plan::new(
                            recipe::Borrow,
                            &bank,
                            Demand::<NativeFiber, StreamTag>::try_new(1).expect("one stream task"),
                        ),
                    )
                    .for_install::<timing::Throughput>()
                    .expect("gRPC topology");
                let mut finish = Completion::new();
                prepared.enter_with(
                    source,
                    |worker| {
                        worker
                            .with(lending::Clean(&mut finish))
                            .expect("stream installation")
                            .expect("stream execution");
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
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
                assert_eq!(facts.drops.get(), 1);
                assert!(reports.into_events().expect("retirement").is_empty());
                Ok::<(), Infallible>(())
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("gRPC peer");
                let metadata = Metadata::new();
                let fields = Fields::new(b"/test/Echo", Some(b"localhost"), &metadata)
                    .expect("gRPC request");
                let stream = client
                    .streams()
                    .headers()
                    .request(fields, Boundary::More)
                    .expect("request headers");
                Sender::new(&mut client)
                    .try_send(stream, &[0; 5], Boundary::End)
                    .expect("empty protobuf request");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("request flush");
                let (trailers, body) = Peer::new(&mut socket, &mut client)
                    .try_response(stream)
                    .expect("stream response");
                assert_eq!(body, [0; 5]);
                assert_eq!(
                    Status::from_fields(&trailers).expect("gRPC status").code(),
                    Code::Ok
                );
            },
        )
        .expect("stream runtime");
}

#[test]
fn unary_expiry() {
    use sark_http::field::{self, Value};

    Pair::bind()
        .expect("deadline listener")
        .run(
            |source, listener| {
                let facts = DueFacts::default();
                let plan = Server::new(CONFIG, 2, State::default())
                    .try_plan(listener.local_addr())
                    .expect("H2 plan");
                let bank = Bank::try_new(
                    plan.connections().count(),
                    Limits::default()
                        .try_calls(1)
                        .expect("one call per connection"),
                )
                .expect("gRPC bank");
                let mut reports = server::Plan::try_reports(&plan).expect("driver claims");
                let prepared = plan
                    .with_bound(listener)
                    .with_server(
                        storage::Value::new(DueHandler(&facts)),
                        Plan::new(
                            recipe::Borrow,
                            &bank,
                            Demand::<NativeFiber, UnaryTag>::try_new(2).expect("two tasks"),
                        ),
                    )
                    .for_install::<timing::Throughput>()
                    .expect("deadline topology");
                let mut finish = Completion::new();
                prepared.enter_with(
                    source,
                    |worker| {
                        worker
                            .with(lending::Clean(&mut finish))
                            .expect("deadline installation")
                            .expect("deadline execution");
                    },
                    recovery::Policy::new(|event| reports.push(event), recover),
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
                let (mut timed_io, mut expired) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("timed peer");
                let (mut live_io, mut live) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("untimed peer");
                let metadata = Metadata::new();
                let fields =
                    Fields::new(b"/deadline/expired", None, &metadata).expect("timed fields");
                let timed = DueFields {
                    fields,
                    timeout: field::literal!(Value::try_bytes(b"500m")),
                };
                let timed_id = expired
                    .streams()
                    .headers()
                    .request(&timed, Boundary::More)
                    .expect("timed request");
                Sender::new(&mut expired)
                    .try_send(timed_id, &[0; 5], Boundary::End)
                    .expect("timed unary message");
                let live_id = live
                    .streams()
                    .headers()
                    .request(
                        Fields::new(b"/deadline/live", None, &metadata).expect("untimed fields"),
                        Boundary::More,
                    )
                    .expect("untimed request");
                Sender::new(&mut live)
                    .try_send(live_id, &[0; 5], Boundary::End)
                    .expect("untimed unary message");
                Peer::new(&mut timed_io, &mut expired)
                    .try_flush()
                    .expect("timed request flush");
                Peer::new(&mut live_io, &mut live)
                    .try_flush()
                    .expect("untimed request flush");
                let (trailers, body) = Peer::new(&mut timed_io, &mut expired)
                    .try_response(timed_id)
                    .expect("deadline response");
                assert_eq!(
                    Status::from_fields(&trailers)
                        .expect("deadline status")
                        .code(),
                    Code::DeadlineExceeded
                );
                assert!(body.is_empty());
                let (trailers, body) = Peer::new(&mut live_io, &mut live)
                    .try_response(live_id)
                    .expect("sibling response");
                assert_eq!(
                    Status::from_fields(&trailers)
                        .expect("sibling status")
                        .code(),
                    Code::Ok
                );
                assert_eq!(body, [0; 5]);
            },
        )
        .expect("isolated unary cancellation");
}
