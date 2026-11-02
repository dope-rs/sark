pub mod common;

use std::{
    convert::Infallible,
    io::{Read as _, Write as _},
    time::Duration,
};

use codec::session::{Audit, Session};
use common::codec;
use dope::{
    core::driver::{life::recovery, route::KeyTag},
    manifold::{
        app::{Host, env::Bundle},
        client::session::{Setup, ingress},
        network::client::topology::plan,
        service::{Endpoint, Revision, Snapshot, health::Domain, sync},
        timing::Balanced,
    },
    net::{
        tcp::Tcp,
        wire::{self, domain::Local},
    },
    runtime::{
        executor::session::Completion,
        topology::{self, lending::Stage},
    },
};
use dope_test::{
    fibers,
    harness::{GUARD, peer::TcpScript},
};
use o3::depot::capacity::Count;
use sark_client::protocol::codec::Codec;

#[test]
fn refusal_resumes_stored() {
    TcpScript::with(
        |stream| {
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx")
                .expect("HTTP response");
            assert_eq!(stream.read(&mut [0]).expect("client close"), 0);
        },
        |peer| {
            let address = peer.addr();
            let audit = Audit::default();
            let plan = plan::Plan::<KeyTag<0>, 1>::new(
                1,
                plan::Spec::new(Duration::from_millis(10), Duration::from_secs(1))
                    .expect("backoff"),
            )
            .expect("connection plan");
            let prepared = topology::Plan::new((), plan)
                .for_install::<Balanced>()
                .expect("runtime claims");
            let mut finish = Completion::new();
            let mut reports = Vec::new();
            prepared.with_local(
                |worker| {
                    worker.lend(|plan, stage| {
                        let hash = stage.hash_state(Domain::DEFAULT);
                        let snapshot =
                            Snapshot::from_one(Revision::new(1), Endpoint::new(address, address));
                        let setup = Setup::new(
                            Session {
                                codec: Codec::default(),
                                audit: &audit,
                            },
                            ingress::Plan::local(
                                ingress::Bytes::new(b"Content-Length: 1\r\n".len()),
                                Count::EMPTY,
                            ),
                        );
                        let plan = plan.bind(Local::new(wire::Identity), hash);
                        let result = plan.install::<
                            _,
                            _,
                            sync::Preserve,
                            _,
                            Bundle<Tcp, wire::Identity, Balanced>,
                            1,
                            _,
                            _,
                        >(
                            setup,
                            snapshot,
                            stage,
                            Stage::driver,
                            |pool, stage| {
                                let result = stage.with_quiesce(
                                    &mut finish,
                                    Host::new(pool),
                                    GUARD,
                                    |mut app| {
                                        let wait = fibers::TEST
                                            .try_wait(&mut app, &audit, 1)
                                            .expect("drive HTTP admission");
                                        assert_eq!(wait, fibers::Wait::Reached);
                                    },
                                );
                                assert!(result.is_ok(), "session teardown");
                            },
                        );
                        result.expect("install HTTP session");
                    });
                },
                recovery::Policy::new(
                    |event| reports.push(event),
                    |error: recovery::Recovery<'_, '_>| -> Infallible {
                        panic!("native recovery: {:?}", error.cause())
                    },
                ),
                |result| assert!(result.is_ok(), "runtime teardown"),
            );
            assert!(reports.is_empty(), "native retirement: {reports:?}");
            let audit = audit.into_result().expect("HTTP session outcome");
            assert_eq!(audit.blocked.get(), 0);
            assert_eq!(audit.attempts.each_ref().map(|count| count.get()), [2, 2]);
            assert_eq!(audit.accepted.each_ref().map(|count| count.get()), [1, 1]);
            assert_eq!(audit.connected.get(), 1);
            assert_eq!(audit.retired.get(), 1);
            peer.finish().expect("peer exit");
        },
    )
    .expect("spawn HTTP peer");
}
