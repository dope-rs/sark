use dope::core::driver::life::recovery;
pub mod common;

use std::{convert::Infallible, sync::mpsc, time::Duration};

use common::session::Connect;
use dope::{
    core::driver::route::{FrameworkTag, Tag, kind},
    runtime::{
        executor::session::{Completion, Error},
        topology,
    },
};
use dope_test::{harness::server::Pair, reclaim::Reports};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{event::Event, machine::Connection},
    server::Server,
};

use crate::common::body::{
    Configure, LIMITS, Peer,
    fairness::{BURST, Fairness, Trace},
    headers,
};

#[test]
fn yields_and_retains_owner() {
    let pair = Pair::bind().expect("H2 fairness harness");
    let bind = pair.addr();
    let (first, started) = mpsc::sync_channel(0);
    let (queued, ready) = mpsc::sync_channel(0);
    pair.check(
        move |context, listener| {
            let trace = Trace::default();
            let handler = Fairness {
                trace: &trace,
                first: || {
                    assert_eq!(
                        trace.flood.get(),
                        1,
                        "the first flood handler must own the deterministic start barrier",
                    );
                    first.send(()).expect("first flood handler");
                    ready.recv_timeout(Duration::from_secs(3))
                        .expect("probe queued before flood resume");
                },
            };
            let prepared = Server::from_handler(handler, LIMITS.try_plan(bind).expect("H2 server plan"))
                .prepare_on(listener).expect("H2 server resources");
            let mut reports = Reports::<1>::new(
                prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE],
            );
            let mut finish = Completion::new();
            prepared.enter_with(
                context,
                |worker| {
                    worker.lend(|plan, stage| {
                        plan.install(stage, |host, stage| {
                            stage.with_app(&mut finish, host, |app| {
                                app.run_for(Duration::from_secs(3)).inspect(|_| {
                                    let at = trace.seen.get().expect("probe dispatched");
                                    assert!(at < BURST,
                                        "probe dispatched only after all {BURST} flood owners ({at})");
                                    assert_eq!(trace.flood.get(), BURST);
                                })
                            })
                        })
                        .map_err(topology::Failure::Install)?
                        .map_err(topology::Failure::Run)?
                        .map_err(|cause| topology::Failure::Run(Error::Drive(cause)))
                    })
                },
                recovery::Policy::new(
                    |event| reports.push(event),
                    common::recover,
                ),
                |result| {
                    let result = match result {
                        Ok(result) => result.map_err(topology::RunError::Failure),
                        Err(error) => Err(topology::RunError::from(error)),
                    };
                    result.as_ref().map(|_| ()).map_err(topology::RunError::cause)
                        .expect("H2 server shutdown")
                },
            );
            assert!(reports.events().expect("H2 retirement").is_empty(), "retirement: {reports:?}");
            assert_eq!(trace.mismatch.get(), None, "request and response DATA owner");
            Ok::<(), Infallible>(())
        },
        move |address| {
            let mut bulk_io = address.try_connect(Duration::from_secs(5)).expect("H2 peer connection");
            let mut probe_io = address.try_connect(Duration::from_secs(5)).expect("H2 peer connection");
            let mut bulk =
                Connection::<ClientRole>::try_new().expect("default H2 client plan is valid");
            let mut peer =
                Connection::<ClientRole>::try_new().expect("default H2 client plan is valid");
            let mut streams = Vec::with_capacity(BURST);
            for _ in 0..BURST {
                let stream = bulk.streams().headers()
                    .request(
                        headers(b"/ingress-flood", b"5").expect("request fields"),
                        Boundary::More,
                    )
                    .expect("start ingress flood request");
                bulk.try_send_all(stream, b"flood", true).expect("H2 request DATA");
                streams.push(stream);
            }
            let probe = peer.streams().headers()
                .request(
                    headers(b"/ingress-probe", b"5").expect("request fields"),
                    Boundary::More,
                )
                .expect("start ingress probe request");
            peer.try_send_all(probe, b"probe", true).expect("H2 request DATA");

            bulk.try_flush(&mut bulk_io).expect("H2 peer flush");
            started.recv_timeout(Duration::from_secs(3)).expect("first flood handler");
            peer.try_flush(&mut probe_io).expect("H2 peer flush");
            queued.send(()).expect("probe wire queued");

            assert_eq!(peer.try_read(&mut probe_io, probe).expect("H2 response body"), b"probe");
            let mut bodies = vec![Vec::new(); BURST];
            let mut ended = vec![false; BURST];
            bulk.try_events(&mut bulk_io, |event| {
                assert!(!matches!(&event, Event::StreamReset { .. }), "flood stream reset");
                let (key, edge, data) = match event {
                    Event::Headers { stream_id: key, boundary, .. } => (key, boundary, None),
                    Event::Data { stream_id: key, data, boundary } => (key, boundary, Some(data)),
                    _ => return None,
                };
                let index = streams.iter().position(|id| *id == key.id()).expect("flood response stream");
                if let Some(data) = data {
                    data.for_each(|part| bodies[index].extend_from_slice(part));
                }
                if matches!(edge, Boundary::End) {
                    assert!(!ended[index], "flood response ended twice");
                    ended[index] = true;
                }
                ended.iter().all(|done| *done).then_some(())
            }).expect("H2 flood responses");
            assert!(bodies.iter().all(|body| body == b"flood"));
        },
        |result| result.expect("H2 server runtime"),
    )
    .expect("body-credit fairness E2E");
}
