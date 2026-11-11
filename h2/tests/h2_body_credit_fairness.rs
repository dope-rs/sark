use dope::core::driver::life::recovery;
pub mod common;

use std::{cell::Cell, convert::Infallible, net::TcpStream, time::Duration};

use common::session::Connect;
use dope::{
    core::driver::route::{FrameworkTag, Tag, kind},
    manifold::network::listener::ExecutionError,
    runtime::{executor::session::Completion, topology},
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
    pair.check(
        move |context, listener| {
            let trace = Trace::default();
            let socket = TcpStream::connect_timeout(&bind, Duration::from_secs(3))
                .expect("H2 probe connection");
            socket.set_nodelay(true).expect("probe TCP_NODELAY");
            socket.set_read_timeout(Some(Duration::from_secs(5))).expect("probe read deadline");
            socket.set_write_timeout(Some(Duration::from_secs(3))).expect("probe write deadline");
            let mut peer = Connection::<ClientRole>::try_new().expect("default H2 client plan is valid");
            let id = peer.streams().headers().request(
                headers(b"/ingress-probe", b"5").expect("request fields"),
                Boundary::More,
            ).expect("start ingress probe request");
            peer.try_send_all(id, b"probe", true).expect("H2 request DATA");
            let probe = Cell::new(Some((socket, peer, id)));
            let handler = Fairness {
                trace: &trace,
                first: || {
                    assert_eq!(
                        trace.flood.get(),
                        1,
                        "the first flood handler queues the probe",
                    );
                    let (mut socket, mut peer, id) = probe.take().expect("probe queued");
                    peer.try_flush(&mut socket).expect("H2 peer flush");
                    probe.set(Some((socket, peer, id)));
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
                            let (host, wait) = host.into_parts();
                            wait.watch().finish(stage.with_app(&mut finish, host, |app| {
                                app.run_for(Duration::from_secs(3)).inspect(|_| {
                                    let at = trace.seen.get().expect("probe dispatched");
                                    assert!(at < BURST,
                                        "probe dispatched only after all {BURST} flood owners ({at})");
                                    assert_eq!(trace.flood.get(), BURST);
                                }).map_err(ExecutionError::Drive)
                            }))
                        })
                        .map_err(topology::Failure::Install)?
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
            let (mut socket, mut peer, id) = probe.take().expect("probe queued");
            assert_eq!(peer.try_read(&mut socket, id).expect("H2 response body"), b"probe");
            assert!(reports.events().expect("H2 retirement").is_empty(), "retirement: {reports:?}");
            assert_eq!(trace.mismatch.get(), None, "request and response DATA owner");
            Ok::<(), Infallible>(())
        },
        move |address| {
            let mut bulk_io = address.try_connect(Duration::from_secs(5)).expect("H2 peer connection");
            let mut bulk =
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
            bulk.try_flush(&mut bulk_io).expect("H2 peer flush");
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
