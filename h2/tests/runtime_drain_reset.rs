pub mod common;

use std::{cell::Cell, convert::Infallible, num::NonZero, time::Duration};

use common::{headers::Generated, recipe::Lexical};
use dope::{
    core::driver::life::recovery::{Policy, Recovery},
    manifold::{
        network::listener::handler::{self, failures},
        timing,
    },
    runtime::{executor::session::Completion, topology::ops},
};
use dope_test::harness::{peer::Peer, server::Pair};
use o3::base::time::Positive;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{event::Event, machine::Connection},
    frame::numbers::ErrorCode,
    server::{runtime::modes, service::Route},
};

#[test]
fn reset_completes_drain() {
    Pair::bind()
        .expect("listener")
        .run(
            |source, listener| {
                let calls = Cell::new(0);
                let mut reports = Vec::new();
                let prepared = Lexical::try_plan()
                    .expect("H2 plan")
                    .with_streams(NonZero::new(1).expect("one physical stream"))
                    .with_bound(listener)
                    .with_life(handler::Lifetime::graceful(
                        Positive::from_millis::<250>(),
                        Positive::from_secs::<10>(),
                    ))
                    .with_plain::<_, modes::Sync>((Route::new(Lexical(&calls)), failures::Close))
                    .for_install::<timing::Throughput>()
                    .expect("H2 layout");
                prepared.enter(
                    source,
                    ops::Run(&mut Completion::new()),
                    Policy::new(
                        |event| reports.push(event),
                        |failure: Recovery<'_, '_>| -> Infallible {
                            panic!("native retirement: {:?}", failure.cause())
                        },
                    ),
                    |result| {
                        let _ = result
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("H2 runtime")
                            .as_ref()
                            .map_err(|error| error.cause())
                            .expect("H2 execution");
                    },
                );
                assert!(reports.is_empty(), "unexpected reclamation: {reports:?}");
                assert_eq!(calls.get(), 0);
                Ok::<_, Infallible>(())
            },
            |address| {
                let socket = Peer::at(address)
                    .try_read(Duration::from_secs(2))
                    .expect("connect H2 peer");
                let mut client = Connection::<ClientRole>::try_new().expect("H2 client");
                let stream = client
                    .streams()
                    .headers()
                    .request_key(Generated(b""), Boundary::More)
                    .expect("open request");
                let mut goaway = false;
                let mut input = [0; 4096];
                loop {
                    while !client.output().is_empty() {
                        let bytes = client.output().first();
                        socket.write_all(bytes).expect("H2 output");
                        let sent = bytes.len();
                        client.transfer().drain(sent);
                    }
                    if goaway {
                        break;
                    }
                    let received = socket.read(&mut input).expect("GOAWAY before timeout");
                    assert_ne!(received, 0, "request remains live through GOAWAY");
                    client
                        .ingress()
                        .ingest(&input[..received])
                        .expect("H2 input");
                    while let Some(event) = client.ingress().poll_event() {
                        match event {
                            Event::GoAway {
                                last_stream_id: last,
                                error,
                                ..
                            } => {
                                assert_eq!(last, stream.id());
                                assert_eq!(error, ErrorCode::NoError);
                                goaway = true;
                            }
                            event => assert!(
                                matches!(
                                    event,
                                    Event::PrefaceComplete
                                        | Event::SettingsApplied
                                        | Event::SettingsAck
                                ),
                                "unexpected response to an open request",
                            ),
                        }
                    }
                }
                client.streams().cancel(stream, ErrorCode::Cancel);
                socket
                    .write_all(client.output().first())
                    .expect("reset final request");
                assert_eq!(
                    socket.read(&mut [0; 1]).expect("EOF before drain timeout"),
                    0,
                );
            },
        )
        .expect("H2 drain scenario");
}
