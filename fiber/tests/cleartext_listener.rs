pub mod common;

use std::{
    convert::Infallible,
    io::{Read, Write},
    net::Shutdown,
    time::Duration,
};

use common::{
    h2::{peer::Peer, tls},
    route::body::tunnels::{WEBSOCKET_FRAMES, WEBSOCKET_OPENING, WEBSOCKET_REPLY},
    server::peer::{Endpoint as _, Receive as _, Response as _},
};
use dope_test::harness::server::Pair;
use sark_core::manifold::stream::{Boundary, Section};
use sark_grpc::{Code, Status, metadata::Metadata, protocol::request};
use sark_h2::conn::{DataOutcome, event::Event};

#[test]
fn selects_existing_protocol_owners() {
    Pair::bind().expect("listener").run(
        |context, listener| {
            let (snapshot, failed) = common::h2::cleartext::run(context, listener);
            assert_eq!(failed, 1);
            assert_eq!(snapshot.capability, 1);
            assert_eq!(snapshot.after_finish, 1);
            assert_eq!(snapshot.dropped, 1);
            assert!(snapshot.websocket_failures.is_empty(), "{snapshot:?}");
            Ok::<_, Infallible>(())
        },
        |address| {
            let (mut wire, mut client) = Peer::try_connect(address, Duration::from_secs(3))
                .expect("H2 peer");
            for round in 0..2 {
                let metadata = Metadata::new();
                let fields = request::Fields::new(tls::UNARY, Some(b"localhost"), &metadata)
                    .expect("gRPC fields");
                let rpc = client.streams().headers().request(fields, Boundary::More).expect("RPC");
                assert!(matches!(client.streams().try_send(rpc, &[0; 5], Boundary::End).expect("message"),
                    DataOutcome::Sent(bytes) if bytes.get() == 5));
                tls::flush(&mut client, &mut wire).expect("request");
                let mut body = Vec::new();
                let mut status = None;
                let mut input = [0; 4096];
                while status.is_none() {
                    let read = wire.read(&mut input).expect("gRPC response");
                    assert_ne!(read, 0);
                    client.ingress().ingest(&input[..read]).expect("H2 ingress");
                    while let Some(event) = client.ingress().poll_event() {
                        match event {
                            Event::Headers { stream_id, head, section, boundary } => {
                                assert_eq!(stream_id.id(), rpc);
                                match section {
                                    Section::Initial => assert_eq!(head.fields().iter().find(|f| f.name == b":status")
                                        .map(|f| f.value), Some(b"200".as_slice())),
                                    Section::Trailing => {
                                        assert_eq!(boundary, Boundary::End);
                                        assert!(status.replace(Status::from_fields(head.fields()).expect("status").code()).is_none());
                                    }
                                }
                            }
                            Event::Data { stream_id, data, .. } => {
                                assert_eq!(stream_id.id(), rpc);
                                data.for_each(|bytes| body.extend_from_slice(bytes));
                            }
                            event => assert!(matches!(event,
                                Event::PrefaceComplete | Event::SettingsApplied | Event::SettingsAck)),
                        }
                    }
                    tls::flush(&mut client, &mut wire).expect("acknowledgments");
                }
                assert_eq!(status, Some(Code::Ok));
                assert_eq!(body, [0, 0, 0, 0, 2, 8, 1]);
                if round == 0 {
                    for (path, suffix) in [
                        ("/stream", b"5\r\nalpha\r\n5\r\n-beta\r\n6\r\n-gamma\r\n0\r\n\r\n".as_slice()),
                        ("/empty", b"\r\n\r\n0\r\n\r\n".as_slice()),
                    ] {
                        let response = address.try_get(path).expect("gated stream completes");
                        assert_eq!(response.status_line(), b"HTTP/1.1 200 OK");
                        assert!(response.ends_with(suffix));
                    }
                    let response = address.try_get("/wait").expect("H1 while H2 bank is full");
                    assert_eq!(response.status_line(), b"HTTP/1.1 200 OK");
                    assert!(response.ends_with(b"waited"));
                    assert_eq!(address.try_request(&[&[WEBSOCKET_OPENING, WEBSOCKET_FRAMES].concat()])
                        .expect("WebSocket while H2 bank is full"), WEBSOCKET_REPLY);
                    let mut rejected = Peer::try_open(address, Duration::from_secs(3))
                        .expect("second H2");
                    rejected.write_all(sark_h2::conn::CLIENT_PREFACE).expect("second preface");
                    assert_eq!(rejected.read(&mut [0]).expect("late build closes"), 0);
                    let mut pending = Peer::try_open(address, Duration::from_secs(3))
                        .expect("unselected connection");
                    pending.write_all(b"PRI").expect("partial preface");
                    pending.shutdown(Shutdown::Write).expect("partial EOF");
                    assert_eq!(pending.read(&mut [0]).expect("no selected output"), 0);
                }
            }
            wire.shutdown(Shutdown::Write).expect("H2 EOF");
            wire.read_to_end(&mut Vec::new()).expect("H2 retirement");
            let mut pending = Peer::try_open(address, Duration::from_secs(3)).expect("prefix peer");
            pending.write_all(b"P").expect("partial preface");
            let (output, elapsed) = pending.try_to_close().expect("input deadline");
            assert!(output.is_empty());
            assert!(elapsed >= Duration::from_secs(1), "{elapsed:?}");
        },
    ).expect("shared cleartext listener");
}

#[test]
fn selects_http_and_h2_without_duplex() {
    Pair::bind().expect("plain listener").run(
        |context, listener| {
            common::h2::cleartext::run_plain(context, listener);
            Ok::<_, Infallible>(())
        },
        |address| {
            let response = address.try_get("/hello").expect("plain HTTP");
            assert_eq!(response.status_line(), b"HTTP/1.1 200 OK");
            assert!(response.ends_with(b"hello"));
            let (mut wire, mut client) = Peer::try_connect(address, Duration::from_secs(3))
                .expect("plain H2 peer");
            Peer::new(&mut wire, &mut client).try_settings().expect("H2 owner");
            wire.shutdown(Shutdown::Write).expect("H2 EOF");
            wire.read_to_end(&mut Vec::new()).expect("H2 retirement");
        },
    ).expect("plain cleartext listener");
}
