use std::time::{Duration, Instant};

use crate::common::conn::{
    bridge::{Pair, certificate},
    build::Fields as _,
    wire::{Ingress, Peer},
};
pub mod common;

use dope_quic::{
    conn::stream::access::bindings,
    errors::Code,
    wire::stream::{Direction, Id},
};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn,
    conn::{
        event::{Event, Origin, Role},
        io::Step,
    },
    frame,
    link::Flush,
    manifold::{self, Session},
    qpack::stream::Instruction,
};
use sark_http::field::{literal, metadata::block::ContentError};

use crate::common::{field::entry, message::promise::Authority};

#[test]
fn qpack_cancellation_stops_response_owner() {
    for request_arrived in [false, true] {
        let options = Pair::try_config()
            .expect("server options")
            .into_builder()
            .streams(|streams| streams.send_bytes(1027))
            .try_build()
            .expect("bounded output");
        let Pair {
            mut server,
            mut client,
            mut space,
        } = Pair::try_with(
            options,
            Pair::try_config().expect("client options"),
            certificate,
        )
        .expect("QUIC pair");
        let mut h3 = Session::with_role(Role::Server).expect("server H3");
        let control = h3.egress().start(&mut server).expect("control stream");
        let id = client.with_scope(|mut scope| {
            scope
                .streams()
                .send()
                .open_bidi()
                .expect("request stream")
                .id()
        });
        let mut now = Instant::now();
        if request_arrived {
            let request = Authority.try_headers().expect("request HEADERS");
            client.with_scope(|mut scope| {
                let mut streams = scope.streams();
                let send = streams.view().resolve_send(id).expect("request sender");
                streams.send().send(send, &request).expect("request bytes");
                streams.send().finish(send).expect("request FIN");
            });
            now = client.view().send_at(now);
            Peer::new(&mut space, &mut client)
                .try_send(&mut server, now, certificate)
                .expect("request transfer");
            Ingress::new(&mut h3)
                .try_receive(&mut server)
                .expect("request ingress");
            server.with_scope(|mut scope| {
                while !matches!(
                    h3.ingress()
                        .consume(&mut scope, |_| ())
                        .expect("completed request"),
                    Step::Idle
                ) {}
            });
            h3.send()
                .headers(id, [literal!(entry(b":status", b"200"))], Boundary::More)
                .expect("response headers");
            for len in [64, 1024] {
                h3.send()
                    .owned(id, vec![7; len], Boundary::More)
                    .expect("response owner");
            }
            assert!(matches!(
                h3.egress()
                    .settle(&mut server)
                    .expect("each complete DATA write fits the native limit"),
                Flush::Blocked
            ));
            assert_eq!(
                h3.h3().retained(id).expect("staged owner").resident_bytes,
                1024
            );
            server.with_scope(|mut scope| {
                let streams = scope.streams();
                let send = streams.view().resolve_send(id).expect("response sender");
                assert!(streams.view().buffered(send) > 0);
            });
        }

        let mut feedback = vec![frame::STREAM_TYPE_QPACK_DECODER.get() as u8];
        for cancelled in [control, id] {
            Instruction::StreamCancellation {
                stream_id: cancelled.get(),
            }
            .encode(&mut feedback);
        }
        client.with_scope(|mut scope| {
            let mut streams = scope.streams();
            let decoder = streams.send().open_uni().expect("decoder stream");
            streams
                .send()
                .send(decoder, &feedback)
                .expect("QPACK cancellation");
        });
        now = client
            .view()
            .send_at(now + Duration::from_millis(1));
        Peer::new(&mut space, &mut client)
            .try_send(&mut server, now, certificate)
            .expect("cancellation precedes STOP_SENDING");
        Ingress::new(&mut h3)
            .try_receive(&mut server)
            .expect("cancellation ingress");
        server.with_scope(|mut scope| {
            let mut stopped = 0;
            loop {
                match h3.ingress().consume(&mut scope, |event| {
                    matches!(event, Event::Stopped { stream_id, error_code, .. }
                        if *stream_id == id
                            && *error_code == Code::from_u16(frame::ErrorCode::RequestCancelled.wire_code()))
                }).expect("response cancellation") {
                    Step::Item(cancelled) => {
                        assert!(cancelled);
                        stopped += 1;
                    }
                    Step::More => {}
                    Step::Idle => break,
                }
            }
            assert_eq!(stopped, 1);
            let streams = scope.streams();
            let send = streams.view().resolve_send(id).expect("cancelled request");
            assert!(!streams.view().is_send_open(send));
            assert_eq!(streams.view().buffered(send), 0);
            let critical = streams.view().resolve_send(control).expect("control sender");
            assert!(streams.view().is_send_open(critical));
        });
        Ingress::new(&mut h3)
            .try_receive(&mut server)
            .expect("native Stopped preserves the original resident generation");
        server.with_scope(|mut scope| {
            while !matches!(
                h3.ingress()
                    .consume(&mut scope, |_| ())
                    .expect("native terminal callback after immediate cancellation"),
                Step::Idle
            ) {}
        });
        assert!(h3.h3().retained(id).expect("released output").is_empty());
        assert_eq!(h3.h3().wire_len(), 0);
    }
}

#[test]
fn goaway_preserves_ids() {
    let Pair {
        client: mut quic,
        mut server,
        mut space,
    } = Pair::try_new(certificate).expect("QUIC pair");
    let mut h3 = Session::new().expect("H3 client");
    server.with_scope(|mut scope| {
        let mut streams = scope.streams();
        let control = streams.send().open_uni().expect("peer control");
        streams
            .send()
            .send(control, &[0, 4, 0, 7, 1, 0])
            .expect("peer shutdown");
    });
    let now = server.view().send_at(Instant::now());
    Peer::new(&mut space, &mut server)
        .try_send(&mut quic, now, certificate)
        .expect("peer shutdown transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut quic)
        .expect("control ingress");
    quic.with_scope(|mut scope| {
        assert!(matches!(
            h3.egress().open_scope(&mut scope),
            Err(manifold::Error::H3(conn::Error::GoAway(0)))
        ));
        let next = scope
            .streams()
            .send()
            .open_handle(Direction::Bidi)
            .expect("untouched stream id");
        assert_eq!(next.id(), Id::ZERO);
    });
}

#[test]
fn raw_terminal_binding() {
    for order in [[false, true], [true, false]] {
        let Pair {
            mut server,
            mut client,
            mut space,
        } = Pair::try_new(certificate).expect("QUIC pair");
        let mut h3 = Session::new().expect("H3 client");
        let id = client.with_scope(|mut scope| {
            let target = h3.egress().open_scope(&mut scope).expect("request");
            let id = target.stream_id();
            let mut streams = scope.streams();
            let handle = streams.resident().handle(id).expect("resident");
            streams.binding().try_bind(handle, 7).expect("binding");
            let send = streams.view().resolve_send(id).expect("send stream");
            streams
                .send()
                .send(send, b"request")
                .expect("request bytes");
            id
        });
        let mut now = client.view().send_at(Instant::now());
        Peer::new(&mut space, &mut client)
            .try_send(&mut server, now, certificate)
            .expect("request transfer");
        let code = Code::from_u16(42);
        let mut received_reset = false;
        for reset in order {
            received_reset |= reset;
            server.with_scope(|mut scope| {
                let mut streams = scope.streams();
                if reset {
                    let send = streams.view().resolve_send(id).expect("response stream");
                    streams.send().reset(send, code).expect("peer reset");
                } else {
                    let recv = streams.view().resolve_recv(id).expect("request stream");
                    streams.receive().stop(recv, code).expect("peer stop");
                }
            });
            now = server
                .view()
                .send_at(now + Duration::from_millis(1));
            Peer::new(&mut space, &mut server)
                .try_send(&mut client, now, certificate)
                .expect("terminal transfer");
            Ingress::new(&mut h3)
                .try_receive(&mut client)
                .expect("raw notification");
            client.with_scope(|mut scope| {
                let consumed = h3
                    .ingress()
                    .consume(&mut scope, |event| match (reset, event) {
                        (
                            true,
                            Event::Reset {
                                stream_id: stream,
                                error_code: error,
                                origin: Origin::Transport,
                                ..
                            },
                        )
                        | (
                            false,
                            Event::Stopped {
                                stream_id: stream,
                                error_code: error,
                                origin: Origin::Transport,
                            },
                        ) => *stream == id && *error == code,
                        _ => false,
                    })
                    .expect("consume terminal");
                assert!(matches!(consumed, Step::Item(true)));
                let handle = scope.streams().resident().handle(id);
                if received_reset {
                    if let Some(handle) = handle {
                        assert!(matches!(
                            scope.streams().binding().try_release(handle),
                            Ok(bindings::Release::Vacant)
                        ));
                    }
                } else {
                    let slot = handle.and_then(|handle| {
                        scope
                            .streams()
                            .binding()
                            .try_slot(handle)
                            .expect("remaining resident")
                    });
                    assert_eq!(slot, Some(7));
                }
            });
        }
    }
}

#[test]
fn stop_preserves_queued_final_headers() {
    for order in [[true, false], [false, true]] {
        let Pair {
            mut server,
            mut client,
            mut space,
        } = Pair::try_new(certificate).expect("QUIC pair");
        let mut h3 = Session::new().expect("H3 client");
        let id = client.with_scope(|mut scope| {
            let target = h3.egress().open_scope(&mut scope).expect("request");
            h3.send()
                .bind(target)
                .expect("request owner")
                .headers(Authority, Boundary::More)
                .expect("request headers");
            h3.egress()
                .settle_scope(&mut scope)
                .expect("request output");
            target.stream_id()
        });
        let mut now = client.view().send_at(Instant::now());
        Peer::new(&mut space, &mut client)
            .try_send(&mut server, now, certificate)
            .expect("request transfer");
        let code = Code::from_u16(42);
        let headers = [literal!(entry(b":status", b"200"))]
            .try_headers()
            .expect("response headers");
        for stop in order {
            server.with_scope(|mut scope| {
                let mut streams = scope.streams();
                if stop {
                    let recv = streams.view().resolve_recv(id).expect("request stream");
                    streams.receive().stop(recv, code).expect("peer stop");
                } else {
                    let send = streams.view().resolve_send(id).expect("response stream");
                    streams.send().send(send, &headers).expect("response bytes");
                    streams.send().finish(send).expect("response FIN");
                }
            });
            now = server
                .view()
                .send_at(now + Duration::from_millis(1));
            Peer::new(&mut space, &mut server)
                .try_send(&mut client, now, certificate)
                .expect("peer transfer");
            Ingress::new(&mut h3)
                .try_receive(&mut client)
                .expect("queue peer event");
        }
        client.with_scope(|mut scope| {
            let handle = scope.streams().resident().handle(id).expect("resident");
            assert!(scope.streams().try_recv_eof(handle).expect("native EOF"));
            let stopped = h3
                .ingress()
                .consume(&mut scope, |event| {
                    matches!(event, Event::Stopped { stream_id, error_code, .. }
                        if *stream_id == id && *error_code == code)
                })
                .expect("consume stop");
            assert!(matches!(stopped, Step::Item(true)));
            assert!(matches!(
                scope.streams().binding().try_attach(handle),
                Ok(bindings::Attach::Present)
            ));
            let finished = h3
                .ingress()
                .consume(&mut scope, |event| {
                    matches!(event, Event::Headers { message, .. }
                        if message.stream_id() == id && message.boundary() == Boundary::End)
                })
                .expect("consume final headers");
            assert!(matches!(finished, Step::Item(true)));
            if let Some(handle) = scope.streams().resident().handle(id) {
                assert!(matches!(
                    scope.streams().binding().try_release(handle),
                    Ok(bindings::Release::Vacant)
                ));
            }
        });
    }
}

#[test]
fn reset_reconciles_final_credit_quantum() {
    let Pair {
        mut server,
        mut client,
        mut space,
    } = Pair::try_new(certificate).expect("QUIC pair");
    let mut response = [literal!(entry(b":status", b"200"))]
        .try_headers()
        .expect("response headers");
    let mut healthy = response.clone();
    frame::Payload::encode(frame::TYPE_DATA, &[7; 64], &mut healthy).expect("DATA frame");
    // CREDIT_QUANTUM=32 resumes before popping: the final DATA receipt stays queued.
    for _ in 0..31 {
        frame::Payload::encode(
            dope_quic::wire::varint::VarInt::new(0x21).expect("unknown frame type"),
            &[],
            &mut response,
        )
        .expect("ignorable frame");
    }
    frame::Payload::encode(frame::TYPE_DATA, &[7], &mut response).expect("final DATA frame");
    let mut config = conn::options::Config::default();
    config.ingress.retained_bytes = healthy.len();
    let mut h3 = Session::with_config(Role::Client, config).expect("H3 client");
    let code = Code::from_u16(42);
    let mut now = Instant::now();
    for (reset, response) in [(true, &response), (false, &healthy)] {
        let id = client.with_scope(|mut scope| {
            let target = h3.egress().open_scope(&mut scope).expect("request");
            h3.send()
                .bind(target)
                .expect("request owner")
                .headers(Authority, Boundary::End)
                .expect("request headers");
            h3.egress()
                .settle_scope(&mut scope)
                .expect("request output");
            target.stream_id()
        });
        now = client
            .view()
            .send_at(now + Duration::from_millis(1));
        Peer::new(&mut space, &mut client)
            .try_send(&mut server, now, certificate)
            .expect("request transfer");
        now = server
            .view()
            .send_at(now + Duration::from_millis(30));
        Peer::new(&mut space, &mut server)
            .try_send(&mut client, now, certificate)
            .expect("request FIN acknowledgement");
        client.with_scope(|mut scope| {
            let streams = scope.streams();
            let send = streams.view().resolve_send(id).expect("request sender");
            assert_eq!(streams.view().buffered(send), 0);
            assert!(!streams.view().is_send_open(send));
        });
        server.with_scope(|mut scope| {
            let mut streams = scope.streams();
            let send = streams.view().resolve_send(id).expect("response stream");
            streams.send().send(send, response).expect("response bytes");
            streams.send().finish(send).expect("response FIN");
        });
        now = server
            .view()
            .send_at(now + Duration::from_millis(1));
        Peer::new(&mut space, &mut server)
            .try_send(&mut client, now, certificate)
            .expect("response transfer");
        Ingress::new(&mut h3)
            .try_receive(&mut client)
            .expect("queue response");
        if reset {
            client.with_scope(|mut scope| {
                let handle = scope.streams().resident().handle(id).expect("resident");
                assert!(scope.streams().try_recv_eof(handle).expect("native FIN"));
            });
            server.with_scope(|mut scope| {
                let mut streams = scope.streams();
                let send = streams.view().resolve_send(id).expect("response stream");
                streams.send().reset(send, code).expect("peer reset");
            });
            now = server
                .view()
                .send_at(now + Duration::from_millis(1));
            Peer::new(&mut space, &mut server)
                .try_send(&mut client, now, certificate)
                .expect("reset transfer");
            Ingress::new(&mut h3)
                .try_receive(&mut client)
                .expect("replace queued terminal with peer reset");
        } else {
            client.with_scope(|mut scope| {
                assert!(matches!(
                    h3.ingress()
                        .consume(
                            &mut scope,
                            |event| matches!(event, Event::Headers { message, .. }
                            if message.stream_id() == id && message.boundary() == Boundary::More)
                        )
                        .expect("response headers"),
                    Step::Item(true)
                ));
            });
        }
        client.with_scope(|mut scope| {
            assert!(matches!(
                h3.ingress()
                    .consume(&mut scope, |event| match (reset, event) {
                        (
                            true,
                            Event::Reset {
                                stream_id,
                                error_code,
                                released: 3,
                                ..
                            },
                        ) => *stream_id == id && *error_code == code,
                        (false, Event::Data { message, .. }) =>
                            message.stream_id() == id && message.boundary() == Boundary::End,
                        _ => false,
                    })
                    .expect("terminal delivery reconciles receive credit"),
                Step::Item(true)
            ));
            if let Some(handle) = scope.streams().resident().handle(id) {
                assert!(matches!(
                    scope.streams().binding().try_release(handle),
                    Ok(bindings::Release::Vacant)
                ));
            }
            assert!(matches!(
                h3.ingress()
                    .consume(&mut scope, Event::stream_id)
                    .expect("drained ingress"),
                Step::Idle
            ));
        });
    }
}

#[test]
fn rejected_stream_absorbs_inflight_data() {
    let headers = [
        literal!(entry(b":method", b"POST")),
        literal!(entry(b":scheme", b"https")),
        literal!(entry(b":authority", b"example.test")),
        literal!(entry(b":path", b"/")),
        literal!(entry(b"content-length", b"0")),
    ]
    .try_headers()
    .expect("request headers");
    let mut malformed = headers.clone();
    frame::Payload::encode(frame::TYPE_DATA, b"x", &mut malformed).expect("excess DATA");
    let mut continuation = Vec::new();
    frame::Payload::encode(frame::TYPE_DATA, b"inflight", &mut continuation)
        .expect("inflight DATA");
    let code = Code::from_u16(frame::ErrorCode::Message.wire_code());
    for reset in [false, true] {
        let Pair {
            mut server,
            mut client,
            mut space,
        } = Pair::try_new(certificate).expect("QUIC pair");
        let mut h3 = Session::with_role(Role::Server).expect("H3 server");
        let [bad, healthy] = client.with_scope(|mut scope| {
            std::array::from_fn(|_| {
                scope
                    .streams()
                    .send()
                    .open_handle(Direction::Bidi)
                    .expect("request stream")
                    .id()
            })
        });
        let mut now = Instant::now();
        let mut observed = [0; 4];
        for stage in 0..3 {
            client.with_scope(|mut scope| {
                let mut streams = scope.streams();
                let rejected = streams.view().resolve_send(bad).expect("open sender");
                match stage {
                    0 => streams
                        .send()
                        .send(rejected, &malformed)
                        .expect("unfinished malformed request"),
                    1 => {
                        streams
                            .send()
                            .send(rejected, &continuation)
                            .expect("continuation before peer sees STOP");
                        let next = streams
                            .view()
                            .resolve_send(healthy)
                            .expect("healthy sender");
                        streams
                            .send()
                            .send(next, &headers)
                            .expect("healthy request");
                        streams.send().finish(next).expect("healthy FIN");
                    }
                    _ if reset => streams.send().reset(rejected, code).expect("late RESET"),
                    _ => streams.send().finish(rejected).expect("late FIN"),
                }
            });
            now = client
                .view()
                .send_at(now + Duration::from_millis(1));
            // Keep the server's STOP/RESET packets queued: the peer may still send.
            Peer::new(&mut space, &mut client)
                .try_send(&mut server, now, certificate)
                .expect("native request transfer");
            Ingress::new(&mut h3)
                .try_receive(&mut server)
                .expect("native request ingress");
            server.with_scope(|mut scope| {
                loop {
                    let step = h3
                        .ingress()
                        .consume(&mut scope, |event| {
                            let index = match (stage, event) {
                                (0, Event::Headers { message, .. })
                                    if message.stream_id() == bad =>
                                {
                                    0
                                }
                                (
                                    0,
                                    Event::Rejected {
                                        stream_id,
                                        error:
                                            ContentError::Exceeded {
                                                remaining: 0,
                                                actual: 1,
                                            },
                                    },
                                ) if *stream_id == bad => 1,
                                (1, Event::Headers { message, .. })
                                    if message.stream_id() == healthy
                                        && message.boundary() == Boundary::End =>
                                {
                                    2
                                }
                                (2, Event::Finished { stream_id })
                                    if !reset && *stream_id == bad =>
                                {
                                    3
                                }
                                (
                                    2,
                                    Event::Reset {
                                        stream_id,
                                        error_code,
                                        ..
                                    },
                                ) if reset && *stream_id == bad && *error_code == code => 3,
                                _ => panic!("unexpected H3 event in stage {stage}"),
                            };
                            observed[index] += 1;
                        })
                        .expect("scoped message delivery");
                    if matches!(step, Step::Idle) {
                        break;
                    }
                }
                let resident = scope.streams().resident().handle(bad);
                match resident {
                    Some(handle) if stage < 2 => {
                        assert!(!scope.streams().try_recv_eof(handle).expect("open receiver"));
                        assert!(matches!(
                            scope.streams().binding().try_attach(handle),
                            Ok(bindings::Attach::Present)
                        ));
                    }
                    Some(handle) => assert!(matches!(
                        scope.streams().binding().try_release(handle),
                        Ok(bindings::Release::Vacant)
                    )),
                    None => assert_eq!(stage, 2),
                }
                assert!(!scope.view().lifecycle().is_closing());
            });
        }
        assert_eq!(observed, [0, 1, 1, 1]);
    }
}

#[test]
fn raw_unknown_uni_releases_binding() {
    for (parsed, reset) in [(false, true), (true, true), (true, false)] {
        let Pair {
            mut client,
            mut server,
            mut space,
        } = Pair::try_new(certificate).expect("QUIC pair");
        let mut h3 = Session::new().expect("H3 client");
        let id = server.with_scope(|mut scope| {
            let mut streams = scope.streams();
            let send = streams.send().open_uni().expect("unknown stream");
            streams.send().send(send, &[0x21]).expect("unknown type");
            send.id()
        });
        let mut now = server.view().send_at(Instant::now());
        Peer::new(&mut space, &mut server)
            .try_send(&mut client, now, certificate)
            .expect("unknown stream transfer");
        if parsed {
            Ingress::new(&mut h3)
                .try_receive(&mut client)
                .expect("unknown stream ingress");
            client.with_scope(|mut scope| {
                assert!(matches!(
                    h3.ingress().consume(&mut scope, |_| ()),
                    Ok(Step::Idle)
                ));
            });
        }
        let code = Code::from_u16(42);
        server.with_scope(|mut scope| {
            let mut streams = scope.streams();
            let send = streams.view().resolve_send(id).expect("unknown stream");
            if reset {
                streams.send().reset(send, code).expect("peer reset");
            } else {
                streams.send().finish(send).expect("peer FIN");
            }
        });
        now = server
            .view()
            .send_at(now + Duration::from_millis(1));
        Peer::new(&mut space, &mut server)
            .try_send(&mut client, now, certificate)
            .expect("terminal transfer");
        Ingress::new(&mut h3)
            .try_receive(&mut client)
            .expect("terminal ingress");
        client.with_scope(|mut scope| {
            let event = h3
                .ingress()
                .consume(&mut scope, |event| {
                    matches!(event, Event::Reset { stream_id, error_code, origin, .. }
                    if *stream_id == id && *error_code == code
                        && *origin == if parsed { Origin::Transport } else { Origin::Wire })
                })
                .expect("terminal consumption");
            assert!(matches!(
                (reset, event),
                (true, Step::Item(true)) | (false, Step::Idle)
            ));
            assert!(scope.streams().resident().handle(id).is_none());
        });
    }
}
