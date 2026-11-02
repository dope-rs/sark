use std::{cell::Cell, iter, ops::ControlFlow};

use sark_h2::conn::event::Event;

use crate::common::headers::entry;

pub mod common;

use sark_core::manifold::{
    server::Connection as _,
    stream::{Boundary, Control, Write as _},
};
use sark_h2::{
    ClientRole, ServerRole,
    conn::{
        DataBlock, Error,
        config::{self, Config, Settings},
        machine::Connection,
    },
    frame::{
        Ack, Block,
        layout::{Header, Headers},
        numbers::{SettingId, Type},
        payload::Continuation,
    },
    hpack::encode::Encoder,
    stream::Id,
};
use sark_http::field::{self, metadata::map::record::Entry};

use crate::common::{
    capacity,
    headers::Counted,
    protocol::{Session, conn::Controls},
};

#[test]
fn rejection_keeps_hpack() {
    let mut client = Connection::<ClientRole>::try_new().expect("construct the HPACK client");
    let id = capacity::try_start(&mut client).expect("capacity fixture");
    let mut server = Connection::<ServerRole>::with_config(Config {
        outbound_capacity: 64,
        ..Config::default()
    })
    .expect("construct the constrained HPACK server");
    client
        .ingress()
        .ingest(server.output().first())
        .expect("ingest the server SETTINGS frame");
    while client.ingress().poll_event().is_some() {}
    server.transfer().drain(usize::MAX);
    server
        .ingress()
        .ingest(client.output().first())
        .expect("ingest the request HEADERS frame");
    while server.ingress().poll_event().is_some() {}
    client
        .ingress()
        .ingest(server.output().first())
        .expect("ingest the server SETTINGS acknowledgement");
    while client.ingress().poll_event().is_some() {}
    server.transfer().drain(usize::MAX);

    server.control().ping([1; 8]).expect("queue ping one");
    server.control().ping([2; 8]).expect("queue ping two");
    server.control().ping([3; 8]).expect("queue ping three");
    let headers = [
        field::literal!(entry(b":status", b"200")),
        field::literal!(entry(b"x-backpressure", b"retained")),
    ];
    let error = server
        .streams()
        .headers()
        .respond(id, headers, Boundary::More);
    assert!(
        error.as_ref().is_err_and(|error| matches!(error,
            Error::OutputAppend(cause)
                if cause.into_parts().0.get() > 13 && cause.into_parts().1 == 13
        )),
        "response output admission must fail"
    );

    server.transfer().drain(usize::MAX);
    server
        .streams()
        .headers()
        .respond(id, headers, Boundary::More)
        .expect("encode the response after releasing output capacity");
    let wire = server.output().first();
    let frame = Header::parse(wire).expect("parse the accepted response HEADERS frame");
    assert!((frame.kind) == (Type::Headers));
    assert_ne!(wire[sark_h2::frame::HEADER_LEN] & 0xe0, 0x20);
    client
        .ingress()
        .ingest(wire)
        .expect("decode the accepted response HEADERS frame");
    let decoded = iter::from_fn(|| client.ingress().poll_event()).find_map(|event| match event {
        Event::Headers { head, .. } => Some(head.into_parts().0),
        _ => None,
    });
    assert!(
        decoded
            .expect("observe the decoded response headers")
            .iter()
            .any(|header| { header.name == b"x-backpressure" && header.value == b"retained" })
    );
}

#[test]
fn prepared_header_owner() {
    for discard in [false, true] {
        let (mut client, mut server, id) = capacity::Upload::new(Config {
            outbound_capacity: 64,
            ..Config::default()
        })
        .try_connect()
        .expect("capacity fixture");
        Session::new(&mut client).try_prime().expect("peer preface");
        let other = capacity::try_start(&mut client).expect("second request");
        server
            .ingress()
            .ingest(client.output().first())
            .expect("second request headers");
        while server.ingress().poll_event().is_some() {}
        let key = server.status().stream_key(id).expect("first stream");
        let next = server.status().stream_key(other).expect("second stream");
        for opaque in 0..3 {
            server.control().ping([opaque; 8]).expect("fill output");
        }
        let headers = [
            field::literal!(entry(b":status", b"200")),
            field::literal!(entry(b"x-backpressure", b"retained")),
        ];
        let calls = Cell::new(0);
        let emits = Cell::new(0);
        assert!(matches!(
            server.writer(key).headers(
                Counted {
                    calls: &calls,
                    source: headers
                },
                Boundary::End
            ),
            Ok(Control::Prepared(DataBlock::Egress))
        ));
        assert!(matches!(
            server.writer(next).headers(
                Counted {
                    calls: &emits,
                    source: headers
                },
                Boundary::End,
            ),
            Ok(Control::Blocked(DataBlock::Egress))
        ));
        server.writer(next).discard();
        for _ in 0..2 {
            assert!(matches!(
                server.writer(key).commit(),
                Ok(ControlFlow::Break(DataBlock::Egress))
            ));
        }
        assert_eq!((calls.get(), emits.get()), (1, 0));
        let settings = Controls::new(Ack::Required)
            .try_settings(&[(SettingId::HeaderTableSize as u16, 2048)])
            .expect("table-size settings");
        assert!(matches!(
            server.ingress().ingest(&settings),
            Err(Error::HeaderBusy)
        ));
        assert_eq!(server.settings().peer().header_table_size, 4096);
        if discard {
            server.writer(key).discard();
        }
        server.transfer().drain(usize::MAX);
        let expected = if discard {
            server.ingress().resume().expect("settings after discard");
            assert!(matches!(
                server.writer(next).headers(
                    Counted {
                        calls: &emits,
                        source: headers
                    },
                    Boundary::End,
                ),
                Ok(Control::Written)
            ));
            other
        } else {
            assert!(matches!(
                server.writer(key).commit(),
                Ok(ControlFlow::Continue(()))
            ));
            server.ingress().resume().expect("settings after commit");
            id
        };
        assert_eq!(server.settings().table_limit(), 2048);
        assert_eq!((calls.get(), emits.get()), (1, usize::from(discard)));
        let (first, second) = server.output().slices();
        for wire in [first, second] {
            client
                .ingress()
                .ingest(wire)
                .expect("decode accepted headers");
        }
        let mut decoded: Vec<_> = iter::from_fn(|| client.ingress().poll_event())
            .filter_map(|event| match event {
                Event::Headers {
                    stream_id: stream,
                    head,
                    ..
                } => Some((stream, head.into_parts().0)),
                _ => None,
            })
            .collect();
        assert_eq!(decoded.len(), 1);
        let (stream, fields) = decoded.pop().expect("one accepted header block");
        assert_eq!(stream.id(), expected);
        assert_eq!(
            fields
                .iter()
                .map(|field| (field.name, field.value))
                .collect::<Vec<_>>(),
            [
                (b":status".as_slice(), b"200".as_slice()),
                (b"x-backpressure".as_slice(), b"retained".as_slice())
            ]
        );
    }
}

#[test]
fn oversized_header_rolls_back() {
    let headers = [
        field::literal!(entry(b":status", b"200")),
        field::literal!(entry(b"x-backpressure", b"retained")),
    ];
    let oversized = [
        headers[0],
        headers[1],
        field::literal!(entry(b"x-large", &[0xff; 256])),
    ];
    // The smaller block pool selects retained output; the larger one selects copying.
    for header_list in [400, 16_384] {
        let (mut client, mut server, id) = capacity::Upload::new(Config {
            outbound_capacity: 64,
            hpack_encoder: config::capacity::Encoder {
                header_list,
                ..Default::default()
            },
            ..Config::default()
        })
        .try_connect()
        .expect("capacity fixture");
        Session::new(&mut client).try_prime().expect("peer preface");
        let key = server.status().stream_key(id).expect("request stream");
        let calls = Cell::new(0);
        let error = server
            .writer(key)
            .headers(
                Counted {
                    calls: &calls,
                    source: oversized,
                },
                Boundary::End,
            )
            .err()
            .expect("an oversized block cannot become temporary pressure");
        assert!(!error.is_pressure());
        let Error::HeaderWire(cause) = error else {
            panic!("expected exact wire capacity error: {error:?}");
        };
        let (required, available) = cause.into_parts();
        assert!((256..400).contains(&required.get()));
        assert_eq!(available, 64);
        assert_eq!(calls.get(), 1);
        assert!(server.output().is_empty());
        assert!(matches!(
            server.writer(key).headers(headers, Boundary::End),
            Ok(Control::Written)
        ));
        let (first, second) = server.output().slices();
        for wire in [first, second] {
            client
                .ingress()
                .ingest(wire)
                .expect("decode the response after HPACK rollback");
        }
        let decoded = iter::from_fn(|| client.ingress().poll_event())
            .find_map(|event| match event {
                Event::Headers {
                    stream_id, head, ..
                } => Some((stream_id, head.into_parts().0)),
                _ => None,
            })
            .expect("one accepted response");
        assert_eq!(decoded.0.id(), id);
        assert_eq!(
            decoded
                .1
                .iter()
                .map(|field| (field.name, field.value))
                .collect::<Vec<_>>(),
            [
                (b":status".as_slice(), b"200".as_slice()),
                (b"x-backpressure".as_slice(), b"retained".as_slice())
            ]
        );
    }
}

#[test]
fn reset_commits_hpack_once() {
    let mut server = Connection::<ServerRole>::with_config(Config {
        outbound_capacity: 60,
        ..Config::default()
    })
    .expect("construct the control-slack server");
    Session::new(&mut server)
        .try_prime()
        .expect("client preface and settings");
    for opaque in 0..3 {
        server
            .control()
            .ping([opaque; 8])
            .expect("reserve output with a ping");
    }
    assert_eq!(server.output().len(), 51);

    let mut encoder = Encoder::try_new(4096).expect("HPACK encoder capacity");
    let valid = [
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/"),
        Entry::new(b"x-base", b"good"),
    ];
    server
        .ingress()
        .ingest(
            &capacity::Fields::new(Id::FIRST_CLIENT, &valid)
                .try_encode(&mut encoder)
                .expect("capacity fixture"),
        )
        .expect("ingest the initial valid header block");
    while server.ingress().poll_event().is_some() {}

    let invalid = [Entry::new(b"x-shift", b"discarded")];
    assert!(
        server.ingress().ingest(
            &capacity::Fields::new(Id::new(3).expect("construct stream three"), &invalid)
                .try_encode(&mut encoder)
                .expect("capacity fixture")
        ) == Err(Error::OutputFull {
            used: 64,
            incoming: 0,
            limit: 60,
        })
    );
    assert_eq!(server.output().len(), 64);
    assert_eq!(
        capacity::Wire::new(server.output().first())
            .try_count(Type::RstStream)
            .expect("capacity fixture"),
        1
    );
    server.transfer().drain(usize::MAX);
    server
        .ingress()
        .resume()
        .expect("resume after emitting the stream reset");
    assert_eq!(server.output().len(), 0);

    let next = capacity::Fields::new(Id::new(5).expect("construct stream five"), &valid)
        .try_encode(&mut encoder)
        .expect("capacity fixture");
    server
        .ingress()
        .ingest(&next)
        .expect("decode the next valid header block");
    let headers = iter::from_fn(|| server.ingress().poll_event()).find_map(|event| match event {
        Event::Headers { head, .. } => Some(head.into_parts().0),
        _ => None,
    });
    assert!(
        headers
            .expect("observe the decoded valid headers")
            .iter()
            .any(|header| header.name == b"x-base" && header.value == b"good")
    );
}

#[test]
fn refusal_advances_hpack() {
    let mut settings = Settings::DEFAULT;
    settings.max_concurrent_streams = Some(1);
    let mut server = Connection::<ServerRole>::with_config(Config {
        local_settings: settings,
        recv_window_target: 65_535,
        ..Config::default()
    })
    .expect("construct the one-stream server");
    Session::new(&mut server)
        .try_prime()
        .expect("client preface and settings");

    let mut encoder = Encoder::try_new(4096).expect("HPACK encoder capacity");
    let base = [
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/"),
        Entry::new(b"x-base", b"good"),
    ];
    server
        .ingress()
        .ingest(
            &capacity::Fields::new(Id::FIRST_CLIENT, &base)
                .try_encode(&mut encoder)
                .expect("capacity fixture"),
        )
        .expect("ingest the admitted header block");
    while server.ingress().poll_event().is_some() {}

    let refused = [
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/refused"),
        Entry::new(b"x-shift", b"discarded"),
    ];
    server
        .ingress()
        .ingest(
            &capacity::Fields::new(Id::new(3).expect("construct stream three"), &refused)
                .try_encode(&mut encoder)
                .expect("capacity fixture"),
        )
        .expect("decode and refuse the excess stream");
    assert_eq!(
        capacity::Wire::new(server.output().first())
            .try_count(Type::RstStream)
            .expect("capacity fixture"),
        1
    );
    server.transfer().drain(usize::MAX);

    server
        .streams()
        .headers()
        .respond(
            Id::FIRST_CLIENT,
            [field::literal!(entry(b":status", b"204"))],
            Boundary::End,
        )
        .expect("finish the admitted stream");
    server.transfer().drain(usize::MAX);

    server
        .ingress()
        .ingest(
            &capacity::Fields::new(Id::new(5).expect("construct stream five"), &base)
                .try_encode(&mut encoder)
                .expect("capacity fixture"),
        )
        .expect("decode the header block after refusal");
    let headers = iter::from_fn(|| server.ingress().poll_event()).find_map(|event| match event {
        Event::Headers { head, .. } => Some(head.into_parts().0),
        _ => None,
    });
    assert!(
        headers
            .expect("observe headers decoded after refusal")
            .iter()
            .any(|header| header.name == b"x-base" && header.value == b"good")
    );
}

#[test]
fn fragments_precede_reset() {
    let mut settings = Settings::DEFAULT;
    settings.max_concurrent_streams = Some(0);
    let mut server = Connection::<ServerRole>::with_config(Config {
        local_settings: settings,
        recv_window_target: 65_535,
        ..Config::default()
    })
    .expect("construct the refusing server");
    Session::new(&mut server)
        .try_prime()
        .expect("client preface and settings");

    let mut encoder = Encoder::try_new(4096).expect("HPACK encoder capacity");
    let headers = [
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/fragmented"),
        Entry::new(b"x-dynamic", b"decoded"),
    ];
    let mut block = Vec::new();
    encoder
        .try_encode(headers, &mut block)
        .expect("HPACK table");
    let split = 1;
    let id = Id::FIRST_CLIENT;
    let mut first = Vec::new();
    Headers::new(id, Boundary::End, Block::More, None, &block[..split])
        .expect("construct the first refused header fragment")
        .encode(&mut first)
        .expect("frame allocation");
    let mut last = Vec::new();
    Continuation::new(id, Block::End, &block[split..])
        .expect("construct the final refused header fragment")
        .encode(&mut last)
        .expect("frame allocation");

    server
        .ingress()
        .ingest(&first)
        .expect("ingest the first refused header fragment");
    assert_eq!(server.output().len(), 0);
    server
        .ingress()
        .ingest(&last)
        .expect("finish the refused header block");
    assert_eq!(
        capacity::Wire::new(server.output().first())
            .try_count(Type::RstStream)
            .expect("capacity fixture"),
        1
    );
    assert!(server.ingress().poll_event().is_none());
}
