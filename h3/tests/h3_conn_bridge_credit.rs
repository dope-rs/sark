use sark_h3::qpack::encoder::Encoder;
use sark_http::field::literal;

use crate::common::{
    conn::{
        bridge::{Pair, certificate},
        wire::{Ingress, Peer},
    },
    field::entry,
};

pub mod common;

use std::time::{Duration, Instant};

use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        event::{Event, Role},
        io::Step,
        options,
    },
    frame::{Payload, STREAM_TYPE_QPACK_ENCODER, TYPE_DATA, TYPE_HEADERS},
    manifold::Session,
};
use sark_http::payload::response::Body as _;

#[test]
fn blocked_credit_is_split() {
    let dynamic = literal!(entry(b"x-held-credit", b"retained"));
    let mut encoder = Encoder::try_bounded(256, 8).expect("fixture QPACK capacities must be valid");
    encoder.set_blocked(1);
    encoder
        .encode_for(8, [dynamic])
        .expect("fixture dynamic entry must be encodable");
    let setup = encoder.take_wire();
    let section = encoder
        .encode_for(
            0,
            [
                literal!(entry(b":method", b"POST")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/blocked")),
                dynamic,
            ],
        )
        .expect("blocked request section must be encodable");
    let mut block = Vec::new();
    section.write_prefix(&mut block);
    block.extend_from_slice(section.lines());
    let mut request = Vec::new();
    Payload::encode(TYPE_HEADERS, &block, &mut request)
        .expect("request HEADERS frame must be encodable");
    let credit = request.len() as u64;
    let mut data = Vec::new();
    Payload::encode(TYPE_DATA, b"after-progress", &mut data)
        .expect("request DATA frame must be encodable");
    let mut input = Vec::new();
    STREAM_TYPE_QPACK_ENCODER
        .encode(&mut input)
        .expect("varint");
    input.extend_from_slice(&setup);

    let Pair {
        mut server,
        mut client,
        mut space,
    } = Pair::try_with(
        Pair::try_credit(credit).expect("bridge credit"),
        Pair::try_credit(credit).expect("bridge credit"),
        certificate,
    )
    .expect("QUIC bridge fixture");
    let mut h3 = Session::with_config(
        Role::Server,
        options::Config {
            qpack: options::Qpack {
                decoder: options::Decode {
                    table_bytes: 256,
                    blocked_streams: 1,
                },
                ..options::Qpack::default()
            },
            ..options::Config::default()
        },
    )
    .expect("fixed bridge scenario must create a server session");

    let stream = client.with_scope(|mut scope| {
        let mut streams = scope.streams();
        let stream = streams
            .send()
            .open_bidi()
            .expect("request stream must be admissible");
        streams
            .send()
            .send(stream.send(), &request)
            .expect("request HEADERS bytes must be admissible");
        streams
            .send()
            .send(stream.send(), &data)
            .expect("request DATA bytes must be admissible");
        streams
            .send()
            .finish(stream.send())
            .expect("request stream must be finishable");
        stream.id()
    });
    assert_eq!(stream.get(), 0);

    let req_at = Instant::now();
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, req_at, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    server.with_scope(|mut scope| {
        assert!(matches!(
            h3.ingress()
                .consume(&mut scope, |_| ())
                .expect("blocked request"),
            Step::Idle
        ));
    });

    Peer::new(&mut space, &mut server)
        .try_send(&mut client, req_at, certificate)
        .expect("QUIC bridge transfer");
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, req_at, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    server.with_scope(|mut scope| {
        assert!(matches!(
            h3.ingress()
                .consume(&mut scope, |_| ())
                .expect("blocked request"),
            Step::Idle
        ));
    });

    let qpack_id = client.with_scope(|mut scope| {
        let mut streams = scope.streams();
        let qpack_id = streams
            .send()
            .open_uni()
            .expect("QPACK encoder stream must be admissible");
        streams
            .send()
            .send(qpack_id, &input)
            .expect("QPACK encoder bytes must be admissible");
        qpack_id.id()
    });
    assert_eq!(qpack_id.get(), 2);
    let enc_at = req_at + Duration::from_millis(1);
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, enc_at, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    server.with_scope(|mut scope| {
        let headers = h3.ingress().consume(&mut scope, |event| {
            matches!(event, Event::Headers { message, .. } if message.stream_id() == Id::ZERO)
        }).expect("unblocked request headers");
        assert!(matches!(headers, Step::Item(true)));
        assert!(matches!(
            h3.ingress()
                .consume(&mut scope, |_| ())
                .expect("server events"),
            Step::Idle
        ));
    });

    Peer::new(&mut space, &mut server)
        .try_send(&mut client, enc_at, certificate)
        .expect("QUIC bridge transfer");
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, enc_at, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    server.with_scope(|mut scope| {
        let data = h3
            .ingress()
            .consume(&mut scope, |event| {
                matches!(event, Event::Data { data, message }
                if data.segments().flatten().copied().eq(b"after-progress".iter().copied())
                    && matches!(message.boundary(), Boundary::End))
            })
            .expect("released request DATA");
        assert!(matches!(data, Step::Item(true)));
        assert!(matches!(
            h3.ingress()
                .consume(&mut scope, |_| ())
                .expect("server events"),
            Step::Idle
        ));
    });
}
