pub mod common;

use std::time::{Duration, Instant};

use common::{
    conn::{
        bridge::Pair,
        wire::{Ingress, Peer},
    },
    field::entry,
    ledger::Section as _,
};
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
    qpack::encoder::Encoder,
};
use sark_http::{field::literal, payload::response::Body as _};

#[test]
fn encoder_resumes_request() {
    let dynamic = literal!(entry(b"x-resume", b"retained"));
    let mut encoder = Encoder::try_bounded(256, 8).expect("encoder");
    encoder.set_blocked(1);
    encoder.encode_for(8, [dynamic]).expect("dynamic entry");
    let setup = encoder.take_wire();
    let section = encoder
        .encode_for(
            0,
            [
                literal!(entry(b":method", b"POST")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/resume")),
                dynamic,
            ],
        )
        .expect("blocked request");
    let mut head = Vec::new();
    Payload::encode(TYPE_HEADERS, &section.into_wire(), &mut head).expect("HEADERS frame");
    let mut data = Vec::new();
    Payload::encode(TYPE_DATA, b"ready", &mut data).expect("DATA frame");
    let mut input = Vec::new();
    STREAM_TYPE_QPACK_ENCODER
        .encode(&mut input)
        .expect("encoder stream type");
    input.extend_from_slice(&setup);

    let Pair {
        mut server,
        mut client,
        mut space,
    } = Pair::try_new(|_| {}).expect("QUIC pair");
    let mut config = options::Config::default();
    config.streams.capacity = 1;
    config.qpack.decoder.table_bytes = 256;
    config.qpack.decoder.blocked_streams = 1;
    let mut h3 = Session::with_config(Role::Server, config).expect("H3 server");
    let id = client.with_scope(|mut scope| {
        scope
            .streams()
            .send()
            .open_bidi()
            .expect("request stream")
            .id()
    });
    assert_eq!(id, Id::ZERO);
    let mut now = Instant::now();
    for (bytes, finish) in [(&head, false), (&data, true)] {
        client.with_scope(|mut scope| {
            let mut streams = scope.streams();
            let send = streams.view().resolve_send(id).expect("request stream");
            streams.send().send(send, bytes).expect("request bytes");
            if finish {
                streams.send().finish(send).expect("request FIN");
            }
        });
        now = client
            .view()
            .send_at(now + Duration::from_millis(1));
        Peer::new(&mut space, &mut client)
            .try_send(&mut server, now, |_| {})
            .expect("request transfer");
        Ingress::new(&mut h3)
            .try_receive(&mut server)
            .expect("blocked ingress");
        server.with_scope(|mut scope| {
            assert!(matches!(
                h3.ingress().consume(&mut scope, |_| ()),
                Ok(Step::Idle)
            ));
        });
    }
    client.with_scope(|mut scope| {
        let mut streams = scope.streams();
        let send = streams.send().open_uni().expect("encoder stream");
        assert_eq!(send.id().get(), 2);
        streams.send().send(send, &input).expect("encoder bytes");
    });
    now = client
        .view()
        .send_at(now + Duration::from_millis(1));
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, now, |_| {})
        .expect("encoder transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("encoder ingress");
    server.with_scope(|mut scope| {
        let head = h3
            .ingress()
            .consume(&mut scope, |event| {
                matches!(event, Event::Headers { message, .. }
                    if message.stream_id() == id && message.boundary() == Boundary::More)
            })
            .expect("consume headers");
        assert!(matches!(head, Step::Item(true)));
        let data = h3
            .ingress()
            .consume(&mut scope, |event| {
                matches!(event, Event::Data { message, data }
                    if message.stream_id() == id && message.boundary() == Boundary::End
                        && data.segments().flatten().copied().eq(b"ready".iter().copied()))
            })
            .expect("consume resumed DATA");
        assert!(matches!(data, Step::Item(true)));
    });
}
