use sark_h3::conn::Error;
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

use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        event::{Event, Role},
        io::Step,
        options,
    },
    link::Flush,
    manifold::Session,
};
use sark_http::payload::response::Body as _;

#[test]
fn settle_preserves_write_quantum() {
    for (limit, flush, remaining) in [(192, Flush::Blocked, 1), (195, Flush::Drained, 0)] {
        let options = Pair::try_config()
            .expect("QUIC options")
            .into_builder()
            .streams(|streams| streams.send_bytes(limit))
            .try_build()
            .expect("write budget");
        let Pair {
            client: mut quic, ..
        } = Pair::try_with(
            Pair::try_config().expect("QUIC options"),
            options,
            certificate,
        )
        .expect("QUIC pair");
        let mut session = Session::new().expect("H3 session");
        let target = session.egress().open(&mut quic).expect("request stream");
        let id = target.stream_id();
        {
            let mut stream = session.send().bind(target).expect("request owner");
            for _ in 0..65 {
                stream
                    .owned(vec![1], Boundary::More)
                    .expect("staged output");
            }
        }
        assert!(matches!(
            session.egress().flush(&mut quic),
            Ok(Flush::Pending)
        ));
        let held = session.h3().retained(id).expect("pending ownership");
        assert_eq!(
            (held.logical_bytes, held.resident_bytes, held.owners),
            (33, 33, 33)
        );
        assert!(session.egress().settle(&mut quic).expect("settled output") == flush);
        let held = session.h3().retained(id).expect("remaining ownership");
        assert_eq!(
            (held.logical_bytes, held.resident_bytes, held.owners),
            (remaining, remaining, remaining)
        );
        assert_eq!(session.h3().wire_len(), 3 * remaining);
    }
}

#[test]
fn ack_wakes_retry() {
    const DATA_WIRE_BYTES: usize = 10;

    let opts = Pair::try_config()
        .expect("bridge options")
        .into_builder()
        .streams(|streams| {
            streams
                .send_bytes(DATA_WIRE_BYTES)
                .resident(64)
                .send_parts(4)
        })
        .try_build()
        .expect("bridge backpressure stream limits");
    let Pair {
        mut server,
        mut client,
        mut space,
    } = Pair::try_with(
        opts,
        Pair::try_config().expect("bridge options"),
        certificate,
    )
    .expect("QUIC bridge fixture");
    let mut h3 = Session::with_config(
        Role::Server,
        options::Config {
            ingress: options::Ingress {
                field_section_bytes: 16_383,
                ..options::Ingress::default()
            },
            egress: options::Egress {
                wire_bytes: DATA_WIRE_BYTES + 1,
                ..options::Egress::default()
            },
            ..options::Config::default()
        },
    )
    .expect("fixed bridge scenario must create a server session");
    let mut peer = Session::with_role(Role::Client)
        .expect("fixed bridge scenario must create a client session");

    h3.egress()
        .start(&mut server)
        .expect("server control stream must be admissible");
    peer.egress()
        .start(&mut client)
        .expect("client control stream must be admissible");
    let set_at = Instant::now() + Duration::from_secs(1);
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, set_at, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    Peer::new(&mut space, &mut server)
        .try_send(&mut client, set_at, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut peer)
        .try_receive(&mut client)
        .expect("HTTP/3 bridge ingress");
    let set_ack = client
        .view()
        .recovery()
        .send_at(set_at + Duration::from_millis(1));
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, set_ack, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    assert_eq!(
        server.view().with_streams(|view| view.usage().wire_bytes()),
        0
    );
    assert_eq!(
        server.view().with_streams(|view| view.usage().cached()),
        0,
        "H3 ownership-transfer output must never enter QUIC's copy cache",
    );

    server.with_scope(|mut scope| {
        let settings = h3
            .ingress()
            .consume(&mut scope, |event| matches!(event, Event::Settings(_)))
            .expect("server SETTINGS");
        assert!(matches!(settings, Step::Item(true)));
    });
    client.with_scope(|mut scope| {
        let settings = peer
            .ingress()
            .consume(&mut scope, |event| matches!(event, Event::Settings(_)))
            .expect("client SETTINGS");
        assert!(matches!(settings, Step::Item(true)));
    });

    let target = peer
        .egress()
        .open(&mut client)
        .expect("request stream must be admissible");
    let request = target.stream_id();
    let mut target = peer
        .send()
        .bind(target)
        .expect("request target must belong to its H3 connection");
    let charge = target
        .headers(
            [
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/backpressure")),
            ],
            Boundary::End,
        )
        .expect("request headers must be encodable");
    assert!(
        peer.h3()
            .retained(request)
            .expect("original retained charge")
            == charge
    );
    assert!(
        matches!(peer.egress().flush(&mut client), Ok(Flush::Drained)),
        "request headers must flush"
    );
    let req_at = set_ack + Duration::from_millis(1);
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, req_at, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    server.with_scope(|mut scope| {
        let headers = h3
            .ingress()
            .consume(&mut scope, |event| {
                matches!(event, Event::Headers { message, .. }
                if message.stream_id() == request && matches!(message.boundary(), Boundary::End))
            })
            .expect("request headers");
        assert!(matches!(headers, Step::Item(true)));
        assert!(matches!(
            h3.ingress()
                .consume(&mut scope, |_| ())
                .expect("server events"),
            Step::Idle
        ));
    });

    let charge = h3
        .send()
        .headers(
            request,
            [literal!(entry(b":status", b"200"))],
            Boundary::More,
        )
        .expect("response headers must be encodable");
    assert!(h3.h3().retained(request).expect("original retained charge") == charge);
    assert!(
        matches!(h3.egress().flush(&mut server), Ok(Flush::Drained)),
        "response headers must flush"
    );
    let head_at = req_at + Duration::from_millis(1);
    Peer::new(&mut space, &mut server)
        .try_send(&mut client, head_at, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut peer)
        .try_receive(&mut client)
        .expect("HTTP/3 bridge ingress");
    let head_ack = client
        .view()
        .recovery()
        .send_at(head_at + Duration::from_millis(1));
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, head_ack, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    assert_eq!(
        server.view().with_streams(|view| view.usage().wire_bytes()),
        0
    );
    assert_eq!(server.view().with_streams(|view| view.usage().cached()), 0);
    client.with_scope(|mut scope| {
        let headers = peer.ingress().consume(&mut scope, |event| {
            matches!(event, Event::Headers { message, .. } if message.stream_id() == request)
        }).expect("response headers");
        assert!(matches!(headers, Step::Item(true)));
    });

    h3.send()
        .owned(request, vec![1; 8], Boundary::More)
        .expect("first response segment must be admissible");
    assert!(
        matches!(h3.egress().flush(&mut server), Ok(Flush::Drained)),
        "first response segment must flush"
    );
    assert_eq!(
        server.view().with_streams(|view| view.usage().wire_bytes()),
        10
    );
    assert_eq!(
        server.view().with_streams(|view| view.usage().segments()),
        2
    );

    h3.send()
        .owned(request, vec![2; 8], Boundary::More)
        .expect("second response segment must enter H3 staging");
    assert!(
        matches!(h3.egress().flush(&mut server), Ok(Flush::Blocked)),
        "saturated response flush must report backpressure"
    );
    assert_eq!(
        server.view().with_streams(|view| view.usage().wire_bytes()),
        10
    );
    assert_eq!(
        server.view().with_streams(|view| view.usage().segments()),
        2
    );
    assert_eq!(h3.h3().wire_len(), 10);
    assert_eq!(h3.h3().resident_len(), 8);
    let held = h3
        .h3()
        .retained(request)
        .expect("blocked response owner charge must be measurable");
    assert_eq!(held.logical_bytes, 8);
    assert_eq!(held.resident_bytes, 8);
    assert_eq!(held.owners, 1);
    assert!(
        h3.send().owned(request, vec![3], Boundary::End)
            == Err(Error::BudgetWire {
                retained: 10,
                additional: 3,
                limit: DATA_WIRE_BYTES + 1,
            }),
        "the owner waiting below QUIC must remain charged to H3",
    );
    assert!(h3.h3().retained(request).expect("original retained charge") == held);

    let first_at = head_ack + Duration::from_millis(1);
    Peer::new(&mut space, &mut server)
        .try_send(&mut client, first_at, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut peer)
        .try_receive(&mut client)
        .expect("HTTP/3 bridge ingress");
    client.with_scope(|mut scope| {
        let data = peer
            .ingress()
            .consume(&mut scope, |event| {
                matches!(event, Event::Data { message, data }
                if message.stream_id() == request
                    && message.boundary() == Boundary::More
                    && data.segments().flatten().copied().eq([1; 8].iter().copied()))
            })
            .expect("first response segment");
        assert!(matches!(data, Step::Item(true)));
    });
    let ack_one = client
        .view()
        .recovery()
        .send_at(first_at + Duration::from_millis(1));
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, ack_one, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    assert_eq!(
        server.view().with_streams(|view| view.usage().wire_bytes()),
        10
    );
    assert_eq!(
        server.view().with_streams(|view| view.usage().cached()),
        0,
        "ACK must release the first H3 owners instead of retaining them as cache",
    );
    assert_eq!(h3.h3().wire_len(), 0);
    assert_eq!(h3.h3().resident_len(), 0);
    assert!(
        h3.h3()
            .retained(request)
            .is_ok_and(|charge| charge.is_empty())
    );
    assert!(
        matches!(h3.egress().flush(&mut server), Ok(Flush::Drained)),
        "ACK retry must flush"
    );

    h3.send()
        .owned(request, vec![3], Boundary::End)
        .expect("the accepted retry releases its exact H3 staging charge");
    assert!(
        matches!(h3.egress().flush(&mut server), Ok(Flush::Blocked)),
        "terminal response segment must report backpressure"
    );
    let tail = h3
        .h3()
        .retained(request)
        .expect("terminal response owner charge must be measurable");
    assert_eq!(tail.logical_bytes, 1);
    assert_eq!(tail.resident_bytes, 1);
    assert_eq!(tail.owners, 1);

    let next_at = ack_one + Duration::from_millis(1);
    Peer::new(&mut space, &mut server)
        .try_send(&mut client, next_at, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut peer)
        .try_receive(&mut client)
        .expect("HTTP/3 bridge ingress");
    client.with_scope(|mut scope| {
        let data = peer
            .ingress()
            .consume(&mut scope, |event| {
                matches!(event, Event::Data { message, data }
                if message.stream_id() == request
                    && message.boundary() == Boundary::More
                    && data.segments().flatten().copied().eq([2; 8].iter().copied()))
            })
            .expect("second response segment");
        assert!(matches!(data, Step::Item(true)));
    });
    let ack_two = client
        .view()
        .recovery()
        .send_at(next_at + Duration::from_millis(1));
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, ack_two, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    assert_eq!(
        server.view().with_streams(|view| view.usage().wire_bytes()),
        3
    );
    assert_eq!(server.view().with_streams(|view| view.usage().cached()), 0);
    assert!(
        h3.h3()
            .retained(request)
            .is_ok_and(|charge| charge.is_empty())
    );

    let last_at = ack_two + Duration::from_millis(1);
    Peer::new(&mut space, &mut server)
        .try_send(&mut client, last_at, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut peer)
        .try_receive(&mut client)
        .expect("HTTP/3 bridge ingress");
    client.with_scope(|mut scope| {
        let data = peer
            .ingress()
            .consume(&mut scope, |event| {
                matches!(event, Event::Data { message, data }
                if message.stream_id() == request
                    && matches!(message.boundary(), Boundary::End)
                    && data.segments().flatten().copied().eq([3].iter().copied()))
            })
            .expect("terminal response segment");
        assert!(matches!(data, Step::Item(true)));
        assert!(matches!(
            peer.ingress()
                .consume(&mut scope, |_| ())
                .expect("client events"),
            Step::Idle
        ));
    });
    let ack_last = client
        .view()
        .recovery()
        .send_at(last_at + Duration::from_millis(1));
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, ack_last, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    assert_eq!(
        server.view().with_streams(|view| view.usage().wire_bytes()),
        0
    );
    assert_eq!(server.view().with_streams(|view| view.usage().active()), 0);
    assert_eq!(server.view().with_streams(|view| view.usage().cached()), 0);
    server.with_scope(|mut scope| {
        assert!(scope.streams().resident().handle(request).is_none());
    });
    assert!(
        h3.h3()
            .retained(request)
            .is_ok_and(|charge| charge.is_empty())
    );
    assert!(server.view().lifecycle().is_ready() && client.view().lifecycle().is_ready());
}
