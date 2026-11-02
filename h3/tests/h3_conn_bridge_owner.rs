use sark_h3::{conn::Error, stream::Mismatch};
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

use dope_quic::{
    conn::stream::{self, access::bindings},
    wire::stream::Id,
};
use dope_test::alloc::Tracker;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        event::{Event, Role},
        io::Step,
        options,
    },
    manifold,
    manifold::Session,
};
use sark_http::{
    owner::{Change, cause},
    payload::response::Body as _,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn settings_round_trip() {
    let Pair {
        mut server,
        mut client,
        mut space,
    } = Pair::try_new(certificate).expect("QUIC bridge fixture");
    let mut h3 = Session::with_role(Role::Server)
        .expect("fixed bridge scenario must create a server session");
    let mut peer = Session::with_role(Role::Client)
        .expect("fixed bridge scenario must create a client session");

    h3.egress()
        .start(&mut server)
        .expect("server control stream must be admissible");
    peer.egress()
        .start(&mut client)
        .expect("client control stream must be admissible");

    let t1 = Instant::now();
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, t1, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");
    Peer::new(&mut space, &mut server)
        .try_send(&mut client, t1, certificate)
        .expect("QUIC bridge transfer");
    Ingress::new(&mut peer)
        .try_receive(&mut client)
        .expect("HTTP/3 bridge ingress");

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
    let stream = target.stream_id();
    let client_key = target.local().key();
    let mut target = peer
        .send()
        .bind(target)
        .expect("request target must belong to its H3 connection");
    let charge = target
        .headers(
            [
                literal!(entry(b":method", b"POST")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":authority", b"example.test")),
                literal!(entry(b":path", b"/pkg.Service/Method")),
            ],
            Boundary::More,
        )
        .expect("request headers must be encodable");
    target
        .data(b"hello", Boundary::End)
        .expect("request body must be encodable");
    assert!(peer.h3().retained(stream).expect("original header charge") == charge);
    peer.egress()
        .settle(&mut client)
        .expect("request output must settle");

    let t2 = Instant::now();
    Peer::new(&mut space, &mut client)
        .try_send(&mut server, t2, certificate)
        .expect("QUIC bridge transfer");
    let key = server.with_scope(|mut scope| {
        scope
            .streams()
            .resident()
            .key(stream)
            .expect("resident request stream")
    });
    Ingress::new(&mut h3)
        .try_receive(&mut server)
        .expect("HTTP/3 bridge ingress");

    server.with_scope(|mut scope| {
        let headers = h3.ingress().consume(&mut scope, |event| {
            matches!(event, Event::Headers { message, .. } if message.stream_id() == Id::ZERO)
        }).expect("request headers");
        assert!(matches!(headers, Step::Item(true)));
        let data = h3
            .ingress()
            .consume(&mut scope, |event| {
                matches!(event, Event::Data { message, data }
                if message.stream_id() == Id::ZERO
                    && matches!(message.boundary(), Boundary::End)
                    && data.segments().flatten().copied().eq(b"hello".iter().copied()))
            })
            .expect("request body");
        assert!(matches!(data, Step::Item(true)));
        assert!(matches!(
            h3.ingress()
                .consume(&mut scope, |_| ())
                .expect("server events"),
            Step::Idle
        ));
        let mut streams = scope.streams();
        let handle = streams
            .resident()
            .handle_key(key)
            .expect("response resident");
        assert_eq!(handle.id(), stream);
        assert_eq!(
            streams.binding().try_slot(handle).expect("app binding"),
            None
        );
        assert!(matches!(
            streams.binding().try_release(handle),
            Ok(bindings::Release::Vacant)
        ));
    });

    let ack_at = t2 + Duration::from_millis(30);
    Peer::new(&mut space, &mut server)
        .try_send(&mut client, ack_at, certificate)
        .expect("request ACK precedes response");
    client.with_scope(|mut scope| {
        let streams = scope.streams();
        let view = streams.view();
        let send = view.resolve_send(stream).expect("request send identity");
        assert_eq!(view.buffered(send), 0);
        assert!(streams.resident().handle_key(client_key).is_some());
    });

    h3.send()
        .headers(stream, [literal!(entry(b":status", b"200"))], Boundary::End)
        .expect("response headers");
    h3.egress().settle(&mut server).expect("response output");
    Peer::new(&mut space, &mut server)
        .try_send(&mut client, ack_at, certificate)
        .expect("response transfer");
    Ingress::new(&mut peer)
        .try_receive(&mut client)
        .expect("response keeps request identity");
    client.with_scope(|mut scope| {
        let response = peer
            .ingress()
            .consume(&mut scope, |event| {
                matches!(event, Event::Headers { message, .. }
                    if message.stream_id() == stream && message.boundary() == Boundary::End)
            })
            .expect("response completion");
        assert!(matches!(response, Step::Item(true)));
        let mut streams = scope.streams();
        if let Some(handle) = streams.resident().handle_key(client_key) {
            assert!(matches!(
                streams.binding().try_release(handle),
                Ok(bindings::Release::Vacant)
            ));
        }
    });
}

#[test]
fn qpack_limits() {
    for decoder in [8192, 0] {
        let Pair {
            mut server,
            mut client,
            mut space,
        } = Pair::try_new(certificate).expect("QUIC bridge fixture");
        let mut config = options::Config::default();
        config.qpack.encoder.table_bytes = 4096;
        config.qpack.decoder.table_bytes = 8192;
        let mut peer =
            Session::with_config(Role::Client, config.clone()).expect("peer QPACK configuration");
        config.qpack.decoder.table_bytes = decoder;
        let mut h3 =
            Session::with_config(Role::Server, config).expect("asymmetric QPACK configuration");
        h3.egress()
            .start(&mut server)
            .expect("server control stream");
        peer.egress()
            .start(&mut client)
            .expect("peer control stream");
        Peer::new(&mut space, &mut client)
            .try_send(&mut server, Instant::now(), certificate)
            .expect("peer SETTINGS transfer");
        Ingress::new(&mut h3)
            .try_receive(&mut server)
            .expect("encoder negotiates its own allocation");
    }
}

#[test]
fn foreign_target_rejected() {
    let Pair {
        client: mut first, ..
    } = Pair::try_new(certificate).expect("QUIC bridge fixture");
    let Pair {
        client: mut second, ..
    } = Pair::try_new(certificate).expect("QUIC bridge fixture");
    let mut h3 = Session::with_role(Role::Client)
        .expect("fixed bridge scenario must create a client session");
    let mut peer =
        Session::with_role(Role::Client).expect("fixed bridge scenario must create a peer session");
    let target = h3
        .egress()
        .open(&mut first)
        .expect("owned request stream must be admissible");

    peer.egress()
        .start(&mut second)
        .expect("peer control stream must be admissible");
    let expected = target.origin();
    let found = second.with_scope(|mut scope| scope.streams().resident().origin());
    let old_wire = peer.h3().wire_len();
    let old_size = peer.h3().resident_len();
    assert!(matches!(
        peer.send().bind(target),
        Err(Error::Target(Mismatch::Foreign {
            expected: actual_expected,
            found: actual_found,
        })) if actual_expected == expected && actual_found == found,
    ));
    assert_eq!(peer.h3().wire_len(), old_wire);
    assert_eq!(peer.h3().resident_len(), old_size);
}

#[test]
fn target_keeps_owner() {
    let Pair {
        client: mut first, ..
    } = Pair::try_new(certificate).expect("QUIC bridge fixture");
    let mut h3 = Session::with_role(Role::Client)
        .expect("fixed bridge scenario must create a client session");
    let target = h3
        .egress()
        .open(&mut first)
        .expect("owned request stream must be admissible");
    let origin = target.origin();
    let local = target.local();

    assert!(target.stream_id() == local.id());
    first.with_scope(|mut scope| {
        let streams = scope.streams();
        let resident = streams.resident();
        let owner = resident
            .owner(origin)
            .expect("origin must identify its opening connection");
        resident
            .bind(owner, local)
            .expect("original owner binds its resident stream");
    });
}

#[test]
fn foreign_flush_is_atomic() {
    let Pair {
        client: mut first, ..
    } = Pair::try_new(certificate).expect("QUIC bridge fixture");
    let Pair {
        client: mut second, ..
    } = Pair::try_new(certificate).expect("QUIC bridge fixture");
    let mut h3 = Session::with_role(Role::Client)
        .expect("fixed bridge scenario must create a client session");
    let target = h3
        .egress()
        .open(&mut first)
        .expect("owned request stream must be admissible");
    let stream = target.stream_id();
    let mut target = h3
        .send()
        .bind(target)
        .expect("request target must belong to its H3 connection");
    target
        .owned(vec![1, 2, 3], Boundary::End)
        .expect("owned request body must be admissible");
    let charge = h3
        .h3()
        .retained(stream)
        .expect("owned request body retention must be measurable");
    assert_eq!(charge.logical_bytes, 3);
    assert_eq!(charge.resident_bytes, 3);
    assert_eq!(charge.owners, 1);
    let retained = h3.h3().wire_len();
    assert!(retained > 0);

    assert!(matches!(
        h3.egress().flush(&mut second),
        Err(manifold::Error::Foreign(_)),
    ));
    assert_eq!(h3.h3().wire_len(), retained);
    assert!(
        h3.h3()
            .retained(stream)
            .expect("foreign flush preserves original charge")
            == charge
    );

    h3.egress()
        .settle(&mut first)
        .expect("owner connection must settle retained output");
    assert_eq!(h3.h3().wire_len(), 0);
    assert!(
        h3.h3()
            .retained(stream)
            .is_ok_and(|charge| charge.is_empty())
    );
}

#[test]
fn failed_flush_keeps_owner() {
    let Pair {
        client: mut quic, ..
    } = Pair::try_new(certificate).expect("QUIC bridge fixture");
    let mut h3 = Session::with_role(Role::Client).expect("client session");
    let id = Id::new(3).expect("remote unidirectional stream");
    h3.send()
        .owned(id, vec![1, 2, 3], Boundary::End)
        .expect("staged body");
    let charge = h3.h3().retained(id).expect("staged charge");
    let wire = h3.h3().wire_len();
    for _ in 0..2 {
        assert!(matches!(
            h3.egress().flush(&mut quic),
            Err(manifold::Error::Resident(stream::resident::Error::Stream(
                stream::Error::InvalidDirection
            )))
        ));
        assert_eq!(h3.h3().wire_len(), wire);
        assert_eq!(h3.h3().resident_len(), charge.resident_bytes);
        assert_eq!(h3.h3().retained(id).expect("restored charge"), charge);
    }
    let mut released = 0;
    h3.close(|effect| {
        assert_eq!(*effect.key(), id);
        assert_eq!(effect.charge(), charge);
        assert!(matches!(
            effect.change(),
            Change::StagingReleased(cause::Staging::Closed)
        ));
        released += 1;
    });
    assert_eq!(released, 1);
}

#[test]
fn decoder_control_is_exact() {
    let Pair { mut server, .. } = Pair::try_new(certificate).expect("QUIC bridge fixture");
    let mut h3 = Session::with_config(
        Role::Server,
        options::Config {
            qpack: options::Qpack {
                decoder: options::Decode {
                    table_bytes: 256,
                    ..options::Decode::default()
                },
                ..options::Qpack::default()
            },
            ..options::Config::default()
        },
    )
    .expect("fixed QPACK decoder configuration must be valid");

    let control = h3
        .egress()
        .start(&mut server)
        .expect("server control stream must be admissible");

    assert_eq!(control.get(), 3);
}

#[test]
fn control_has_no_alloc() {
    let Pair { mut server, .. } = Pair::try_new(certificate).expect("QUIC bridge fixture");
    let mut h3 = Session::with_role(Role::Server)
        .expect("fixed bridge scenario must create a server session");

    let (control, allocs) = Tracker::count(|| h3.egress().start(&mut server));

    assert_eq!(
        control
            .expect("server control stream must be admissible")
            .get(),
        3
    );
    assert_eq!(allocs, 0);
}
