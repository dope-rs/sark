use core::slice;

use dope_quic::wire::stream::Id;
use o3::depot::capacity;
use sark_h3::frame::Header;
use sark_http::field::literal;

use crate::common::{
    conn::{Connection as _, Events, build::Settings as _},
    field::entry,
    message::{
        Frame as _,
        capacity::Capacity as _,
        promise::{Authority, Push as _},
        qpack::{Kind, Wire as _},
    },
    stream::Stream as _,
};

pub mod common;

use std::num::NonZero;

use dope_quic::wire::varint::VarInt;
use sark_core::manifold::stream::{Boundary, Section};
use sark_h3::{
    conn::{
        Connection, Error,
        event::{Event, Role},
        io::{Receive, Send},
        options::{Config, Decode, Egress, Ingress, Qpack, plan},
    },
    frame::{Settings, TYPE_HEADERS},
    qpack::{decoder, dynamics, encoder},
};
use sark_http::field::metadata::{block::Connect, integer, section::rules::RetainRaw};

#[test]
fn waiting_section_preserves_required_count() {
    use common::qpack::Instruction;

    let mut wire = vec![2];
    Instruction::Capacity(64)
        .try_encode(&mut wire)
        .expect("table capacity");
    Instruction::Insert {
        name: b":status",
        value: b"200",
    }
    .try_encode(&mut wire)
    .expect("first entry");
    let first = wire.len();
    for value in [b"201", b"202", b"203", b"204"] {
        Instruction::Insert {
            name: b":status",
            value,
        }
        .try_encode(&mut wire)
        .expect("replacement entry");
    }
    for initial in [0, first] {
        let mut conn = Connection::with_config(
            Role::Client,
            Config {
                qpack: Qpack {
                    decoder: Decode {
                        table_bytes: 64,
                        blocked_streams: 1,
                    },
                    ..Qpack::default()
                },
                egress: Egress {
                    wire_bytes: 2,
                    ..Egress::default()
                },
                ..Config::default()
            },
        )
        .expect("connection");
        conn.control()
            .open_decoder(Id::new(2).expect("decoder id"))
            .expect("decoder stream");
        conn.events()
            .poll_write()
            .expect("decoder prefix")
            .expect("decoder prefix");
        conn.control()
            .open_encoder(Id::new(6).expect("encoder id"))
            .expect("occupy one output byte");
        let encoder = Id::new(3).expect("peer encoder id");
        if initial != 0 {
            Receive::new(&mut conn)
                .borrowed(encoder, &wire[..initial], Boundary::More)
                .expect("first insertion");
        }
        Receive::new(&mut conn)
            .borrowed(Id::ZERO, &[1, 3, 2, 0, 0x80], Boundary::More)
            .expect("waiting field section");
        assert!(Receive::new(&mut conn).is_blocked(Id::ZERO));
        Receive::new(&mut conn)
            .borrowed(encoder, &wire[initial..], Boundary::More)
            .expect("insertions before resuming the section");
        assert!(matches!(
            conn.events().poll_write(),
            Err(Error::Qpack(decoder::Error::DynamicReference(
                dynamics::Reference::Evicted {
                    absolute: 0,
                    inserted: 5,
                    resident: 1,
                }
            )))
        ));
    }
}

#[test]
fn blocked_streams_resume() {
    let (block, code) = Kind::Request
        .try_blocked(Id::ZERO)
        .expect("message fixture");
    let mut conn = Connection::try_qpack(Role::Server).expect("message fixture");
    Receive::new(&mut conn)
        .owned(
            Id::new(0).expect("stream id"),
            (&block).try_frame(TYPE_HEADERS).expect("message fixture"),
            Boundary::End,
        )
        .expect("blocked request stream must be valid");
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("blocked request must poll")
            .is_none()
    );
    Receive::new(&mut conn)
        .owned(
            Id::new(2).expect("stream id"),
            code.try_wire().expect("encoder wire"),
            Boundary::More,
        )
        .expect("request encoder stream must be valid");
    assert!(
        matches!(Events::new(&mut conn).poll_token().expect("unblocked request must poll"), Some(Event::Headers {
            section: Section::Initial,
            message,
            ..
        }) if matches!(message.boundary(), Boundary::End))
    );
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("drained request must poll")
            .is_none()
    );

    let (block, code) = Kind::Response
        .try_blocked(Id::new(3).expect("push stream"))
        .expect("message fixture");
    let mut conn = plan::Config::new(Config::qpack())
        .expect("QPACK configuration")
        .build::<RetainRaw, Authority>(Role::Client)
        .expect("client push authority");
    conn.allow_push(Role::Client, 11).expect("push grant");
    Send::new(&mut conn)
        .headers(Id::ZERO, Authority, Boundary::End)
        .expect("originating GET request");
    assert!(
        conn.events()
            .poll_write()
            .expect("request output")
            .is_some()
    );
    Receive::new(&mut conn)
        .owned(
            Id::ZERO,
            VarInt::new(11)
                .expect("push id")
                .try_promise(&Authority.try_request().expect("promised GET"))
                .expect("push promise"),
            Boundary::More,
        )
        .expect("promise admission");
    assert!(matches!(
        Events::new(&mut conn)
            .poll_token()
            .expect("accepted promise"),
        Some(Event::PushPromise { push_id: 11, .. })
    ));
    Receive::new(&mut conn)
        .owned(
            Id::new(3).expect("stream id"),
            VarInt::new(11)
                .expect("push id")
                .try_stream(&(&block).try_frame(TYPE_HEADERS).expect("message fixture"))
                .expect("push wire"),
            Boundary::End,
        )
        .expect("blocked push stream must be valid");
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("blocked push must poll")
            .is_none()
    );
    Receive::new(&mut conn)
        .owned(
            Id::new(7).expect("stream id"),
            code.try_wire().expect("encoder wire"),
            Boundary::More,
        )
        .expect("push encoder stream must be valid");
    let event = Events::new(&mut conn)
        .poll_token()
        .expect("unblocked push must poll");
    assert!(
        matches!(&event, Some(Event::PushHeaders {
                push_id: 11,
                section: Section::Initial,
                message,
                ..
            }) if matches!(message.boundary(), Boundary::End)),
        "unblocked push must preserve its final initial headers"
    );
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("drained push must poll")
            .is_none()
    );
}

#[test]
fn fragments_release() {
    let (block, code) = Kind::Request
        .try_blocked(Id::ZERO)
        .expect("message fixture");
    let wire = (&block).try_frame(TYPE_HEADERS).expect("message fixture");
    let mut conn = Connection::with_config(
        Role::Server,
        Config {
            qpack: Qpack {
                decoder: Decode {
                    table_bytes: 256,
                    blocked_streams: 1,
                },
                ..Qpack::default()
            },
            ingress: Ingress {
                retained_bytes: wire.len() + 1,
                ..Ingress::default()
            },
            ..Config::default()
        },
    )
    .expect("fragmented QPACK receiver configuration must be valid");
    Receive::new(&mut conn)
        .owned(Id::new(0).expect("stream id"), wire, Boundary::More)
        .expect("blocked request frame must be valid");
    assert!(
        Events::new(&mut conn)
            .poll_token()
            .expect("blocked fragmented request must poll")
            .is_none()
    );

    let wire = code.try_wire().expect("encoder wire");
    for byte in &wire {
        Receive::new(&mut conn)
            .borrowed(
                Id::new(2).expect("stream id"),
                slice::from_ref(byte),
                Boundary::More,
            )
            .expect("each encoder fragment must extend valid instructions");
    }
    assert!(matches!(
        Events::new(&mut conn)
            .poll_token()
            .expect("released request must poll"),
        Some(Event::Headers {
            section: Section::Initial,
            ..
        })
    ));
}

#[test]
fn peer_bounds_capacity() {
    let mut conn = Connection::try_qpack(Role::Server).expect("message fixture");
    assert!(conn.control().set_capacity(0) == Err(Error::EncoderMissing));
    conn.control()
        .open_encoder(Id::new(3).expect("stream id"))
        .expect("server encoder stream id must be valid");
    assert!(
        conn.events()
            .poll_write()
            .expect("encoder stream prefix must poll")
            .is_some()
    );
    assert!(
        conn.control().set_capacity(1)
            == Err(Error::Encode(encoder::Error::Table(
                dynamics::Capacity::Maximum {
                    requested: 1,
                    maximum: 0,
                }
            )))
    );

    let peer = Settings::new(VarInt::from_u8(64), None, VarInt::ZERO, Connect::Disabled);
    Receive::new(&mut conn)
        .owned(
            Id::new(2).expect("stream id"),
            peer.try_wire().expect("SETTINGS wire"),
            Boundary::More,
        )
        .expect("fixed peer settings stream must be valid");
    assert!(
        matches!(Events::new(&mut conn).poll_token(), Ok(Some(Event::Settings(settings))) if settings == peer)
    );
    conn.control()
        .set_capacity(64)
        .expect("advertised peer capacity must be accepted");
    assert!(
        conn.control().set_capacity(65)
            == Err(Error::Encode(encoder::Error::Table(
                dynamics::Capacity::Maximum {
                    requested: 65,
                    maximum: 64,
                }
            )))
    );

    Send::new(&mut conn)
        .headers(Id::ZERO, [literal!(entry(b"x-a", b"a"))], Boundary::More)
        .expect("speculative insertion");
    while conn.events().poll_write().expect("queued writes").is_some() {}
    assert_eq!(
        conn.control().set_capacity(0),
        Err(Error::Encode(encoder::Error::Table(
            dynamics::Capacity::Protected {
                requested: 0,
                minimum: 36,
            }
        )))
    );
    Receive::new(&mut conn)
        .borrowed(Id::new(6).expect("decoder stream"), &[3, 1], Boundary::More)
        .expect("insertion receipt");
    conn.control()
        .set_capacity(0)
        .expect("received unreferenced entry can be evicted");
}

#[test]
fn capacity_change_atomic() {
    let mut conn = Connection::with_config(
        Role::Server,
        Config {
            egress: Egress {
                write_capacity: 2,
                ..Egress::default()
            },
            ..Config::default()
        },
    )
    .expect("bounded server configuration must be valid");
    conn.control()
        .open_encoder(Id::new(3).expect("stream id"))
        .expect("server encoder stream id must be valid");
    conn.control()
        .open(Id::new(7).expect("control stream"))
        .expect("critical queue full");
    let peer = Settings::new(VarInt::from_u8(64), None, VarInt::ZERO, Connect::Disabled);
    Receive::new(&mut conn)
        .owned(
            Id::new(2).expect("stream id"),
            peer.try_wire().expect("SETTINGS wire"),
            Boundary::More,
        )
        .expect("fixed peer settings stream must be valid");
    assert!(
        matches!(Events::new(&mut conn).poll_token(), Ok(Some(Event::Settings(settings))) if settings == peer)
    );

    assert!(matches!(
        conn.control().set_capacity(64),
        Err(Error::Egress(o3::depot::queue::group::Error::Entries(
            capacity::Shortage {
                requested: NonZero::<usize>::MIN,
                available: 0,
            }
        )))
    ));
    for id in [3, 7] {
        let prefix = conn
            .events()
            .poll_write()
            .expect("critical prefix must poll")
            .expect("critical prefix queued");
        assert_eq!(prefix.stream_id().get(), id);
    }
    Send::new(&mut conn)
        .headers(
            Id::new(0).expect("stream id"),
            [literal!(entry(b"x-capacity", b"not-yet-dynamic"))],
            Boundary::More,
        )
        .expect("static fallback header must fit one write");
    let headers = conn
        .events()
        .poll_write()
        .expect("static fallback must poll")
        .expect("static fallback write must be queued");
    assert!(headers.stream_id() == Id::new(0).expect("stream id"));
    let head = Header::parse(&headers.prefix)
        .expect("queued headers must contain a complete frame header");
    assert!(head.kind == TYPE_HEADERS);
    let (count, _) = integer::PrefixedInt::<8>::decode(&headers.prefix[head.header_len..])
        .expect("queued headers must contain a required insert count");
    assert_eq!(count.get(), 0);

    conn.control()
        .set_capacity(64)
        .expect("capacity change must commit after credit returns");
    let write = conn
        .events()
        .poll_write()
        .expect("capacity instruction must poll")
        .expect("capacity instruction must be queued");
    assert!(write.stream_id() == Id::new(3).expect("stream id"));
    let bytes = &*write.prefix;
    assert_eq!(bytes.first().map(|first| first & 0xe0), Some(0x20));
    let (size, used) = integer::PrefixedInt::<5>::decode(bytes)
        .expect("capacity instruction must contain a complete integer");
    assert_eq!((size.get(), used.get()), (64, bytes.len()));

    assert!(
        conn.events()
            .poll_write()
            .expect("drained writes must poll")
            .is_none()
    );
}
