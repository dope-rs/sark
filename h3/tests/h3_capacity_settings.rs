use core::mem;

use crate::common::message::capacity::{Capacity as _, ZstPush};
pub mod common;
use sark_h3::conn::{
    Connection,
    event::Role,
    options::{Config, Decode, Ingress, Qpack, Streams, plan},
};
use sark_http::field::metadata::section::rules::RetainRaw;

#[test]
fn derives_settings() {
    let config = Config {
        streams: Streams { capacity: 3 },
        ingress: Ingress {
            field_section_bytes: 12_345,
            ..Ingress::default()
        },
        qpack: Qpack {
            decoder: Decode {
                table_bytes: 4096,
                blocked_streams: 2,
            },
            ..Qpack::default()
        },
        ..Config::default()
    };
    let plan =
        (plan::Config::new(config)).expect("capacity fixture satisfies its declared invariants");
    let settings = plan.settings();
    assert_eq!(settings.table().get(), 4096);
    assert_eq!(settings.blocked().get(), 2);
    assert_eq!(
        settings.field_limit().map(|value| value.get()),
        Some(12_345)
    );

    let defaults = plan::Config::new(Default::default()).expect("default H3 options");
    assert_eq!(
        defaults.settings().field_limit().map(|value| value.get()),
        Some(64 * 1024)
    );
}

#[test]
fn reuses_config() {
    let config = (plan::Config::new(Config::single()))
        .expect("capacity fixture satisfies its declared invariants");
    let _: Connection<'static> = (config.clone().build(Role::Client))
        .expect("capacity fixture satisfies its declared invariants");
    let _: Connection<'static> =
        (config.build(Role::Server)).expect("capacity fixture satisfies its declared invariants");
}

#[test]
fn reports_qpack_footprint() {
    let layout = plan::Config::new(Default::default())
        .expect("default H3 options")
        .footprint();
    assert_eq!(layout.resident(), 8_400);
    assert_eq!(layout.allocations(), 2);
}

#[test]
fn zst_authority_layout() {
    assert_eq!(
        mem::size_of::<Connection<'static>>(),
        mem::size_of::<Connection<'static, RetainRaw, ZstPush>>(),
    );
}

#[test]
fn peer_field_limit_counts_uncompressed_and_generated_fields() {
    use common::{conn::build::Settings as _, field::entry, qpack::NeverValue};
    use dope_quic::wire::{stream::Id, varint::VarInt};
    use sark_core::manifold::stream::Boundary;
    use sark_h3::{
        conn::{
            Error,
            io::{Receive, Send},
        },
        frame::Settings,
        qpack::encoder,
    };
    use sark_http::field::{Lower, Value, literal, metadata::block::Connect};

    for limit in [41, 42] {
        for generated in [false, true] {
            let mut conn = Connection::with_role(Role::Server).expect("server");
            let peer = Settings::new(
                VarInt::ZERO,
                Some(VarInt::from_u8(limit)),
                VarInt::ZERO,
                Connect::Disabled,
            );
            Receive::new(&mut conn)
                .owned(
                    Id::new(2).expect("control stream"),
                    peer.try_wire().expect("SETTINGS"),
                    Boundary::More,
                )
                .expect("peer SETTINGS");
            let result = if generated {
                Send::new(&mut conn).headers(
                    Id::ZERO,
                    NeverValue(
                        literal!(Lower::try_bytes(b"x")),
                        literal!(Value::try_bytes(b"123456789")),
                    ),
                    Boundary::More,
                )
            } else {
                Send::new(&mut conn).headers(
                    Id::ZERO,
                    [literal!(entry(b":status", b"200"))],
                    Boundary::More,
                )
            };
            if limit == 42 {
                assert!(result.is_ok());
            } else {
                let (name, value) = if generated { (1, 9) } else { (7, 3) };
                assert_eq!(
                    result,
                    Err(Error::Encode(encoder::Error::FieldSection {
                        remaining: 41,
                        name,
                        value
                    }))
                );
                assert_eq!((conn.wire_len(), conn.resident_len()), (0, 0));
                assert!(
                    conn.events()
                        .poll_write()
                        .expect("unchanged output")
                        .is_none()
                );
            }
        }
    }
}

#[test]
fn peer_field_rejection_rolls_back_push_and_dynamic_table() {
    use common::{conn::build::Settings as _, field::entry, stream::Stream as _};
    use dope_quic::wire::{stream::Id, varint::VarInt};
    use sark_core::manifold::stream::Boundary;
    use sark_h3::{
        conn::{
            Error,
            io::{Receive, Send},
        },
        frame::Settings,
        qpack::encoder,
    };
    use sark_http::field::{literal, metadata::block::Connect};

    let mut conn = Connection::with_role(Role::Server).expect("server");
    let peer = Settings::new(
        VarInt::new(256).expect("table capacity"),
        Some(VarInt::from_u8(248)),
        VarInt::from_u8(1),
        Connect::Disabled,
    );
    Receive::new(&mut conn)
        .owned(
            Id::new(2).expect("control stream"),
            peer.try_wire().expect("SETTINGS"),
            Boundary::More,
        )
        .expect("peer SETTINGS");
    conn.allow_push(Role::Server, 0).expect("push admission");
    conn.control()
        .open_encoder(Id::new(3).expect("encoder stream"))
        .expect("encoder");
    conn.control().set_capacity(256).expect("table capacity");
    while conn
        .events()
        .poll_write()
        .expect("initial output")
        .is_some()
    {}
    let fields = [
        literal!(entry(b":method", b"GET")),
        literal!(entry(b":scheme", b"https")),
        literal!(entry(b":authority", b"example.test")),
        literal!(entry(b":path", b"/")),
        literal!(entry(b"x-a", b"a")),
        literal!(entry(b"x-b", b"b")),
    ];
    assert_eq!(
        Send::new(&mut conn).promise(Id::ZERO, 0, fields),
        Err(Error::Encode(encoder::Error::FieldSection {
            remaining: 34,
            name: 3,
            value: 1
        }))
    );
    assert_eq!((conn.wire_len(), conn.resident_len()), (0, 0));
    conn.control()
        .set_capacity(0)
        .expect("rejected section retained no dynamic insertion");
    Send::new(&mut conn)
        .promise(Id::ZERO, 0, &fields[..4])
        .expect("rejected section retained no push identity");
}
