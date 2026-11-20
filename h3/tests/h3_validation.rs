use sark_http::field::literal;

use crate::common::{
    conn::{Connection as _, build::Fields as _, next_event},
    field::entry,
    message::validate::{self, UPPERCASE},
};

pub mod common;

use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::conn::{Connection, Error, event::Event, io::Receive};
use sark_http::field::metadata::block::Connect;

#[test]
fn isolates_malformed_requests() {
    let method = literal!(entry(b":method", b"GET"));
    let scheme = literal!(entry(b":scheme", b"https"));
    let authority = literal!(entry(b":authority", b"example.test"));
    let path = literal!(entry(b":path", b"/"));
    let bad_path = entry(b":path", validate::INVALID_PATH).expect("field bytes");
    let regular = literal!(entry(b"x-regular", b"1"));
    let mut conn = Connection::try_validate(Connect::Disabled).expect("connection");
    for (stream, fields) in [
        (0, &[method, scheme, authority, bad_path][..]),
        (4, &[method, path][..]),
        (8, &[method, scheme, path, regular, authority][..]),
    ] {
        let id = Id::new(stream).expect("request stream");
        let wire = fields.try_headers().expect("request fields");
        Receive::new(&mut conn)
            .owned(id, wire, Boundary::More)
            .expect("request framing");
        validate::expect_malformed(&mut conn, id);
    }
    let uppercase = Id::new(12).expect("request stream");
    Receive::new(&mut conn)
        .borrowed(uppercase, UPPERCASE, Boundary::More)
        .expect("request framing");
    validate::expect_malformed(&mut conn, uppercase);

    let next = Id::new(16).expect("request stream");
    let valid = [
        method,
        scheme,
        authority,
        path,
        literal!(entry(b"te", b"trailers")),
    ];
    Receive::new(&mut conn)
        .owned(
            next,
            valid.try_headers().expect("request fields"),
            Boundary::More,
        )
        .expect("next request");
    assert!(matches!(
        next_event(&mut conn).expect("poll next request"),
        Some(Event::Headers { message, .. }) if message.stream_id() == next.into()
    ));
}

#[test]
fn gates_connect_by_setting() {
    use sark_http::field::metadata::block::Form;

    let standard = [
        literal!(entry(b":method", b"CONNECT")),
        literal!(entry(b":authority", b"origin.example:443")),
    ]
    .try_headers()
    .expect("validation fields");
    let mut conn = Connection::try_validate(Connect::Disabled).expect("validation connection");
    Receive::new(&mut conn)
        .owned(Id::ZERO, standard, Boundary::More)
        .expect("standard CONNECT request must be admitted");
    let control = next_event(&mut conn)
        .expect("standard CONNECT event polling must succeed")
        .and_then(|event| match event {
            Event::Headers { head, .. } => head.into_parts().2,
            _ => None,
        })
        .expect("standard CONNECT must produce headers with control metadata");
    assert!(control.form() == Form::Connect);

    let extended = [
        literal!(entry(b":method", b"CONNECT")),
        literal!(entry(b":protocol", b"websocket")),
        literal!(entry(b":scheme", b"https")),
        literal!(entry(b":authority", b"origin.example")),
        literal!(entry(b":path", b"/chat")),
    ]
    .try_headers()
    .expect("validation fields");
    let mut disabled = Connection::try_validate(Connect::Disabled).expect("validation connection");
    Receive::new(&mut disabled)
        .owned(Id::ZERO, extended.clone(), Boundary::More)
        .expect("Extended CONNECT framing");
    validate::expect_malformed(&mut disabled, Id::ZERO);

    let mut enabled = Connection::try_validate(Connect::Enabled).expect("validation connection");
    Receive::new(&mut enabled)
        .owned(Id::ZERO, extended, Boundary::More)
        .expect("enabled Extended CONNECT request must be admitted");
    let control = next_event(&mut enabled)
        .expect("Extended CONNECT event polling must succeed")
        .and_then(|event| match event {
            Event::Headers { head, .. } => head.into_parts().2,
            _ => None,
        })
        .expect("Extended CONNECT must produce headers with control metadata");
    assert!(control.form() == Form::ExtendedConnect);
}

#[test]
fn sends_extended_connect_only_after_peer_opt_in() {
    use common::conn::build::Settings as _;
    use dope_quic::wire::varint::VarInt;
    use sark_h3::{
        conn::{
            event::Role,
            io::Send,
            options::{Config, Protocol},
        },
        frame::Settings,
        qpack::encoder,
    };

    let standard = [
        literal!(entry(b":method", b"CONNECT")),
        literal!(entry(b":authority", b"origin.example:443")),
    ];
    let extended = [
        standard[0],
        literal!(entry(b":protocol", b"websocket")),
        literal!(entry(b":scheme", b"https")),
        literal!(entry(b":authority", b"origin.example")),
        literal!(entry(b":path", b"/chat")),
    ];
    for enabled in [None, Some(Connect::Disabled), Some(Connect::Enabled)] {
        let mut conn = Connection::with_config(
            Role::Client,
            Config {
                protocol: Protocol {
                    extended_connect: Connect::Enabled,
                    ..Protocol::default()
                },
                ..Config::default()
            },
        )
        .expect("client");
        if let Some(enabled) = enabled {
            let peer = Settings::new(VarInt::ZERO, None, VarInt::ZERO, enabled);
            Receive::new(&mut conn)
                .owned(
                    Id::new(3).expect("control stream"),
                    peer.try_wire().expect("SETTINGS"),
                    Boundary::More,
                )
                .expect("peer SETTINGS");
        }
        let result = Send::new(&mut conn).headers(Id::ZERO, extended, Boundary::More);
        if enabled == Some(Connect::Enabled) {
            assert!(result.is_ok());
        } else {
            assert_eq!(result, Err(Error::Encode(encoder::Error::ConnectDisabled)));
            assert_eq!((conn.wire_len(), conn.resident_len()), (0, 0));
            Send::new(&mut conn)
                .headers(Id::ZERO, standard, Boundary::More)
                .expect("ordinary CONNECT remains valid after rejected extended CONNECT");
        }
    }
}
