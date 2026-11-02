use sark_h3::conn::Connection;
use sark_http::field::literal;

use crate::common::{
    conn::{Connection as _, Events, build::Fields as _},
    field::entry,
    message::validate::{self, UPPERCASE},
};

pub mod common;

use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::conn::{Error, event::Event, io::Receive};
use sark_http::field::metadata::{block::Connect, section::rules};

#[test]
fn rejects_invalid_path() {
    assert!(matches!(
        validate::try_ingest_request(&[
            literal!(entry(b":method", b"GET")),
            literal!(entry(b":scheme", b"https")),
            literal!(entry(b":authority", b"example.test")),
            entry(b":path", validate::INVALID_PATH).expect("valid field bytes"),
        ]),
        Err(validate::Error::Connection(Error::Malformed {
            stream_id: 0,
            kind: rules::Kind::Request,
        }))
    ));
}

#[test]
fn validates_request_fields() {
    assert!(
        matches!(
            validate::try_ingest_request(&[
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":path", b"/")),
            ]),
            Err(validate::Error::Connection(Error::Malformed {
                stream_id: 0,
                kind: rules::Kind::Request,
            }))
        ),
        "the request pseudo-header set must be complete",
    );
    assert!(
        matches!(
            validate::try_ingest_request(&[
                literal!(entry(b":method", b"GET")),
                literal!(entry(b":scheme", b"https")),
                literal!(entry(b":path", b"/")),
                literal!(entry(b"x-regular", b"1")),
                literal!(entry(b":authority", b"example.test")),
            ]),
            Err(validate::Error::Connection(Error::Malformed {
                stream_id: 0,
                kind: rules::Kind::Request,
            }))
        ),
        "pseudo-headers cannot follow regular fields",
    );
    let mut conn = Connection::try_validate(Connect::Disabled).expect("validation connection");
    assert!(
        Receive::new(&mut conn).borrowed(Id::ZERO, UPPERCASE, Boundary::More)
            == Err(Error::Malformed {
                stream_id: 0,
                kind: rules::Kind::Request,
            }),
        "uppercase field names are forbidden",
    );
    assert!(
        validate::try_ingest_request(&[
            literal!(entry(b":method", b"GET")),
            literal!(entry(b":scheme", b"https")),
            literal!(entry(b":authority", b"example.test")),
            literal!(entry(b":path", b"/")),
            literal!(entry(b"te", b"trailers")),
        ])
        .is_ok(),
    );
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
    let control = Events::new(&mut conn)
        .poll_token()
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
    assert!(
        Receive::new(&mut disabled).owned(Id::ZERO, extended.clone(), Boundary::More)
            == Err(Error::Malformed {
                stream_id: 0,
                kind: rules::Kind::Request,
            }),
    );

    let mut enabled = Connection::try_validate(Connect::Enabled).expect("validation connection");
    Receive::new(&mut enabled)
        .owned(Id::ZERO, extended, Boundary::More)
        .expect("enabled Extended CONNECT request must be admitted");
    let control = Events::new(&mut enabled)
        .poll_token()
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
