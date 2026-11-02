use common::{
    hpack::Fields,
    protocol::{Session, rules::Settings, session::Frame},
};
use sark_h2::{ServerRole, conn::machine::Connection};
pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::frame::Block;
use sark_http::field::metadata::map::record::Entry;

#[test]
fn uses_local_policy() {
    use sark_h2::conn::event::Event;
    use sark_http::field::metadata::block::Form;

    let standard = Fields::new(&[
        Entry::new(b":method", b"CONNECT"),
        Entry::new(b":authority", b"origin.example:443"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let mut conn = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(1, &standard)
                .try_headers(Boundary::More, Block::End)
                .expect("protocol wire fixture"),
        )
        .expect("standard CONNECT HEADERS frame fits ingress");
    let control = match conn
        .ingress()
        .poll_event()
        .expect("standard CONNECT must be admitted")
    {
        Event::Headers { head, .. } => Some(head.into_parts().2),
        _ => None,
    }
    .expect("standard CONNECT must be admitted");
    assert!(
        (control
            .expect("standard CONNECT carries an admission plan")
            .form())
            == (Form::Connect)
    );

    let extended = Fields::new(&[
        Entry::new(b":method", b"CONNECT"),
        Entry::new(b":protocol", b"websocket"),
        Entry::new(b":scheme", b"https"),
        Entry::new(b":authority", b"origin.example"),
        Entry::new(b":path", b"/chat"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let mut disabled = Connection::<ServerRole>::try_new().expect("server fixture");
    Session::new(&mut disabled)
        .try_prime()
        .expect("protocol fixture");
    disabled
        .ingress()
        .ingest(
            &Frame::new(1, &extended)
                .try_headers(Boundary::More, Block::End)
                .expect("protocol wire fixture"),
        )
        .expect("disabled extended CONNECT frame fits ingress");
    assert!(disabled.ingress().poll_event().is_none());

    let mut enabled = Settings::extended()
        .try_connect::<ServerRole>()
        .expect("protocol fixture");
    Session::new(&mut enabled)
        .try_prime()
        .expect("protocol fixture");
    enabled
        .ingress()
        .ingest(
            &Frame::new(1, &extended)
                .try_headers(Boundary::More, Block::End)
                .expect("protocol wire fixture"),
        )
        .expect("enabled extended CONNECT frame fits ingress");
    let control = match enabled
        .ingress()
        .poll_event()
        .expect("enabled Extended CONNECT must be admitted")
    {
        Event::Headers { head, .. } => Some(head.into_parts().2),
        _ => None,
    }
    .expect("enabled Extended CONNECT must be admitted");
    assert!(
        (control
            .expect("extended CONNECT carries an admission plan")
            .form())
            == (Form::ExtendedConnect)
    );
}

#[test]
fn sends_extended_connect_only_after_peer_opt_in() {
    use common::{
        headers::entry,
        protocol::{conn::Controls, preface::Preface},
    };
    use sark_h2::{
        ClientRole,
        conn::Error,
        frame::{Ack, numbers::SettingId},
        hpack::encode,
    };
    use sark_http::field::literal;

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
    for enabled in [None, Some(0), Some(1)] {
        let mut conn = Connection::<ClientRole>::try_new().expect("client");
        if let Some(enabled) = enabled {
            let settings = Controls::new(Ack::Required)
                .try_settings(&[(SettingId::EnableConnectProtocol as u16, enabled)])
                .expect("peer SETTINGS");
            Preface::new(&settings)
                .try_client(&mut conn)
                .expect("peer preface");
        } else {
            conn.transfer().drain(usize::MAX);
        }
        let result = conn.streams().headers().request(extended, Boundary::More);
        if enabled == Some(1) {
            assert!(result.is_ok());
        } else {
            assert!(matches!(
                result,
                Err(Error::HeaderEncode(encode::fields::Error::ConnectDisabled))
            ));
            assert!(conn.output().first().is_empty());
            let id = conn
                .streams()
                .headers()
                .request(standard, Boundary::More)
                .expect("ordinary CONNECT remains valid after rejected extended CONNECT");
            assert_eq!(
                id,
                sark_h2::stream::Id::new(1).expect("first client stream")
            );
        }
    }
}
