use dope_quic::{errors::Code, wire::stream::Id};
use sark_core::manifold::stream::Boundary;
use sark_h3::conn::{
    Connection, Error,
    event::{Role, Token},
    io::{Receive, Send, Step},
    options,
};

#[test]
fn critical_stop() {
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    let control = Id::new(3).expect("control");
    let encoder = Id::new(7).expect("encoder");
    let decoder = Id::new(11).expect("decoder");
    conn.control().open(control).expect("control");
    conn.control().open_encoder(encoder).expect("encoder");
    conn.control().open_decoder(decoder).expect("decoder");
    let queued = conn.wire_len();
    for id in [control, encoder, decoder] {
        assert!(
            matches!(Receive::new(&mut conn).stop(id, None, Code::from_u16(0)),
            Err((Error::ClosedCriticalStream(stream), charge)) if stream == id.get() && charge.is_empty())
        );
    }
    assert_eq!(conn.wire_len(), queued);
}

#[test]
fn peer_critical_stop() {
    for (role, first) in [(Role::Client, 3), (Role::Server, 2)] {
        let mut config = options::Config::default();
        config.streams.capacity = 1;
        let mut conn = Connection::with_config(role, config).expect("connection");
        Receive::new(&mut conn)
            .borrowed(Id::ZERO, &[], Boundary::More)
            .expect("only ordinary slot occupied");
        for (offset, kind) in [(0, 0), (4, 2), (8, 3)] {
            let id = Id::new(first + offset).expect("peer stream");
            Receive::new(&mut conn)
                .borrowed(id, &[kind], Boundary::More)
                .expect("critical stream type");
            for reset in [false, true] {
                let receive = Receive::new(&mut conn);
                let result = if reset {
                    receive.reset(id, None, Code::from_u16(0))
                } else {
                    receive.stop(id, None, Code::from_u16(0))
                };
                assert!(matches!(result,
                    Err((Error::ClosedCriticalStream(stream), charge))
                    if stream == id.get() && charge.is_empty()));
            }
        }
        assert!(matches!(conn.events().poll_token(), Ok(Step::Idle)));
    }
}

#[test]
fn critical_headroom() {
    for kind in [1, 0x21] {
        let mut config = options::Config::default();
        config.streams.capacity = 1;
        let mut conn = Connection::with_config(Role::Client, config).expect("connection");
        Receive::new(&mut conn)
            .borrowed(Id::ZERO, &[], Boundary::More)
            .expect("only ordinary slot occupied");
        assert_eq!(
            Receive::new(&mut conn).borrowed(
                Id::new(3).expect("peer stream"),
                &[kind],
                Boundary::More,
            ),
            Err(Error::StreamAdmission {
                stream_id: 3,
                active: 2,
                capacity: 1,
            })
        );
    }
}

#[test]
fn token_rollback() {
    let mut config = options::Config::default();
    config.streams.capacity = 1;
    let mut conn = Connection::with_config(Role::Server, config).expect("connection");
    Receive::new(&mut conn)
        .borrowed(Id::ZERO, &[], Boundary::More)
        .expect("receive stream");
    Send::new(&mut conn)
        .data(Id::ZERO, b"queued", Boundary::More)
        .expect("queued output");
    let queued = conn.wire_len();
    for reset in [false, true] {
        let receive = Receive::new(&mut conn);
        let wrong = Some(Token::new(4).expect("wrong token"));
        let result = if reset {
            receive.reset(Id::ZERO, wrong, Code::from_u16(0))
        } else {
            receive.stop(Id::ZERO, wrong, Code::from_u16(0))
        };
        assert!(matches!(result,
            Err((Error::TokenMismatch { expected: 0, actual: 4 }, charge))
            if charge.is_empty()));
    }
    assert_eq!(conn.wire_len(), queued);
    assert!(matches!(conn.events().poll_token(), Ok(Step::Idle)));
    assert_eq!(
        Receive::new(&mut conn).borrowed(Id::new(4).expect("next stream"), &[], Boundary::More),
        Err(Error::StreamCapacity {
            stream_id: 4,
            active: 1,
            capacity: 1,
        })
    );
}
