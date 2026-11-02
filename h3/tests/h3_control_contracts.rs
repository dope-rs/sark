use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection, Error,
        event::{Event, Role},
        io::{Receive, Send, Step},
        options,
    },
    frame::{ParseError, Settings},
};

#[test]
fn settings_identifiers() {
    let control = Id::new(2).expect("peer control");
    for setting in [0, 2, 3, 4, 5] {
        let mut conn = Connection::with_role(Role::Server).expect("connection");
        assert_eq!(
            Receive::new(&mut conn).borrowed(control, &[0, 4, 2, setting, 0], Boundary::More),
            Err(Error::Parse(ParseError::BadSettings))
        );
    }
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    Receive::new(&mut conn)
        .borrowed(control, &[0, 4, 2, 0x21, 0], Boundary::More)
        .expect("unknown setting ignored");
    assert!(matches!(conn.events().poll_token(),
        Ok(Step::Item(Event::Settings(settings))) if settings == Settings::default()));
}

#[test]
fn received_goaway_order() {
    for (role, control, first, next, invalid) in
        [(Role::Client, 3, 8, 4, 8), (Role::Server, 2, 9, 5, 6)]
    {
        let mut conn = Connection::with_role(role).expect("connection");
        let control = Id::new(control).expect("peer control");
        Receive::new(&mut conn)
            .borrowed(control, &[0, 4, 0], Boundary::More)
            .expect("settings");
        assert!(matches!(
            conn.events().poll_token(),
            Ok(Step::Item(Event::Settings(_)))
        ));
        for id in [first, next, next] {
            Receive::new(&mut conn)
                .borrowed(control, &[7, 1, id], Boundary::More)
                .expect("nonincreasing GOAWAY");
            assert!(matches!(conn.events().poll_token(),
                Ok(Step::Item(Event::GoAway { id: actual })) if actual == u64::from(id)));
        }
        assert_eq!(
            Receive::new(&mut conn).borrowed(control, &[7, 1, invalid], Boundary::More),
            Err(Error::GoAwayOrder {
                previous: u64::from(next),
                actual: u64::from(invalid),
            })
        );
    }
    let mut conn = Connection::with_role(Role::Client).expect("connection");
    let control = Id::new(3).expect("peer control");
    Receive::new(&mut conn)
        .borrowed(control, &[0, 4, 0], Boundary::More)
        .expect("settings");
    assert_eq!(
        Receive::new(&mut conn).borrowed(control, &[7, 1, 3], Boundary::More),
        Err(Error::GoAwayId(3))
    );
}

#[test]
fn sent_goaway_order() {
    for (role, control, first, next, invalid) in
        [(Role::Client, 2, 9, 5, 6), (Role::Server, 3, 8, 4, 8)]
    {
        let mut conn = Connection::with_role(role).expect("connection");
        conn.control()
            .open(Id::new(control).expect("local control"))
            .expect("control");
        if role == Role::Server {
            assert_eq!(Send::new(&mut conn).goaway(3), Err(Error::GoAwayId(3)));
        }
        for id in [first, next, next] {
            Send::new(&mut conn)
                .goaway(id)
                .expect("nonincreasing GOAWAY");
        }
        let queued = conn.wire_len();
        assert_eq!(
            Send::new(&mut conn).goaway(invalid),
            Err(Error::GoAwayOrder {
                previous: next,
                actual: invalid,
            })
        );
        assert_eq!(conn.wire_len(), queued);
    }
}

#[test]
fn goaway_rollback() {
    let mut config = options::Config::default();
    config.egress.write_capacity = 2;
    let mut conn = Connection::with_config(Role::Server, config).expect("connection");
    conn.control()
        .open(Id::new(3).expect("local control"))
        .expect("control");
    conn.control()
        .open_encoder(Id::new(7).expect("local encoder"))
        .expect("critical queue full");
    for (rejected, accepted) in [(4, 8), (0, 4)] {
        assert!(matches!(
            Send::new(&mut conn).goaway(rejected),
            Err(Error::Egress(_))
        ));
        conn.events()
            .poll_write()
            .expect("queued write")
            .expect("queue was full");
        Send::new(&mut conn)
            .goaway(accepted)
            .expect("rejected GOAWAY did not lower the committed ID");
    }
}
