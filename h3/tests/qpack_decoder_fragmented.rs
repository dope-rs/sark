use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection, Error,
        event::{Event, Role},
        io::{Receive, Step},
        options::{Config, Ingress},
    },
    qpack::stream,
};

#[test]
fn fragmented_cancel() {
    let mut conn = Connection::with_config(
        Role::Server,
        Config {
            ingress: Ingress {
                event_capacity: 1,
                ..Ingress::default()
            },
            ..Config::default()
        },
    )
    .expect("connection");
    let id = Id::new(2).expect("decoder stream");
    Receive::new(&mut conn)
        .owned(id, vec![3, 0x7f], Boundary::More)
        .expect("incomplete cancellation");
    Receive::new(&mut conn)
        .owned(id, vec![1, 0x40], Boundary::More)
        .expect("segmented cancellation fills the event queue before the next instruction");
    for expected in [64, 0] {
        assert!(matches!(
            conn.events().poll_token().expect("cancellation survives pressure"),
            Step::Item(Event::QpackCancelled { stream_id }) if stream_id.get() == expected
        ));
    }
    let error = Receive::new(&mut conn)
        .owned(id, vec![0], Boundary::More)
        .expect_err("next instruction");
    assert_eq!(
        error,
        Error::QpackDecoderStream(stream::Error::ZeroIncrement)
    );
}

#[test]
fn cancellation_rejects_non_stream_id() {
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    let mut wire = vec![3];
    stream::Instruction::StreamCancellation { stream_id: 1 << 62 }.encode(&mut wire);
    assert_eq!(
        Receive::new(&mut conn).owned(Id::new(2).expect("decoder"), wire, Boundary::More),
        Err(Error::QpackDecoderStream(stream::Error::StreamId(
            dope_quic::wire::varint::Error::TooLarge { value: 1 << 62 }
        )))
    );
}
