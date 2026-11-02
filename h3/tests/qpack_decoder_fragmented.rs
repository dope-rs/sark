use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{Connection, Error, event::Role, io::Receive},
    qpack::stream,
};

#[test]
fn fragmented_cancel() {
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    let id = Id::new(2).expect("decoder stream");
    Receive::new(&mut conn)
        .owned(id, vec![3, 0x7f], Boundary::More)
        .expect("incomplete cancellation");
    Receive::new(&mut conn)
        .owned(id, vec![0], Boundary::More)
        .expect("segmented cancellation");
    let error = Receive::new(&mut conn)
        .owned(id, vec![0], Boundary::More)
        .expect_err("next instruction");
    assert_eq!(
        error,
        Error::QpackDecoderStream(stream::Error::ZeroIncrement)
    );
}
