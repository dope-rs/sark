use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection, Error,
        event::Role,
        io::{Receive, Step},
    },
    frame,
    qpack::decoder,
};

#[test]
fn rejects_split_overflow() {
    let mut encoded = [0x80; 11];
    encoded[0] = 0xff;
    encoded[10] = 0x02;
    let mut wire = Vec::new();
    frame::Payload::encode(frame::TYPE_HEADERS, &encoded, &mut wire).expect("headers frame");
    let prefix = wire.len() - encoded.len();
    for split in 0..=encoded.len() {
        let mut conn = Connection::with_role(Role::Server).expect("server connection");
        let cut = prefix + split;
        let first = Receive::new(&mut conn).owned(Id::ZERO, wire[..cut].to_vec(), Boundary::More);
        let received = if cut != wire.len() {
            first.expect("incomplete transport segment");
            Receive::new(&mut conn).owned(Id::ZERO, wire[cut..].to_vec(), Boundary::More)
        } else {
            first
        };
        let error = match received {
            Err(error) => Some(error),
            Ok(()) => loop {
                match conn.events().poll_token() {
                    Err(error) => break Some(error),
                    Ok(Step::More) => {}
                    Ok(Step::Item(_) | Step::Idle) => break None,
                }
            },
        }
        .expect("split overflow must be rejected");
        assert!(
            matches!(
                error,
                Error::Qpack(decoder::Error::Integer {
                    prefix: 8,
                    available: 11,
                    cause: sark_http::field::metadata::integer::Overflow {
                        value: 255,
                        shift: 63,
                        byte: 2
                    },
                })
            ),
            "split at {split}: {error:?}"
        );
    }
}
