use dope_quic::wire::stream::Id;
use dope_test::alloc::Tracker;
use o3::{alloc, buffer::storage::inline};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    VarInt,
    conn::{Connection, Error, event::Role, io::Send},
    frame::{EncodeError, Header, Payload, Settings, TYPE_DATA},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn keeps_bounded_prefix() {
    let header = Header {
        kind: TYPE_DATA,
        length: VarInt::from_u8(1),
        header_len: 2,
    };
    let mut head = inline::Bytes::<2>::from([0xa5]);
    assert!(header.encode(&mut head).is_err());
    assert_eq!(head.as_ref(), [0xa5]);

    let mut settings = inline::Bytes::<4>::from([0xa5]);
    assert!(Settings::default().encode(&mut settings).is_err());
    assert_eq!(settings.as_ref(), [0xa5]);
}

#[test]
fn keeps_payload_prefix() {
    let mut wire = Vec::from([0xa5]);
    let result = Tracker::fail_after(0, || Payload::encode(TYPE_DATA, b"x", &mut wire));
    assert!(matches!(
        result,
        Err(EncodeError::Reserve {
            required: 3,
            error: alloc::Error::Exhausted,
        })
    ));
    assert_eq!(wire, [0xa5]);
    Payload::encode(TYPE_DATA, b"x", &mut wire).expect("recovered allocation");
    assert_eq!(wire, [0xa5, 0, 1, b'x']);
}

#[test]
fn reserves_data_once() {
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    let stream = Id::new(0).expect("stream");
    let payload = [0xa5; 128];
    let error = Tracker::fail_after(0, || {
        Send::new(&mut conn).data(stream, &payload, Boundary::End)
    });
    assert!(matches!(error, Err(Error::Prefix { required: 131, .. })));
    assert!(conn.events().poll_write().expect("empty queue").is_none());

    let (result, allocs) =
        Tracker::count(|| Send::new(&mut conn).data(stream, &payload, Boundary::End));
    result.expect("recovered allocation");
    assert_eq!(allocs, 1);
    let output = conn.events().poll_write().expect("write").expect("DATA");
    assert_eq!(&output.prefix[..3], &[0, 0x40, 0x80]);
    assert_eq!(&output.prefix[3..], &payload);
    assert_eq!(output.boundary(), Boundary::End);
}
