use dope_quic::wire::stream::Id;

use crate::common::{atomic::Setup, stream::Stream as _};

pub mod common;

use std::iter;

use dope_quic::{errors::Code, wire::varint::VarInt};
use dope_test::alloc::Tracker;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection,
        event::Role,
        io::{Receive, Send},
        output::WritePrefix,
    },
    frame::TYPE_DATA,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn stop_allocates_nothing() {
    let target = Id::new(0).expect("stream id");
    let mut conn = Setup::new(64, usize::MAX, usize::MAX)
        .try_build()
        .expect("egress configuration");
    let writes: Vec<_> = (0u8..48)
        .map(|index| {
            let stream = Id::new(u64::from(index % 12) * 4).expect("stream id");
            (stream, vec![index])
        })
        .collect();

    let (_, (allocs, _)) = Tracker::usage(|| {
        for (stream, payload) in writes {
            Send::new(&mut conn)
                .owned(stream, payload, Boundary::More)
                .expect("indexed fixture write must fit the queue");
        }
        assert_eq!(
            conn.retained(target).expect("retained charge").owners,
            4,
            "four DATA writes retain only their four payload owners",
        );
        let released = Receive::new(&mut conn)
            .stop(target, None, Code::from_u16(0x10))
            .map_err(|(error, charge)| {
                (
                    error,
                    charge.logical_bytes,
                    charge.resident_bytes,
                    charge.owners,
                )
            })
            .expect("peer stop must release indexed target writes");
        assert_eq!(released.owners, 4);
    });
    assert_eq!(allocs, 0);
    assert!(conn.retained(target).expect("retained charge").is_empty());
    assert_eq!(
        iter::from_fn(|| conn.events().poll_write().expect("write queue")).count(),
        44,
        "only the four target writes are removed",
    );
}

#[test]
fn small_writes_are_inline() {
    let mut server = Connection::with_role(Role::Server)
        .expect("default fixture server configuration must be valid");
    let mut client = Connection::with_role(Role::Client)
        .expect("default fixture client configuration must be valid");
    server.allow_push(Role::Server, 17).expect("push grant");
    server.promise(17).expect("push promise");
    while server
        .events()
        .poll_write()
        .expect("promise write")
        .is_some()
    {}

    let (_, (allocs, _)) = Tracker::usage(|| {
        server
            .control()
            .open(Id::new(3).expect("stream id"))
            .expect("server control stream must open once");
        server
            .control()
            .open_encoder(Id::new(7).expect("stream id"))
            .expect("server encoder stream must open once");
        server
            .control()
            .open_decoder(Id::new(11).expect("stream id"))
            .expect("server decoder stream must open once");
        server
            .control()
            .set_capacity(0)
            .expect("zero QPACK capacity must be valid");
        Send::new(&mut server)
            .cancel(17)
            .expect("fixture cancellation id must be valid");
        Send::new(&mut server)
            .goaway(4)
            .expect("fixture GOAWAY id must be valid");
        Send::new(&mut server)
            .data(Id::new(0).expect("stream id"), b"small", Boundary::More)
            .expect("small fixture DATA must fit inline storage");

        client
            .control()
            .open(Id::new(2).expect("stream id"))
            .expect("client control stream must open once");
        Send::new(&mut client)
            .max_push(17)
            .expect("fixture push limit must be valid");
    });

    assert_eq!(allocs, 0);
    for write in iter::from_fn(|| server.events().poll_write().expect("write queue")).chain(
        iter::from_fn(|| client.events().poll_write().expect("write queue")),
    ) {
        assert!(matches!(write.prefix, WritePrefix::Inline(_)));
        assert!(write.payload.is_none());
    }
}

#[test]
fn large_allocates_once() {
    const BODY: [u8; 256] = [0x5a; 256];

    let mut conn = Connection::with_role(Role::Server)
        .expect("default fixture server configuration must be valid");
    let expected = usize::from(TYPE_DATA.encoded_len())
        + usize::from(
            VarInt::from_usize(BODY.len())
                .expect("fixture body length must fit the QUIC variable integer")
                .encoded_len(),
        )
        + BODY.len();
    let (_, alloc) = Tracker::usage(|| {
        Send::new(&mut conn)
            .data(Id::new(0).expect("stream id"), &BODY, Boundary::More)
            .expect("large fixture DATA must fit the configured queue");
    });

    assert_eq!(alloc, (1, expected));
    let write = conn
        .events()
        .poll_write()
        .expect("write queue")
        .expect("queued write");
    let bytes = match write.prefix {
        WritePrefix::Owned(bytes) => Some(bytes),
        _ => None,
    }
    .expect("large borrowed DATA must have one exact owner");
    assert_eq!(bytes.capacity(), expected);
    assert_eq!(&bytes[expected - BODY.len()..], &BODY);
    assert!(write.payload.is_none());
}
