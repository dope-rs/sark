use dope_quic::wire::stream::Id;
use sark_h3::{
    conn::{
        io::replies,
        output::{Write, WritePayload},
    },
    frame::Header,
};
use sark_http::field::literal;

use crate::common::{atomic::Setup, field::entry};

pub mod common;

use dope_quic::wire::varint::VarInt;
use dope_test::alloc::Tracker;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::Connection,
    frame::{TYPE_DATA, TYPE_HEADERS},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn empty_finishes_headers() {
    let stream = Id::new(0).expect("stream id");
    let mut conn = Setup::new(2, usize::MAX, usize::MAX)
        .try_build()
        .expect("egress configuration");
    let prepared = conn
        .reply()
        .prepare(stream, [literal!(entry(b":status", b"204"))])
        .expect("empty fixture response head must encode");

    let charge = prepared
        .finish()
        .commit()
        .expect("empty response must commit in one write slot");
    assert!(charge.is_empty());
    assert!(conn.retained(stream).expect("retained charge") == charge);

    let headers = frame(&mut conn, TYPE_HEADERS);
    assert!(matches!(headers.boundary(), Boundary::End));
    assert!(conn.events().poll_write().expect("write queue").is_none());
}

#[test]
fn keeps_body_owner() {
    let stream = Id::new(0).expect("stream id");
    let mut conn = Setup::new(3, usize::MAX, usize::MAX)
        .try_build()
        .expect("egress configuration");
    let body = b"owner-preserved".to_vec();
    let pointer = body.as_ptr();
    let prepared = conn
        .reply()
        .prepare(stream, [literal!(entry(b":status", b"200"))])
        .expect("fixture response head must encode");

    let done =
        replies::Data::try_new(WritePayload::Owned(body)).map(|body| prepared.complete(body));
    assert!(done.is_ok(), "fixture body fits one DATA frame");
    if let Ok(batch) = done {
        let charge = batch.commit().expect("prepared response commits");
        assert_eq!(charge.logical_bytes, b"owner-preserved".len());
        assert_eq!(charge.resident_bytes, conn.resident_len());
        assert_eq!(charge.owners, 1);

        let headers = frame(&mut conn, TYPE_HEADERS);
        assert!(matches!(headers.boundary(), Boundary::More));
        let data = frame(&mut conn, TYPE_DATA);
        let boundary = data.boundary();
        let payload = data.payload.expect("response retains its owned body");
        assert!(
            matches!(&payload, WritePayload::Owned(_)),
            "response retains its owned body"
        );
        if let WritePayload::Owned(body) = payload {
            assert_eq!(body.as_ptr(), pointer);
            assert_eq!(body, b"owner-preserved");
            assert!(matches!(boundary, Boundary::End));
            assert!(conn.events().poll_write().expect("write queue").is_none());
        }
    }
}

fn frame<'d>(conn: &mut Connection<'d>, kind: VarInt) -> Write<'d> {
    let write = (conn)
        .events()
        .poll_write()
        .expect("write queue")
        .expect("queued write");
    assert!(
        (Header::parse(&write.prefix)
            .expect("valid frame prefix")
            .kind)
            == (kind)
    );
    write
}
