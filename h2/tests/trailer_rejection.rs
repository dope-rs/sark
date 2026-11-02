use common::{
    capacity,
    hpack::Fields,
    protocol::{
        Session,
        session::{Frame, Wire},
        trailers,
    },
};
use dope_test::alloc::Tracker;
use o3::buffer::pool::Pool;
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    conn::event::Event,
    frame::{Block, numbers::ErrorCode},
    hpack::encode::{Encoder, Mode},
    stream::Id,
};

pub mod common;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn rejects_complete() {
    let mut conn = trailers::try_server().expect("bounded server connection");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let mut enc = Encoder::try_new(4096).expect("HPACK encoder");
    enc.set_mode(Mode::Plain);

    conn.ingress()
        .ingest(
            &capacity::Fields::new(Id::FIRST_CLIENT, &trailers::REQUEST)
                .try_headers(&mut enc, Boundary::More)
                .expect("initial request fixture"),
        )
        .expect("initial request");
    {
        let held = conn.ingress().poll_event().expect("request event");
        assert!(matches!(&held, Event::Headers { .. }));

        let block = Fields::new(&trailers::FIELDS)
            .try_with(&mut enc)
            .expect("trailer block");
        let wire = Frame::new(1, &block)
            .try_headers(Boundary::More, Block::End)
            .expect("invalid trailers");
        let pool = Pool::try_new(1, wire.len()).expect("receive pool");
        let mut lease = pool.acquire().expect("receive owner");
        lease.try_append(&wire).expect("receive bytes").commit();
        let retained = lease.freeze().into_shared();
        let (_, alloc) = Tracker::usage(|| {
            conn.ingress()
                .ingest_owned(retained)
                .expect("decode-only trailer rejection");
        });
        assert_eq!(alloc, (0, 0));
        assert!(pool.acquire().is_some(), "invalid trailer owner returned");
        assert!(
            matches!(conn.ingress().poll_event(), Some(Event::StreamReset {
                stream_id: id,
                error: ErrorCode::ProtocolError,
                ..
            }) if id == Id::FIRST_CLIENT)
        );
        assert_eq!(
            Wire::new(conn.output().first())
                .try_reset()
                .expect("server reset"),
            Some((Id::FIRST_CLIENT, ErrorCode::ProtocolError)),
        );
    }
    conn.transfer().drain(usize::MAX);
    let mut block = Fields::new(&trailers::REQUEST)
        .try_with(&mut enc)
        .expect("follow-up request block");
    let indexed = Fields::new(&trailers::FIELDS)
        .try_with(&mut enc)
        .expect("indexed trailer block");
    assert_eq!(
        indexed.len(),
        1,
        "follow-up field must use the dynamic table"
    );
    block.extend_from_slice(&indexed);
    let next = Frame::new(3, &block)
        .try_headers(Boundary::End, Block::End)
        .expect("dynamic follow-up request");
    conn.ingress()
        .ingest(&next)
        .expect("HPACK state remains synchronized");
    assert!(
        matches!(conn.ingress().poll_event(), Some(Event::Headers { stream_id: id, .. }) if id == Id::new(3).expect("next stream"))
    );
}

#[test]
fn rejects_fragments() {
    let mut conn = trailers::try_server().expect("bounded server connection");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    let mut enc = Encoder::try_new(4096).expect("HPACK encoder");
    enc.set_mode(Mode::Plain);
    conn.ingress()
        .ingest(
            &capacity::Fields::new(Id::FIRST_CLIENT, &trailers::REQUEST)
                .try_headers(&mut enc, Boundary::More)
                .expect("initial request fixture"),
        )
        .expect("initial request");
    let held = conn.ingress().poll_event().expect("request event");
    assert!(matches!(&held, Event::Headers { .. }));

    let block = Fields::new(&trailers::FIELDS)
        .try_with(&mut enc)
        .expect("trailer block");
    let split = block.len() / 2;
    assert!(split != 0 && split != block.len());
    let first = Frame::new(1, &block[..split])
        .try_headers(Boundary::More, Block::More)
        .expect("fragmented invalid trailers");
    conn.ingress()
        .ingest(&first)
        .expect("begin decode-only trailer block");
    assert!(conn.ingress().poll_event().is_none());

    let tail = Frame::new(1, &block[split..])
        .try_continue(Block::End)
        .expect("final continuation");
    conn.ingress()
        .ingest(&tail)
        .expect("finish decode-only trailer block");
    assert!(
        matches!(conn.ingress().poll_event(), Some(Event::StreamReset {
            stream_id: id,
            error: ErrorCode::ProtocolError,
            ..
        }) if id == Id::FIRST_CLIENT)
    );
}
