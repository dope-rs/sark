use common::{
    hpack::{Fields, GET},
    protocol::{
        Session,
        conn::{Control, Controls},
        session::{Frame, Wire},
    },
};
use sark_h2::conn::event::Event;

use crate::common::headers::entry;
pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole, Endpoint,
    conn::{Association, Error, Protocol, config::Config, machine::Connection},
    frame,
    frame::{Ack, numbers::ErrorCode},
    stream,
    stream::Id,
};
use sark_http::field::{self, metadata::map::record::Entry};

use crate::common::protocol::session::Authority;

#[test]
fn push_registers_stream() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");

    let id = conn
        .streams()
        .headers()
        .request(
            [
                field::literal!(entry(b":method", b"GET")),
                field::literal!(entry(b":scheme", b"http")),
                field::literal!(entry(b":path", b"/")),
                field::literal!(entry(b":authority", b"x")),
            ],
            Boundary::End,
        )
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);

    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/a"),
        Entry::new(b":authority", b"x"),
    ])
    .try_encode()
    .expect("protocol fixture");
    conn.ingress()
        .ingest(
            &Frame::new(id.into_u32(), &block)
                .try_push(2, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");

    let ev = conn
        .ingress()
        .poll_event()
        .expect("scenario emits the expected event");
    let (parent, pushed) = match ev {
        Event::PushPromise {
            stream_id: parent,
            promised_stream_id: pushed,
            ..
        } => Some((parent, pushed)),
        _ => None,
    }
    .expect("scenario must emit PushPromise");
    assert!((parent) == (id));
    assert!((pushed) == (Id::new(2).expect("fixture domain")));
    assert!(
        (conn
            .status()
            .stream_state(Id::new(2).expect("fixture domain")))
            == (Some(stream::State::ReservedRemote))
    );
    assert_eq!(
        conn.ingress()
            .ingest(&Control::new(2).try_window(1).expect("window fixture")),
        Err(Error::Protocol(Protocol::WindowUpdate {
            stream: pushed.id(),
            state: stream::State::ReservedRemote,
        }))
    );
}

#[test]
fn rejects_push_on_tunnel() {
    let block = Fields::new(&GET).try_encode().expect("push fixture");
    for status in [None, Some(b"200"), Some(b"403")] {
        for fragmented in [false, true] {
            let mut conn =
                Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
            Session::new(&mut conn)
                .try_prime()
                .expect("protocol fixture");
            let id = conn
                .streams()
                .headers()
                .request(common::response::CONNECT, Boundary::More)
                .expect("CONNECT request");
            conn.transfer().drain(usize::MAX);
            if let Some(status) = status {
                let response = Fields::new(&[Entry::new(b":status", status)])
                    .try_encode()
                    .expect("response fixture");
                conn.ingress()
                    .ingest(
                        &Frame::new(id.into_u32(), &response)
                            .try_headers(Boundary::More, frame::Block::End)
                            .expect("response frame"),
                    )
                    .expect("CONNECT response");
                assert!(matches!(
                    conn.ingress().poll_event(),
                    Some(Event::Headers { .. })
                ));
            }
            let split = if fragmented {
                block.len() / 2
            } else {
                block.len()
            };
            let result = conn.ingress().ingest(
                &Frame::new(id.into_u32(), &block[..split])
                    .try_push(
                        2,
                        if fragmented {
                            frame::Block::More
                        } else {
                            frame::Block::End
                        },
                    )
                    .expect("push frame"),
            );
            if status == Some(b"200") {
                assert_eq!(
                    result,
                    Err(Error::Protocol(Protocol::Association(Association::Tunnel(
                        id
                    ))))
                );
            } else {
                result.expect("unconnected stream accepts push");
                if fragmented {
                    conn.ingress()
                        .ingest(
                            &Frame::new(id.into_u32(), &block[split..])
                                .try_continue(frame::Block::End)
                                .expect("continuation frame"),
                        )
                        .expect("complete push");
                }
                assert!(matches!(
                    conn.ingress().poll_event(),
                    Some(Event::PushPromise { .. })
                ));
            }
        }
    }
}

#[test]
fn push_resumes_event() {
    let mut conn = Connection::<ClientRole>::try_push::<Authority>(Config {
        event_capacity: 2,
        ..Config::default()
    })
    .expect("protocol scenario precondition");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);

    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/promised"),
        Entry::new(b":authority", b"x"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let split = block.len() / 2;
    let mut wire = Controls::new(Ack::Required)
        .try_ping([1; 8])
        .expect("ping fixture");
    wire.extend_from_slice(
        &Controls::new(Ack::Required)
            .try_ping([2; 8])
            .expect("ping fixture"),
    );
    wire.extend_from_slice(
        &Frame::new(1, &block[..split])
            .try_push(2, frame::Block::More)
            .expect("protocol fixture"),
    );
    wire.extend_from_slice(
        &Frame::new(1, &block[split..])
            .try_continue(frame::Block::End)
            .expect("protocol fixture"),
    );

    assert!(conn.ingress().ingest(&wire) == Err(Error::EventFull { used: 2, limit: 2 }));
    assert!(
        matches!(conn.ingress().poll_event(), Some(Event::Ping { opaque, .. }) if opaque == [1; 8])
    );
    assert!(
        matches!(conn.ingress().poll_event(), Some(Event::Ping { opaque, .. }) if opaque == [2; 8])
    );
    conn.ingress()
        .resume()
        .expect("protocol scenario precondition");
    assert!(
        matches!(conn.ingress().poll_event(), Some(Event::PushPromise {
            stream_id,
            promised_stream_id,
            ..
        }) if stream_id == Id::new(1).expect("fixture domain") && promised_stream_id == Id::new(2).expect("fixture domain"))
    );
}

#[test]
fn accepts_push_on_open() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::More)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);
    assert!(
        (conn
            .status()
            .stream_state(Id::new(1).expect("fixture domain")))
            == (Some(stream::State::Open))
    );

    conn.ingress()
        .ingest(
            &Frame::new(
                1,
                &Fields::new(&GET).try_encode().expect("protocol fixture"),
            )
            .try_push(2, frame::Block::End)
            .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");

    assert!(
        matches!(conn.ingress().poll_event(), Some(Event::PushPromise {
            stream_id,
            promised_stream_id,
            ..
        }) if stream_id == Id::new(1).expect("fixture domain") && promised_stream_id == Id::new(2).expect("fixture domain"))
    );
    assert!(
        (conn
            .status()
            .stream_state(Id::new(2).expect("fixture domain")))
            == (Some(stream::State::ReservedRemote))
    );
}

#[test]
fn rejects_unsafe_push() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);
    let block = Fields::new(&[
        Entry::new(b":method", b"POST"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/unsafe"),
        Entry::new(b":authority", b"x"),
    ])
    .try_encode()
    .expect("protocol fixture");

    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_push(2, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");

    assert!(conn.ingress().poll_event().is_none());
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"))
            == (Some((
                Id::new(2).expect("fixture domain"),
                ErrorCode::ProtocolError
            )))
    );
    assert!(
        !conn
            .status()
            .has_stream(Id::new(2).expect("fixture domain"))
    );
}

#[test]
fn rejects_content_push() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);
    let block = Fields::new(&[
        Entry::new(b":method", b"GET"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/content"),
        Entry::new(b":authority", b"x"),
        Entry::new(b"content-length", b"1"),
    ])
    .try_encode()
    .expect("protocol fixture");
    let split = block.len() / 2;

    conn.ingress()
        .ingest(
            &Frame::new(1, &block[..split])
                .try_push(2, frame::Block::More)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");
    assert!(conn.ingress().poll_event().is_none());
    conn.ingress()
        .ingest(
            &Frame::new(1, &block[split..])
                .try_continue(frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");

    assert!(conn.ingress().poll_event().is_none());
    assert!(
        (Wire::new(conn.output().first())
            .try_reset()
            .expect("protocol fixture"))
            == (Some((
                Id::new(2).expect("fixture domain"),
                ErrorCode::ProtocolError
            )))
    );
    assert!(
        !conn
            .status()
            .has_stream(Id::new(2).expect("fixture domain"))
    );
}

#[test]
fn accepts_empty_head_push() {
    let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
    Session::new(&mut conn)
        .try_prime()
        .expect("protocol fixture");
    conn.streams()
        .headers()
        .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
        .expect("protocol scenario precondition");
    conn.transfer().drain(usize::MAX);
    let block = Fields::new(&[
        Entry::new(b":method", b"HEAD"),
        Entry::new(b":scheme", b"http"),
        Entry::new(b":path", b"/metadata"),
        Entry::new(b":authority", b"x"),
        Entry::new(b"content-length", b"0"),
    ])
    .try_encode()
    .expect("protocol fixture");

    conn.ingress()
        .ingest(
            &Frame::new(1, &block)
                .try_push(2, frame::Block::End)
                .expect("protocol fixture"),
        )
        .expect("protocol scenario precondition");

    assert!(
        matches!(conn.ingress().poll_event(), Some(Event::PushPromise {
            promised_stream_id,
            ..
        }) if promised_stream_id == Id::new(2).expect("fixture domain"))
    );
    assert!(
        (conn
            .status()
            .stream_state(Id::new(2).expect("fixture domain")))
            == (Some(stream::State::ReservedRemote))
    );
    let response = Fields::new(&[
        Entry::new(b":status", b"200"),
        Entry::new(b"content-length", b"42"),
    ])
    .try_encode()
    .expect("HEAD response block");
    conn.ingress()
        .ingest(
            &Frame::new(2, &response)
                .try_headers(Boundary::End, frame::Block::End)
                .expect("HEAD response frame"),
        )
        .expect("HEAD response ingress");
    assert!(matches!(
        conn.ingress().poll_event(),
        Some(Event::Headers { stream_id, boundary: Boundary::End, .. })
            if stream_id == Id::new(2).expect("fixture domain")
    ));
}

#[test]
fn post_goaway_refuses_push() {
    let block = Fields::new(&GET).try_encode().expect("protocol fixture");
    let promised = Id::new(2).expect("fixture domain");
    for split in [0, block.len() / 2] {
        let mut conn = Connection::<ClientRole>::with_push::<Authority>().expect("client fixture");
        Session::new(&mut conn)
            .try_prime()
            .expect("protocol fixture");
        conn.streams()
            .headers()
            .request([field::literal!(entry(b":method", b"GET"))], Boundary::End)
            .expect("request fixture");
        conn.transfer().drain(usize::MAX);
        if split != 0 {
            conn.ingress()
                .ingest(
                    &Frame::new(1, &block[..split])
                        .try_push(2, frame::Block::More)
                        .expect("protocol fixture"),
                )
                .expect("partial PUSH_PROMISE");
        }
        conn.control().goaway(ErrorCode::NoError);
        conn.transfer().drain(usize::MAX);

        let frame = Frame::new(1, &block[split..]);
        let wire = if split == 0 {
            frame.try_push(2, frame::Block::End)
        } else {
            frame.try_continue(frame::Block::End)
        }
        .expect("protocol fixture");
        conn.ingress().ingest(&wire).expect("refused push");
        assert!(!conn.status().has_stream(promised));
        assert!(conn.ingress().poll_event().is_none());
        assert_eq!(
            Wire::new(conn.output().first())
                .try_reset()
                .expect("protocol fixture"),
            Some((promised, ErrorCode::RefusedStream))
        );
        conn.transfer().drain(usize::MAX);

        assert!(matches!(
            conn.ingress().ingest(
                &Frame::new(1, &block)
                    .try_push(2, frame::Block::End)
                    .expect("protocol fixture")
            ),
            Err(Error::Protocol(Protocol::Peer {
                actual,
                last,
                endpoint: Endpoint::Client,
            })) if actual == promised && last == promised
        ));
        assert!(conn.ingress().poll_event().is_none());
    }
}
