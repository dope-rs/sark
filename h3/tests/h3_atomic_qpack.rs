use sark_h3::conn::push;
use sark_http::field::literal;

use crate::common::{
    atomic::dynamic_sender,
    conn::{Events, payload::Write as _},
    field::entry,
    message::promise::Authority,
    qpack::Instruction,
    stream::Stream as _,
};

pub mod common;

use std::num::NonZero;

use dope_quic::wire::stream::Id;
use o3::depot::{capacity, queue::group};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection, Error,
        event::{Event, Role},
        io::{Receive, Send, replies},
        options,
        options::Config,
        output::WritePayload,
    },
    frame::{Header, STREAM_TYPE_QPACK_ENCODER, TYPE_HEADERS},
};

#[test]
fn headers_wait_for_section() {
    let mut conn = dynamic_sender(2, usize::MAX);
    conn.control()
        .open(Id::new(7).expect("control stream"))
        .expect("one total slot remains");
    let queued = conn.wire_len();
    for _ in 0..128 {
        assert!(matches!(
            Send::new(&mut conn).headers(
                Id::new(0).expect("stream id"),
                [literal!(entry(b"x-failed", b"must-not-survive"))],
                Boundary::More,
            ),
            Err(Error::Egress(group::Error::Entries(capacity::Shortage {
                requested: need,
                available: 1,
            }))) if need == NonZero::<usize>::MIN.saturating_add(1)
        ));
        assert_eq!(conn.wire_len(), queued, "failed batch publishes no bytes");
    }
    let control = conn
        .events()
        .poll_write()
        .expect("write queue")
        .expect("only control prefix queued");
    assert_eq!(control.stream_id().get(), 7);
    assert!(conn.events().poll_write().expect("write queue").is_none());

    let charge = Send::new(&mut conn)
        .headers(
            Id::new(4).expect("stream id"),
            [literal!(entry(b":status", b"204"))],
            Boundary::End,
        )
        .expect("fixed atomic QPACK scenario must succeed");
    assert!(charge.is_empty());
    let headers = conn
        .events()
        .poll_write()
        .expect("fixed atomic QPACK scenario must succeed")
        .expect("one static HEADERS write");
    assert!(headers.stream_id() == Id::new(4).expect("stream id"));
    assert!(
        Header::parse(&headers.prefix)
            .expect("fixed atomic QPACK scenario must succeed")
            .kind
            == TYPE_HEADERS
    );
    assert!(
        conn.events()
            .poll_write()
            .expect("fixed atomic QPACK scenario must succeed")
            .is_none()
    );
}

#[test]
fn byte_credit_is_atomic() {
    let mut conn = dynamic_sender(8, 8);
    assert!(matches!(
        Send::new(&mut conn).headers(
            Id::new(0).expect("stream id"),
            [literal!(entry(b"x-over-byte-budget", b"never-committed"))],
            Boundary::More,
        ),
        Err(Error::BudgetWire {
            retained: 0,
            additional,
            limit: 8,
        }) if additional > 8
    ));
    assert!(
        conn.events()
            .poll_write()
            .expect("fixed atomic QPACK scenario must succeed")
            .is_none()
    );

    let charge = Send::new(&mut conn)
        .headers(
            Id::new(4).expect("stream id"),
            [literal!(entry(b":status", b"204"))],
            Boundary::End,
        )
        .expect("fixed atomic QPACK scenario must succeed");
    assert!(charge.is_empty());
    assert!(
        conn.events()
            .poll_write()
            .expect("fixed atomic QPACK scenario must succeed")
            .expect("fixed atomic QPACK scenario must succeed")
            .stream_id()
            == Id::new(4).expect("stream id")
    );
    assert!(
        conn.events()
            .poll_write()
            .expect("fixed atomic QPACK scenario must succeed")
            .is_none()
    );
}

#[test]
fn push_batch_is_atomic() {
    let mut conn = dynamic_sender(2, usize::MAX);
    conn.control()
        .open(Id::new(7).expect("control stream"))
        .expect("one total slot remains");
    conn.allow_push(Role::Server, 7).expect("push grant");
    assert!(matches!(
        Send::new(&mut conn).promise(Id::new(0).expect("stream id"), 7, [literal!(entry(b"x-push", b"must-not-survive"))],),
        Err(Error::Egress(group::Error::Entries(capacity::Shortage {
            requested: need,
            available: 1,
        }))) if need == NonZero::<usize>::MIN.saturating_add(1)
    ));
    assert!(matches!(
        Send::new(&mut conn).cancel(7),
        Err(Error::Push(push::Error::Unknown { push_id: 7 }))
    ));
    let control = conn
        .events()
        .poll_write()
        .expect("write queue")
        .expect("only control prefix queued");
    assert_eq!(control.stream_id().get(), 7);
    assert!(
        conn.events()
            .poll_write()
            .expect("fixed atomic QPACK scenario must succeed")
            .is_none()
    );

    let charge = Send::new(&mut conn)
        .headers(
            Id::new(4).expect("stream id"),
            [literal!(entry(b":status", b"204"))],
            Boundary::End,
        )
        .expect("fixed atomic QPACK scenario must succeed");
    assert!(charge.is_empty());
    assert!(
        conn.events()
            .poll_write()
            .expect("fixed atomic QPACK scenario must succeed")
            .expect("fixed atomic QPACK scenario must succeed")
            .stream_id()
            == Id::new(4).expect("stream id")
    );
    assert!(
        conn.events()
            .poll_write()
            .expect("fixed atomic QPACK scenario must succeed")
            .is_none()
    );
}

#[test]
fn dynamic_batch_decodes() {
    let mut sender = dynamic_sender(3, usize::MAX);
    let body = b"owner".to_vec();
    let pointer = body.as_ptr();
    let prepared = sender
        .reply()
        .prepare(
            Id::new(0).expect("stream id"),
            [
                literal!(entry(b":status", b"200")),
                literal!(entry(b"x-dynamic", b"committed-owned-prefix")),
            ],
        )
        .expect("fixed atomic QPACK scenario must succeed");
    let done =
        replies::Data::try_new(WritePayload::Owned(body)).map(|body| prepared.complete(body));
    assert!(done.is_ok(), "fixture body fits one DATA frame");
    if let Ok(batch) = done {
        let charge = batch.commit().expect("dynamic batch must commit");
        assert_eq!(charge, sender.retained(Id::ZERO).expect("response owners"));

        let qpack = sender
            .events()
            .poll_write()
            .expect("fixed atomic QPACK scenario must succeed")
            .expect("QPACK delta");
        let headers = sender
            .events()
            .poll_write()
            .expect("fixed atomic QPACK scenario must succeed")
            .expect("HEADERS");
        let data = sender
            .events()
            .poll_write()
            .expect("fixed atomic QPACK scenario must succeed")
            .expect("DATA");
        assert!(qpack.stream_id() == Id::new(3).expect("stream id"));
        assert!(headers.stream_id() == Id::new(0).expect("stream id"));
        assert!(data.stream_id() == Id::new(0).expect("stream id"));
        let payload = data
            .payload
            .as_ref()
            .expect("DATA must retain its owned payload");
        assert!(
            matches!(&payload, WritePayload::Owned(_)),
            "DATA must retain its owned payload"
        );
        if let WritePayload::Owned(body) = payload {
            assert_eq!(body.as_ptr(), pointer);

            let mut receiver = Connection::with_config(
                Role::Client,
                Config {
                    qpack: options::Qpack {
                        decoder: options::Decode {
                            table_bytes: 256,
                            blocked_streams: 1,
                        },
                        ..options::Qpack::default()
                    },
                    ..Config::default()
                },
            )
            .expect("fixed atomic QPACK scenario must succeed");
            Send::new(&mut receiver)
                .headers(Id::ZERO, Authority, Boundary::End)
                .expect("originating request");
            let mut wire = Vec::new();
            STREAM_TYPE_QPACK_ENCODER.encode(&mut wire).expect("varint");
            Instruction::Capacity(256)
                .try_encode(&mut wire)
                .expect("QPACK fixture");
            wire.extend(qpack.into_bytes());
            Receive::new(&mut receiver)
                .owned(Id::new(3).expect("stream id"), wire, Boundary::More)
                .expect("fixed atomic QPACK scenario must succeed");
            Receive::new(&mut receiver)
                .owned(
                    Id::new(0).expect("stream id"),
                    headers.into_bytes(),
                    Boundary::More,
                )
                .expect("fixed atomic QPACK scenario must succeed");
            assert!(match Events::new(&mut receiver)
                .poll_token()
                .expect("fixed atomic QPACK scenario must succeed")
            {
                Some(Event::Headers { head, .. }) => head.into_parts().0.iter().any(|field| {
                    field.name == b"x-dynamic" && field.value == b"committed-owned-prefix"
                }),
                _ => false,
            });
        }
    }
}
