use std::iter;

use dope_quic::errors::Code;
use sark_http::field::literal;

use crate::common::{atomic::Setup, field::entry};

pub mod common;

use dope_quic::wire::stream::Id;
use o3::buffer::{resident, storage};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Error,
        io::{Receive, Send, replies},
        output::WritePayload,
    },
    stream::Target,
};
use sark_http::payload::owned;

#[test]
fn rejects_partial_batch() {
    let mut conn = Setup::new(3, 2, usize::MAX)
        .try_build()
        .expect("egress configuration");
    let prepared = conn
        .reply()
        .prepare(
            Id::new(0).expect("stream id"),
            [literal!(entry(b":status", b"200"))],
        )
        .expect("response head encodes");

    assert!(matches!(
        replies::Data::try_new(WritePayload::Owned(b"body".to_vec()))
            .map(|body| prepared.complete(body).commit()),
        Ok(Err(Error::BudgetWire {
            retained: 0,
            additional: 11,
            limit: 2,
        })),
    ));
    assert!(conn.events().poll_write().expect("write queue").is_none());
}

#[test]
fn keeps_rejected_owner() {
    let mut conn = Setup::new(2, usize::MAX, usize::MAX)
        .try_build()
        .expect("egress configuration");
    let prepared = conn
        .reply()
        .prepare(
            Id::new(0).expect("stream id"),
            [literal!(entry(b":status", b"204"))],
        )
        .expect("response head encodes");
    let body = Vec::with_capacity(32);
    let pointer = body.as_ptr();

    let (error, body) = replies::Data::try_new(WritePayload::Owned(body))
        .err()
        .expect("empty DATA is rejected with its draft");
    assert!(
        matches!(&body, WritePayload::Owned(_)),
        "rejected draft retains its owned body"
    );
    if let WritePayload::Owned(body) = body {
        assert_eq!(error, replies::Error::Empty);
        assert_eq!(body.as_ptr(), pointer);
        prepared
            .finish()
            .commit()
            .expect("recovered head must finish");
        assert!(matches!(
            conn.events()
                .poll_write()
                .expect("write queue")
                .expect("queued write")
                .boundary(),
            Boundary::End
        ));
        assert!(conn.events().poll_write().expect("write queue").is_none());
    }
}

#[test]
fn releases_wire_credit() {
    let stream = Id::new(0).expect("stream id");
    let mut conn = Setup::new(8, 6, usize::MAX)
        .try_build()
        .expect("egress configuration");

    Send::new(&mut conn)
        .owned(stream, b"abc".to_vec(), Boundary::More)
        .expect("first write fits wire budget");
    assert!(
        (Send::new(&mut conn).owned(stream, b"abc".to_vec(), Boundary::More))
            == (Err(Error::BudgetWire {
                retained: 5,
                additional: 5,
                limit: 6,
            }))
    );
    {
        let released = conn
            .events()
            .poll_write()
            .expect("write queue")
            .expect("queued write");
        assert!((released.stream_id()) == (stream));
    }
    Send::new(&mut conn)
        .owned(stream, b"abc".to_vec(), Boundary::More)
        .expect("handoff releases wire charge");
}

#[test]
fn tracks_resident_owner() {
    let stream = Id::new(0).expect("stream id");
    let mut conn = Setup::new(8, 6, 64)
        .try_build()
        .expect("egress configuration");
    let owner = || {
        let mut backing = Vec::with_capacity(48);
        backing.push(0x5a);
        storage::Shared::try_from(&mut backing)
    };

    let first = (owner()).expect("first resident owner must allocate");
    assert!(
        Send::new(&mut conn)
            .shared(stream, first, Boundary::More)
            .is_ok(),
        "first owner fits resident budget"
    );
    let second = (owner()).expect("second resident owner must allocate");
    assert!(
        (Send::new(&mut conn).shared(stream, second, Boundary::More))
            == (Err(Error::BudgetResident {
                retained: 48,
                additional: 48,
                limit: 64,
            })),
        "two tiny slices must not hide ninety-six resident bytes"
    );
    {
        let released = conn
            .events()
            .poll_write()
            .expect("write queue")
            .expect("queued write");
        assert!((released.stream_id()) == (stream));
    }
    let retry = (owner()).expect("retried resident owner must allocate");
    assert!(
        Send::new(&mut conn)
            .shared(stream, retry, Boundary::More)
            .is_ok(),
        "handoff releases owner charge"
    );
}

#[test]
fn keeps_body_on_pressure() {
    const BODY: [u8; 32] = [0x62; 32];

    let stream = Id::new(0).expect("stream id");
    for critical in [false, true] {
        let mut conn = Setup::new(2, usize::MAX, usize::MAX)
            .try_build()
            .expect("egress configuration");
        if critical {
            let busy = Id::new(3).expect("critical stream");
            conn.control()
                .open(busy)
                .expect("control prefix enters queue");
            conn.control()
                .open_encoder(Id::new(7).expect("encoder stream"))
                .expect("critical writes fill queue");
        } else {
            Send::new(&mut conn)
                .data(stream, b"busy", Boundary::More)
                .expect("regular write fills queue");
        }
        let body = owned::Segments::from(resident::Shared::from(BODY.as_slice()));
        let pointer = body.segment(0).expect("one body segment").as_ptr();

        let (error, body) = conn
            .reply()
            .send(stream, body, Boundary::End)
            .expect_err("full queue returns the exact body");
        if critical {
            assert!(matches!(
                error,
                Error::Egress(o3::depot::queue::group::Error::Entries(shortage))
                    if shortage.requested.get() == 1 && shortage.available == 0
            ));
        } else {
            assert_eq!(
                error,
                Error::QueueRegular {
                    queued: 1,
                    additional: 1,
                    limit: 1,
                }
            );
        }
        assert_eq!(
            body.segment(0).expect("returned body segment").as_ptr(),
            pointer
        );
        for expected in if critical { &[3, 7][..] } else { &[0][..] } {
            let released = conn
                .events()
                .poll_write()
                .expect("write queue")
                .expect("queued write");
            assert_eq!(released.stream_id().get(), *expected);
        }
        assert!(
            conn.reply().send(stream, body, Boundary::End).is_ok(),
            "returned body must remain sendable",
        );
        let write = conn
            .events()
            .poll_write()
            .expect("write queue")
            .expect("queued write");
        let payload = write.payload.expect("DATA contains its original owner");
        assert!(
            matches!(&payload, WritePayload::Retained(_)),
            "retry preserves the single physical owner"
        );
        if let WritePayload::Retained(body) = payload {
            assert_eq!(body.as_slice().as_ptr(), pointer);
        }
    }
}

#[test]
fn static_owner_is_free() {
    const BODY: [u8; 256] = [0x73; 256];

    let stream = Id::new(0).expect("stream id");
    let mut conn = Setup::new(2, 1024, 2)
        .try_build()
        .expect("egress configuration");
    let body = owned::Segments::from(resident::Shared::from(BODY.as_slice()));
    assert!(
        conn.reply().send(stream, body, Boundary::More).is_ok(),
        "static owner fits write budget",
    );

    assert!(conn.wire_len() > BODY.len());
    assert_eq!(conn.resident_len(), 0);
    assert!(conn.retained(stream).expect("retained charge").is_empty());
    let write = conn
        .events()
        .poll_write()
        .expect("write queue")
        .expect("queued write");
    let payload = write.payload.expect("DATA contains its original owner");
    assert!(
        matches!(&payload, WritePayload::Retained(_)),
        "static DATA preserves its retained owner"
    );
    if let WritePayload::Retained(payload) = payload {
        assert_eq!(payload.as_slice(), BODY);
    }
}

#[test]
fn keeps_target() {
    let target = Target::new(Id::new(0).expect("stream id"));
    let mut conn = Setup::new(3, usize::MAX, usize::MAX)
        .try_build()
        .expect("egress configuration");
    let prepared = conn
        .reply()
        .prepare(target, [literal!(entry(b":status", b"200"))])
        .expect("fixture response head must encode");
    let done = replies::Data::try_new(WritePayload::Owned(b"resident-target".to_vec()))
        .map(|body| prepared.complete(body));
    assert!(done.is_ok(), "fixture body fits one DATA frame");
    if let Ok(batch) = done {
        let charge = batch.commit().expect("target response must commit");
        assert!(conn.retained(target.stream_id()).expect("retained charge") == charge);

        let headers = conn
            .events()
            .poll_write()
            .expect("write queue")
            .expect("queued write");
        assert!(headers.stream_id() == target.stream_id());
        assert!(matches!(headers.boundary(), Boundary::More));
        let data = conn
            .events()
            .poll_write()
            .expect("write queue")
            .expect("queued write");
        assert!(data.stream_id() == target.stream_id());
        assert!(matches!(data.boundary(), Boundary::End));
        assert!(conn.events().poll_write().expect("write queue").is_none());
    }
}

#[test]
fn stop_releases_target() {
    let first = Id::new(0).expect("stream id");
    let second = Id::new(4).expect("stream id");
    let mut conn = Setup::new(5, usize::MAX, usize::MAX)
        .try_build()
        .expect("egress configuration");
    let first_tx = conn
        .reply()
        .prepare(first, [literal!(entry(b":status", b"200"))])
        .expect("first fixture response head must encode");
    let done = replies::Data::try_new(WritePayload::Owned(b"first".to_vec()))
        .map(|body| first_tx.complete(body));
    assert!(done.is_ok(), "fixture body fits one DATA frame");
    if let Ok(batch) = done {
        let first_q = batch.commit().expect("first prepared response commits");
        let next_tx = conn
            .reply()
            .prepare(second, [literal!(entry(b":status", b"200"))])
            .expect("second fixture response head must encode");
        let done = replies::Data::try_new(WritePayload::Owned(b"second".to_vec()))
            .map(|body| next_tx.complete(body));
        assert!(done.is_ok(), "fixture body fits one DATA frame");
        if let Ok(batch) = done {
            let second_q = batch.commit().expect("second prepared response commits");

            assert!(
                matches!(
                    Receive::new(&mut conn).stop(first, None, Code::from_u16(0x10)),
                    Ok(released) if released == first_q
                ),
                "peer stop must release the exact target charge"
            );
            assert!(conn.retained(second).expect("retained charge") == second_q);
            let writes: Vec<_> =
                iter::from_fn(|| conn.events().poll_write().expect("write queue")).collect();
            assert_eq!(writes.len(), 2);
            assert!(writes.iter().all(|write| write.stream_id() == second));
        }
    }
}
