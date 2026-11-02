use std::ptr;

use crate::common::atomic::{
    Setup,
    pressure::{AcceptAll, BlockData, BlockOnce},
};

pub mod common;

use dope_quic::wire::stream::Id;
use dope_test::alloc::Tracker;
use o3::buffer::hold;
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{Error, io::Send},
    link::{Flush, Transport},
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn reserves_critical_slot() {
    let mut conn = Setup::new(16, 1024, 1024)
        .try_build()
        .expect("egress configuration");
    for _ in 0..15 {
        Send::new(&mut conn)
            .owned(Id::new(0).expect("stream id"), vec![0x41], Boundary::More)
            .expect("regular write fits reserved queue");
    }
    assert!(
        (Send::new(&mut conn).owned(Id::new(0).expect("stream id"), vec![0x42], Boundary::More))
            == (Err(Error::QueueRegular {
                queued: 15,
                additional: 1,
                limit: 15,
            })),
        "regular output must not consume the critical slot"
    );

    conn.control()
        .open(Id::new(3).expect("stream id"))
        .expect("critical lane remains admissible");
    assert!(
        (conn
            .events()
            .poll_write()
            .expect("write queue")
            .expect("queued write")
            .stream_id())
            == (Id::new(3).expect("stream id"))
    );
    assert!(
        (conn
            .events()
            .poll_write()
            .expect("write queue")
            .expect("queued write")
            .stream_id())
            == (Id::new(0).expect("stream id"))
    );
}

#[test]
fn critical_bypasses_block() {
    let mut conn = Setup::new(16, 1024, 1024)
        .try_build()
        .expect("egress configuration");
    Send::new(&mut conn)
        .owned(Id::new(0).expect("stream id"), vec![0x41], Boundary::More)
        .expect("blocked write enters queue");
    let mut link = BlockData::default();
    assert!((link.pump_writes(&mut conn).expect("infallible transport")) == (Flush::Blocked));
    assert_eq!(conn.wire_len(), 3);
    assert_eq!(conn.resident_len(), 1);

    conn.control()
        .open(Id::new(3).expect("stream id"))
        .expect("critical write bypasses saturation");
    assert!((link.pump_writes(&mut conn).expect("infallible transport")) == (Flush::Blocked));
    assert!((link.accepted) == ([Id::new(3).expect("stream id")]));
    assert_eq!(conn.wire_len(), 3);
    assert_eq!(conn.resident_len(), 1);
}

#[test]
fn reports_write_quantum() {
    let mut conn = Setup::new(64, 4096, 4096)
        .try_build()
        .expect("egress configuration");
    for _ in 0..33 {
        Send::new(&mut conn)
            .owned(Id::new(0).expect("stream id"), vec![0x41], Boundary::More)
            .expect("quantum write fits queue");
    }
    let mut link = AcceptAll;

    assert!((link.pump_writes(&mut conn).expect("infallible transport")) == (Flush::Pending));
    assert!((link.pump_writes(&mut conn).expect("infallible transport")) == (Flush::Drained));
}

#[test]
fn blocked_keeps_owner() {
    let mut conn = Setup::new(3, 7, usize::MAX)
        .try_build()
        .expect("egress configuration");
    Send::new(&mut conn)
        .owned(
            Id::new(0).expect("stream id"),
            b"body".to_vec(),
            Boundary::More,
        )
        .expect("blocked owner enters queue");
    let mut link = BlockOnce {
        blocked: false,
        payload: ptr::null(),
        mismatch: None,
    };

    assert!((link.pump_writes(&mut conn).expect("infallible transport")) == (Flush::Blocked));
    assert_eq!(conn.wire_len(), 6);
    assert_eq!(conn.resident_len(), 4);
    assert!(
        conn.retained(Id::new(0).expect("stream id"))
            .expect("retained charge")
            == hold::Charge::from_owner(4, 4)
    );
    assert!(
        conn.retained(Id::new(4).expect("stream id"))
            .expect("retained charge")
            .is_empty()
    );
    assert!(
        (Send::new(&mut conn).owned(
            Id::new(4).expect("stream id"),
            b"x".to_vec(),
            Boundary::More
        )) == (Err(Error::BudgetWire {
            retained: 6,
            additional: 3,
            limit: 7,
        })),
        "a popped but unaccepted owner must remain charged"
    );
    assert!((link.pump_writes(&mut conn).expect("infallible transport")) == (Flush::Drained));
    assert_eq!(link.mismatch, None, "blocked write keeps owner");
    assert_eq!(conn.wire_len(), 0);
    assert_eq!(conn.resident_len(), 0);
    assert!(
        conn.retained(Id::new(0).expect("stream id"))
            .expect("retained charge")
            .is_empty()
    );
    Send::new(&mut conn)
        .owned(
            Id::new(4).expect("stream id"),
            b"x".to_vec(),
            Boundary::More,
        )
        .expect("acceptance releases staging charge");
}
