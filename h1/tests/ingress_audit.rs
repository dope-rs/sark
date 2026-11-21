pub mod common;

use std::{convert::Infallible, pin::Pin};

use common::session::Frame;
use sark_core::life::audit::Snapshot;
use sark_h1::{session, switch};

#[test]
fn bounded_head_total() {
    let mut conn = session::Conn::<(), Infallible, switch::Disabled, Infallible, ()>::default();
    let mut pending = Frame {
        head: Some(()),
        ..Frame::default()
    };
    let cycle = conn.drive(
        Pin::new(&mut pending),
        b"head",
        o3::buffer::write::Slice::new(&mut [0; 1]).reborrow(),
        &mut sark_h1::pump::Tail::Empty,
    );
    assert!(cycle.is_ok());
    let _ = conn.retain(b"x");

    let snapshot = conn.ingress.audit(|_| 0, |_| 0, |_| u32::MAX);
    let expected = Snapshot::default().request(1, u32::MAX as usize + 1, 2, 1);
    assert_eq!(snapshot, expected);
}
