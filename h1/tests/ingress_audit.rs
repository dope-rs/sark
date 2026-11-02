pub mod common;

use std::{convert::Infallible, pin::Pin};

use common::session::audit::Pending;
use sark_core::life::audit::Snapshot;
use sark_h1::{pump, session, switch, wire::head};

#[test]
fn bounded_head_total() {
    let mut conn = session::Conn::<(), Infallible, switch::Disabled, Infallible, ()>::default();
    let mut pending = Pending;
    let cycle = pump::Ingress::drive(
        &mut conn,
        Pin::new(&mut pending),
        b"head",
        &mut [0; 1],
        head::Connection::KeepAlive,
    );
    assert!(cycle.state.is_ok());
    let _ = pump::Ingress::retain(&mut conn, b"x");

    let snapshot = conn.ingress.audit(|_| 0, |_| 0, |_| u32::MAX);
    let expected = Snapshot::default().request(1, u32::MAX as usize + 1, 2, 1);
    assert_eq!(snapshot, expected);
}
