use crate::common::stream;

pub mod common;

use dope_quic::wire::stream::Id;
use o3::buffer::{resident, storage};
use sark_core::manifold::stream::Boundary;
use sark_h3::{
    conn::{
        Connection,
        event::Role,
        io::{Receive, Step},
    },
    link::{Handoff, Status},
};

#[test]
fn unclassified_fin() {
    let mut conn = Connection::with_role(Role::Server).expect("connection");
    for (index, bytes) in stream::UNCLASSIFIED.into_iter().enumerate() {
        let id = Id::new(2 + 4 * index as u64).expect("stream");
        let bytes = storage::Shared::try_copy(bytes).expect("owner");
        let length = bytes.len();
        assert!(matches!(
            Receive::new(&mut conn).hold(id, resident::Shared::from(bytes), Boundary::End),
            Handoff::Accepted(Ok(Status::Open))
        ));
        let credit = conn.events().poll_credit().expect("credit");
        if length == 0 {
            assert!(credit.is_none());
        } else {
            assert!(
                matches!(credit, Some((stream, amount, _)) if stream == id && amount == length)
            );
        }
    }
    assert!(matches!(
        conn.events().poll_token().expect("events"),
        Step::Idle
    ));
}
