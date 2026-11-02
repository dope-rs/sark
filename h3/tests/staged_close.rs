pub mod common;

use common::conn::bridge::{Pair, certificate};
use sark_core::manifold::stream::Boundary;
use sark_h3::{link::Flush, manifold::Session};
use sark_http::owner::{Change, cause};

#[test]
fn closes_remaining_output() {
    let Pair {
        client: mut quic, ..
    } = Pair::try_new(certificate).expect("QUIC pair");
    let mut session = Session::new().expect("session");
    let first = session.egress().open(&mut quic).expect("first stream");
    session
        .send()
        .bind(first)
        .expect("first owner")
        .owned(vec![1; 64], Boundary::End)
        .expect("first output");
    assert!(matches!(
        session.egress().flush(&mut quic).expect("transport output"),
        Flush::Drained
    ));
    let ids = [(2, 128), (3, 256)].map(|(byte, len)| {
        let target = session.egress().open(&mut quic).expect("staged stream");
        session
            .send()
            .bind(target)
            .expect("staged owner")
            .owned(vec![byte; len], Boundary::End)
            .expect("staged output");
        target.stream_id()
    });
    let charges = ids.map(|id| session.h3().retained(id).expect("staged charge"));
    let mut released = Vec::new();
    session.close(|effect| released.push(effect));
    assert_eq!(released.len(), 2);
    for ((id, charge), effect) in ids.into_iter().zip(charges).zip(released) {
        assert!(*effect.key() == id);
        assert!(effect.charge() == charge);
        assert!(matches!(
            effect.change(),
            Change::StagingReleased(cause::Staging::Closed)
        ));
    }
}
