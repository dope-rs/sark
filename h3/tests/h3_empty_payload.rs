pub mod common;

use std::cell::Cell;

use common::conn::bridge::Pair;
use sark_core::manifold::stream::Boundary;
use sark_h3::{conn::event::Role, link::Flush, manifold::Session};

#[test]
fn empty_payload_transfer() {
    let client = Pair::try_config()
        .expect("transport options")
        .into_builder()
        .streams(|streams| streams.resident(64))
        .try_build()
        .expect("resident budget");
    let error = Cell::new(None);
    let Pair { mut client, .. } = Pair::try_with(
        Pair::try_config().expect("server options"),
        client,
        |found| error.set(Some(found.clone())),
    )
    .expect("connected peers");
    assert_eq!(error.into_inner(), None);
    let mut h3 = Session::with_role(Role::Client).expect("HTTP/3 session");
    let target = h3.egress().open(&mut client).expect("request stream");
    let empty = Vec::with_capacity(64);
    let resident = empty.capacity();
    h3.send()
        .bind(target)
        .expect("request owner")
        .owned(empty, Boundary::More)
        .expect("empty DATA frame");
    assert_eq!(h3.h3().resident_len(), resident);
    assert!(matches!(h3.egress().flush(&mut client), Ok(Flush::Drained)));
    assert_eq!(h3.h3().resident_len(), 0);
    assert_eq!(
        client
            .view()
            .with_streams(|streams| streams.usage().resident()),
        0
    );
    assert_eq!(
        client
            .view()
            .with_streams(|streams| streams.usage().wire_bytes()),
        2
    );
}
