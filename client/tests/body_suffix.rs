use o3::buffer::{resident, segments};
use sark_client::protocol::codec::output::DATA_LIMIT;

pub mod common;
use common::{
    codec::Scope as _,
    response::{Decode as _, SUFFIX},
};

#[test]
fn sized_body_keeps_tail() {
    let bytes = SUFFIX;
    resident::Domain::try_scope(64 * DATA_LIMIT, |budget| {
        let segments = segments::Pool::try_new(segments::Plan {
            segments: 4,
            chains: 1,
        })
        .expect("body metadata");
        let response = segments.try_body(budget, "", bytes).expect("response");
        assert_eq!(response.body().segments().last(), Some(&b"l"[..]));
    })
    .expect("resident budget");
}
