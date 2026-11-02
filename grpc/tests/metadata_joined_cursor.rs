pub mod common;

use common::headers;
use sark_grpc::Status;

#[test]
fn retains_following_field() {
    let status = Status::from_fields(&headers::try_joined().expect("joined fields"))
        .expect("joined metadata");
    assert_eq!(
        status
            .metadata()
            .entries()
            .map(|entry| (entry.name, entry.value, entry.indexing))
            .collect::<Vec<_>>(),
        headers::JOINED,
    );
}
