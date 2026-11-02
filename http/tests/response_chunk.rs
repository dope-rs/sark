use o3::buffer::resident;
use sark_http::payload::response::dispatch::chunk::Chunk;

#[test]
fn empty_owner_is_returned() {
    let empty = Chunk::try_new(resident::Shared::new())
        .err()
        .expect("empty response owner must be rejected");
    assert!(empty.owner().is_empty());
}
