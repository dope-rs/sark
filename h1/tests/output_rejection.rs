use dope::manifold::network::listener::conn::write::{self, error};
use o3::buffer::resident::bound;
use sark_h1::server::payload::{self, write::Failure};

#[test]
fn capacity_cause() {
    let rejected = payload::write::reject::<(), bound::Unbounded, ()>(error::Error::Capacity(
        write::Reject::Header,
    ));
    assert!(matches!(rejected, Failure::Capacity(write::Reject::Header)));
}
