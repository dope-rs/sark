use o3::depot::index::bitmap;
use sark_h2::stream::Key;

/// The first readiness admission did not fit its connection's row domain.
#[derive(Debug)]
pub struct Error {
    pub key: Key,
    pub cause: bitmap::Error,
}
