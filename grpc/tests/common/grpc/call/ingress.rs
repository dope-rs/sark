use sark_grpc::{Status, protocol::request::Scheme};
use sark_h2::conn::{self, DataOutcome, config};

pub const SCHEMES: [(Scheme, &[u8]); 2] = [(Scheme::Http, b"http"), (Scheme::Https, b"https")];

pub const INCOMPLETE_RESPONSES: [&[u8]; 2] =
    [&[0, 0, 0, 0, 0, 0], &[0, 0, 0, 0, 0, 0, 0, 0, 0, 2, b'x']];

#[derive(Debug)]
pub enum Failure {
    Build(config::Error),
    Wire(conn::Error),
    Status(Status),
    Data {
        expected: usize,
        actual: DataOutcome,
    },
}
