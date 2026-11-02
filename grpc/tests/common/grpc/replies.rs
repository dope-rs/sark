use sark_h2::conn::{self, config};

use crate::common::grpc::call::calls;

pub const METHOD: &[u8] = b"/svc/Method";
pub const REQUEST: &[u8] = b"request";
pub const SMALL: &[u8] = b"application-owned-response";
pub const SHARED: &[u8] = &[b'x'; 256];

#[derive(Debug)]
pub enum Error {
    Build(config::Error),
    Ingress(conn::Error),
    MissingStream,
    Response(calls::Error),
}
