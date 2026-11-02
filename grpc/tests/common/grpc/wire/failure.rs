use std::io::Error;

use o3::alloc;
use sark_core::manifold::client::io;
use sark_grpc::Status;
use sark_h2::conn::{self, config};
use sark_http::field;

#[derive(Debug)]
pub enum Failure {
    Reserve(alloc::Error),
    Prepare(io::Error),
    Drive(Status),
    Request(conn::Error),
    Response(Status),
    Empty,
    Io(Error),
    Config(config::Error),
    Protocol(conn::Error),
    Field(field::Error),
}
