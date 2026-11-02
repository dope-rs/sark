use dope_quic::wire::stream::Id;
use sark_core::manifold::stream::Boundary;
use sark_h3::conn::{self, Connection, io::Receive, options};
use sark_http::field::{Entry, metadata::block::Connect};

use crate::common::conn::{
    Connection as _,
    build::{self, Fields as _},
};

/// Static request pseudo-fields followed by a literal uppercase field name.
pub const UPPERCASE: &[u8] = b"\x01\x17\x00\x00\xd1\xd7\x50\x0cexample.test\xc1\x21X\x01v";
pub const INVALID_PATH: &[u8] = b"x";

#[derive(Debug)]
pub enum Error {
    Config(options::Error),
    Fields(build::Build),
    Connection(conn::Error),
}

pub fn try_ingest_request(fields: &[Entry<'_>]) -> Result<(), Error> {
    let mut conn = Connection::try_validate(Connect::Disabled).map_err(Error::Config)?;
    let wire = fields.try_headers().map_err(Error::Fields)?;
    Receive::new(&mut conn)
        .owned(Id::ZERO, wire, Boundary::More)
        .map_err(Error::Connection)
}
