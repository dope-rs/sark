use o3::alloc;
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    Status,
    client::session,
    protocol::{Block, section},
};
use sark_h2::{
    ServerRole, client,
    conn::{self, machine::Connection},
    frame::{numbers::ErrorCode, payload::GoAway},
    stream,
};

use crate::common::grpc::wire::{Failure, Peer, Transfer};

pub fn try_peer(
    client: &mut session::Session<'_, client::Plan<section::Plan>>,
) -> Result<(Connection<ServerRole>, Vec<stream::Key>), Failure> {
    let (mut server, streams) = Peer::try_server(client)?;
    Transfer {
        client,
        server: &mut server,
    }
    .try_response()?;
    Ok((server, streams))
}

#[derive(Debug)]
pub enum Error {
    Encode(alloc::Error),
    Status(Status),
    Protocol(conn::Error),
    Goaway { last: stream::Id, code: ErrorCode },
}

pub trait Last {
    fn try_goaway(self, code: ErrorCode) -> Result<Vec<u8>, Error>;
}

impl Last for stream::Id {
    fn try_goaway(self, code: ErrorCode) -> Result<Vec<u8>, Error> {
        let mut wire = Vec::new();
        GoAway::new(self, code, b"peer retirement")
            .ok_or(Error::Goaway { last: self, code })?
            .encode(&mut wire)
            .map_err(Error::Encode)?;
        Ok(wire)
    }
}

pub trait Server {
    fn try_finish(&mut self, stream: stream::Key) -> Result<(), Error>;
}

impl Server for Connection<ServerRole> {
    fn try_finish(&mut self, stream: stream::Key) -> Result<(), Error> {
        let mut fields = Block::new();
        fields.push(b":status", b"200").map_err(Error::Status)?;
        fields
            .push(b"content-type", b"application/grpc+proto")
            .map_err(Error::Status)?;
        fields.push(b"grpc-status", b"0").map_err(Error::Status)?;
        self.streams()
            .headers()
            .respond(stream.id(), &fields, Boundary::End)
            .map_err(Error::Protocol)
    }
}
