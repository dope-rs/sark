use sark_core::manifold::client::io::Output;
use sark_grpc::{
    client::{
        io::{Session as _, ingress::Session as _},
        session::Session,
    },
    protocol::section,
};
use sark_h2::{
    ServerRole, client,
    conn::{event::Event, machine::Connection},
    stream::Id,
};

use crate::common::grpc::wire::Failure;

pub struct Transfer<'a, 'scope> {
    pub client: &'a mut Session<'scope, client::Plan<section::Plan>>,
    pub server: &'a mut Connection<ServerRole>,
}

impl Transfer<'_, '_> {
    pub fn try_request(&mut self) -> Result<Vec<Id>, Failure> {
        let mut wrote = false;
        let mut streams = Vec::new();
        loop {
            let mut prefix = vec![0; 64 * 1024];
            let (write, driven) = self
                .client
                .take_write(&mut prefix)
                .map_err(Failure::Prepare)?;
            driven.map_err(Failure::Drive)?;
            match write {
                Output::Buffered(0) => break,
                Output::Buffered(len) => {
                    wrote = true;
                    self.server
                        .ingress()
                        .ingest(&prefix[..len])
                        .map_err(Failure::Request)?;
                }
                Output::Split {
                    prefix_len: len,
                    body,
                } => {
                    wrote = true;
                    self.server
                        .ingress()
                        .ingest(&prefix[..len])
                        .map_err(Failure::Request)?;
                    self.server
                        .ingress()
                        .ingest(body.as_slice())
                        .map_err(Failure::Request)?;
                }
            }
            while let Some(event) = self.server.ingress().poll_event() {
                if let Event::Headers { stream_id: id, .. } | Event::Data { stream_id: id, .. } =
                    event
                    && !streams.contains(&id.id())
                {
                    streams.push(id.id());
                }
            }
        }
        if !wrote {
            return Err(Failure::Empty);
        }
        Ok(streams)
    }

    pub fn try_response(&mut self) -> Result<(), Failure> {
        let response = self.server.output().first().to_vec();
        if response.is_empty() {
            return Err(Failure::Empty);
        }
        self.client.ingest(&response).map_err(Failure::Response)?;
        self.server.transfer().drain(response.len());
        Ok(())
    }
}
