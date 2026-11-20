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

pub fn try_request(
    client: &mut Session<'_, client::Plan<section::Plan>>,
    server: &mut Connection<ServerRole>,
) -> Result<Vec<Id>, Failure> {
    let mut wrote = false;
    let mut streams = Vec::new();
    let mut prefix = vec![0; 64 * 1024];
    loop {
        let (write, driven) = client.take_write(&mut prefix).map_err(Failure::Prepare)?;
        driven.map_err(Failure::Drive)?;
        match write {
            Output::Buffered(0) => break,
            Output::Buffered(len) => {
                server
                    .ingress()
                    .ingest(&prefix[..len])
                    .map_err(Failure::Request)?;
            }
            Output::Split {
                prefix_len: len,
                body,
            } => {
                server
                    .ingress()
                    .ingest(&prefix[..len])
                    .map_err(Failure::Request)?;
                server
                    .ingress()
                    .ingest(body.as_slice())
                    .map_err(Failure::Request)?;
            }
        }
        wrote = true;
        while let Some(event) = server.ingress().poll_event() {
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

pub fn try_response(
    client: &mut Session<'_, client::Plan<section::Plan>>,
    server: &mut Connection<ServerRole>,
) -> Result<(), Failure> {
    let response = server.output().first();
    if response.is_empty() {
        return Err(Failure::Empty);
    }
    let len = response.len();
    client.ingest(response).map_err(Failure::Response)?;
    server.transfer().drain(len);
    Ok(())
}
