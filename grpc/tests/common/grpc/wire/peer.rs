use std::{
    io,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use o3::alloc::VecExt;
use sark_core::manifold::stream::Boundary;
use sark_grpc::{
    client::{io::Session as _, session},
    protocol::section,
};
use sark_h2::{
    ClientRole, ServerRole, client,
    conn::{config, event, machine::Connection},
    frame::numbers::ErrorCode,
    stream,
};
use sark_http::field::{self, Entry, Lower, Pseudo, Value};

use crate::common::grpc::wire::{Failure, Transfer};

pub struct Peer {
    pub socket: TcpStream,
    pub client: Connection<ClientRole>,
    input: [u8; 4096],
}

impl Peer {
    pub fn try_server(
        client: &mut session::Session<'_, client::Plan<section::Plan>>,
    ) -> Result<(Connection<ServerRole>, Vec<stream::Key>), Failure> {
        let mut server = Connection::<ServerRole>::try_new().map_err(Failure::Config)?;
        let request = client.outbound();
        let len = request.len();
        server.ingress().ingest(request).map_err(Failure::Request)?;
        client.drain(len).map_err(Failure::Drive)?;
        let mut streams = Vec::new();
        while let Some(event) = server.ingress().poll_event() {
            if let event::Event::Headers { stream_id: id, .. } = event {
                VecExt::try_reserve(&mut streams, 1).map_err(Failure::Reserve)?;
                streams.push(id);
            }
        }
        Ok((server, streams))
    }

    pub fn try_reset(
        client: &mut session::Session<'_, client::Plan<section::Plan>>,
    ) -> Result<usize, Failure> {
        let (mut server, streams) = Self::try_server(client)?;
        let count = streams.len();
        for stream in streams {
            server
                .streams()
                .try_cancel(stream, ErrorCode::Cancel)
                .map_err(sark_h2::conn::Error::InactiveStream)
                .map_err(Failure::Protocol)?;
        }
        Transfer {
            client,
            server: &mut server,
        }
        .try_response()?;
        Ok(count)
    }

    pub fn try_config(addr: SocketAddr, config: config::Config) -> Result<Self, Failure> {
        let socket = TcpStream::connect(addr).map_err(Failure::Io)?;
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .map_err(Failure::Io)?;
        Ok(Self {
            socket,
            client: Connection::<ClientRole>::with_config(config).map_err(Failure::Config)?,
            input: [0; 4096],
        })
    }

    pub fn try_rejected<const N: usize>(
        addr: SocketAddr,
        path: &[u8],
        boundary: Boundary,
    ) -> Result<[ErrorCode; N], Failure> {
        let mut peer = Self::try_config(
            addr,
            config::Config {
                local_settings: config::Settings {
                    max_header_list_size: Some(1),
                    ..config::Settings::DEFAULT
                },
                ..config::Config::default()
            },
        )?;
        let mut codes = [ErrorCode::NoError; N];
        for code in &mut codes {
            let stream = peer
                .client
                .streams()
                .headers()
                .request(Self::try_headers(path).map_err(Failure::Field)?, boundary)
                .map_err(Failure::Protocol)?;
            peer.try_flush()?;
            *code = 'reset: loop {
                peer.try_read()?;
                while let Some(item) = peer.client.ingress().poll_event() {
                    if let event::Event::StreamReset {
                        stream_id: id,
                        error,
                        ..
                    } = item
                        && id.id() == stream
                    {
                        break 'reset error;
                    }
                }
            };
        }
        Ok(codes)
    }

    pub fn try_flush(&mut self) -> Result<(), Failure> {
        self.socket
            .write_all(self.client.output().first())
            .map_err(Failure::Io)?;
        self.client.transfer().drain(usize::MAX);
        Ok(())
    }

    pub fn try_read(&mut self) -> Result<(), Failure> {
        let read = self.socket.read(&mut self.input).map_err(Failure::Io)?;
        if read == 0 {
            return Err(Failure::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "streaming trailers",
            )));
        }
        self.client
            .ingress()
            .ingest(&self.input[..read])
            .map_err(Failure::Protocol)
    }
}

impl Peer {
    pub(in crate::common) fn try_headers(path: &[u8]) -> Result<[Entry<'_>; 7], field::Error> {
        Ok([
            Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"POST"))),
            Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"http"))),
            Entry::pseudo(Pseudo::Path, Value::try_bytes(path)?),
            Entry::pseudo(
                Pseudo::Authority,
                field::literal!(Value::try_bytes(b"localhost")),
            ),
            Entry::new(
                field::literal!(Lower::try_bytes(b"content-type")),
                field::literal!(Value::try_bytes(b"application/grpc+proto")),
            ),
            Entry::new(
                field::literal!(Lower::try_bytes(b"te")),
                field::literal!(Value::try_bytes(b"trailers")),
            ),
            Entry::new(
                field::literal!(Lower::try_bytes(b"x-owner-request")),
                field::literal!(Value::try_bytes(b"retained-through-response")),
            ),
        ])
    }
}
